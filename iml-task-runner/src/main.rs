// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::{future::try_join_all, lock::Mutex, TryFutureExt};
use iml_action_client::invoke_rust_agent;
use iml_orm::{
    clientmount::ChromaCoreLustreclientmount as Client,
    filesystem::ChromaCoreManagedfilesystem as Filesystem,
    hosts::ChromaCoreManagedhost as Host,
    task::{self, ChromaCoreTask as Task},
    tokio_diesel::{AsyncRunQueryDsl as _, OptionalExtension as _},
    DbPool,
};
use iml_postgres::pool;
use iml_wire_types::{db::FidTaskQueue, AgentResult, FidError, FidItem, TaskAction};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::time;

pub mod error;

// Number of fids to chunk together
const FID_LIMIT: i64 = 2000;
// Number of seconds between cycles
const DELAY: u64 = 5;

async fn available_workers(
    pool: &DbPool,
    active: Arc<Mutex<HashSet<i32>>>,
) -> Result<Vec<Client>, error::ImlTaskRunnerError> {
    let list = active.lock().await;
    let clients: Vec<Client> = Client::not_ids(list.iter().copied())
        .get_results_async(pool)
        .await?;
    Ok(clients)
}

async fn tasks_per_worker(
    pool: &DbPool,
    worker: &Client,
) -> Result<Vec<Task>, error::ImlTaskRunnerError> {
    let fs_id = {
        let fsmap: Option<Filesystem> = Filesystem::by_name(&worker.filesystem)
            .first_async(pool)
            .await
            .optional()?;
        match fsmap {
            Some(f) => f.id,
            None => return Ok(vec![]),
        }
    };

    let tasks: Vec<Task> = Task::outgestable(fs_id, worker.host_id)
        .get_results_async(pool)
        .await?;
    Ok(tasks)
}

async fn worker_fqdn(pool: &DbPool, worker: &Client) -> Result<String, error::ImlTaskRunnerError> {
    let host: Host = Host::by_id(worker.host_id).first_async(pool).await?;
    Ok(host.fqdn)
}

async fn send_work(
    orm_pool: DbPool,
    mut client: pool::Client,
    fqdn: String,
    fsname: String,
    task: &Task,
    host_id: i32,
) -> Result<(), error::ImlTaskRunnerError> {
    let taskargs: HashMap<String, String> = serde_json::from_value(task.args.clone())?;

    tracing::debug!("send_work({}, {}, {})", &fqdn, &fsname, task.name);

    let trans = client.transaction().await?;

    let sql = "DELETE FROM chroma_core_fidtaskqueue WHERE id in ( SELECT id FROM chroma_core_fidtaskqueue WHERE task_id = $1 LIMIT $2 FOR UPDATE SKIP LOCKED ) RETURNING *";
    let s = trans.prepare(sql).await?;

    // @@ could convert this to query_raw and map stream then collect
    let rowlist = trans.query(&s, &[&task.id, &FID_LIMIT]).await?;

    tracing::debug!(
        "send_work({}, {}, {}) found {} fids",
        &fqdn,
        &fsname,
        task.name,
        rowlist.len()
    );

    let fidlist = rowlist
        .into_iter()
        .map(|row| {
            let ft: FidTaskQueue = row.into();
            FidItem {
                fid: ft.fid.to_string(),
                data: ft.data,
            }
        })
        .collect();

    let mut completed = fidlist.len();
    let mut failed = 0;
    let args = TaskAction(fsname, taskargs, fidlist);

    // send fids to actions runner
    // action names on Agents are "action.ACTION_NAME"
    for action in task.actions.iter().map(|a| format!("action.{}", a)) {
        let (_, fut) = invoke_rust_agent(&fqdn, &action, &args);
        match fut.await {
            Err(e) => {
                tracing::info!("Failed to send {} to {}: {}", &action, &fqdn, e);
                return trans.rollback().err_into().await;
            }
            Ok(res) => {
                let agent_result: AgentResult = serde_json::from_value(res)?;
                match agent_result {
                    Ok(data) => {
                        tracing::debug!("Success {} on {}: {:?}", &action, &fqdn, data);
                        let errors: Vec<FidError> = serde_json::from_value(data)?;
                        failed += errors.len();
                        completed -= errors.len();

                        if task.keep_failed {
                            let sql = "INSERT INTO chroma_core_fidtaskerror (fid, task, data, errno) VALUES ($1, $2, $3, $4)";
                            let s = trans.prepare(sql).await?;
                            let task_id = task.id;
                            for err in errors.iter() {
                                if let Err(e) = trans
                                    .execute(&s, &[&err.fid, &task_id, &err.data, &err.errno])
                                    .await
                                {
                                    tracing::info!(
                                        "Failed to insert fid error ({} : {}): {}",
                                        err.fid,
                                        err.errno,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(err) => {
                        tracing::info!("Failed {} on {}: {}", &action, &fqdn, err);
                        return trans.rollback().err_into().await;
                    }
                }
            }
        }
    }

    trans.commit().await?;

    if completed > 0 || failed > 0 {
        task::increase_finished(task.id, completed as i64, failed as i64)
            .execute_async(&orm_pool)
            .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    iml_tracing::init();

    let orm_pool = iml_orm::pool()?;
    let pg_pool = pool::pool().await?;
    let activeclients = Arc::new(Mutex::new(HashSet::new()));

    // Task Runner Loop
    let mut interval = time::interval(Duration::from_secs(DELAY));
    loop {
        interval.tick().await;

        let workers = available_workers(&pool, activeclients.clone())
            .await
            .unwrap_or(vec![]);

        tokio::spawn({
            let shared_client = shared_client.clone();
            try_join_all(workers.into_iter().map(|worker| {
                let shared_client = shared_client.clone();
                let fsname = worker.filesystem.clone();
                let pool = pool.clone();
                let activeclients = activeclients.clone();

                async move {
                    activeclients.lock().await.insert(worker.id);
                    let tasks = tasks_per_worker(&pool, &worker).await?;
                    let fqdn = worker_fqdn(&pool, &worker).await?;

                    let rc = try_join_all(tasks.into_iter().map(|task| {
                        let shared_client = shared_client.clone();
                        let fsname = fsname.clone();
                        let fqdn = fqdn.clone();
                        async move {
                            send_work(shared_client.clone(), fqdn, fsname, &task)
                                .await
                                .map_err(|e| {
                                    tracing::warn!(
                                        "send_work({}) failed {:?}",
                                        task.name,
                                        e
                                    );
                                    e
                                })
                        }
                    }))
                    .await;
                    activeclients.lock().await.remove(&worker.id);
                    rc
                }
            }))
        });
    }
}
