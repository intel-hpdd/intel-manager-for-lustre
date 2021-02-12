// Copyright (c) 2021 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use emf_action_client::Client;
use emf_manager_env::get_pool_limit;
use emf_postgres::get_db_pool;
use emf_tracing::tracing;
use emf_wire_types::{
    db::{FidTaskQueue, LustreFid},
    task::Task,
    AgentResult, FidError, FidItem, LustreClient, TaskAction,
};
use futures::{future::join_all, lock::Mutex, FutureExt, TryFutureExt};
use lazy_static::lazy_static;
use sqlx::{postgres::PgPool, Executor};
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::time;

pub mod error;

// Number of fids to chunk together
const FID_LIMIT: i64 = 2000;
// Number of seconds between cycles
const DELAY: Duration = Duration::from_secs(5);

// Default pool limit if not overridden by POOL_LIMIT
lazy_static! {
    static ref POOL_LIMIT: u32 = get_pool_limit().unwrap_or(8);
}

async fn available_workers(
    pool: &PgPool,
    ids: Vec<i32>,
) -> Result<Vec<LustreClient>, error::EmfTaskRunnerError> {
    let clients = sqlx::query_as!(
        LustreClient,
        r#"
        SELECT * FROM lustreclientmount
        WHERE
            state = 'mounted'
            AND id != ALL($1)
        LIMIT $2
        "#,
        &ids,
        max(*POOL_LIMIT as i64 - ids.len() as i64, 0),
    )
    .fetch_all(pool)
    .await?;

    Ok(clients)
}

async fn tasks_per_worker(
    pool: &PgPool,
    worker: &LustreClient,
) -> Result<Vec<Task>, error::EmfTaskRunnerError> {
    let fs_id = sqlx::query!(
        "select id from filesystem where name = $1",
        &worker.filesystem
    )
    .fetch_optional(pool)
    .await?
    .map(|x| x.id);

    let fs_id = match fs_id {
        Some(x) => x,
        None => return Ok(vec![]),
    };

    let tasks = sqlx::query_as!(
        Task,
        r#"
        select * from task
        where
            filesystem_id = $1
            and state <> 'closed'
            and fids_total > fids_completed 
            and (running_on_id is Null or running_on_id = $2)"#,
        fs_id,
        worker.host_id
    )
    .fetch_all(pool)
    .await?;

    Ok(tasks)
}

async fn worker_fqdn(
    pool: &PgPool,
    worker: &LustreClient,
) -> Result<String, error::EmfTaskRunnerError> {
    let fqdn = sqlx::query!("SELECT fqdn FROM host WHERE id = $1", worker.host_id)
        .fetch_one(pool)
        .await
        .map(|x| x.fqdn)?;

    Ok(fqdn)
}

async fn send_work(
    action_client: &Client,
    pg_pool: &PgPool,
    fqdn: &str,
    fsname: &str,
    task: &Task,
    host_id: i32,
) -> Result<i64, error::EmfTaskRunnerError> {
    let taskargs: HashMap<String, String> = serde_json::from_value(task.args.clone())?;

    // Setup running_on if unset
    if task.single_runner && task.running_on_id.is_none() {
        tracing::trace!(
            "Attempting to Set Task {} ({}) running_on to host {} ({})",
            task.name,
            task.id,
            fqdn,
            host_id
        );

        let cnt = sqlx::query!(
            r#"
            UPDATE task
            SET running_on_id = $1
                WHERE id = $2
                AND running_on_id is Null"#,
            host_id,
            task.id
        )
        .execute(pg_pool)
        .await?
        .rows_affected();

        if cnt == 1 {
            tracing::info!(
                "Set Task {} ({}) running on host {} ({})",
                task.name,
                task.id,
                fqdn,
                host_id
            );
        } else {
            tracing::debug!(
                "Failed to Set Task {} running_on to host {}: {}",
                task.name,
                fqdn,
                cnt
            );

            return Ok(0);
        }
    }

    tracing::trace!("send_work({}, {}, {})", fqdn, fsname, task.name);

    let mut trans = pg_pool.begin().await?;

    tracing::trace!(
        "Started transaction for {}, {}, {}",
        fqdn,
        fsname,
        task.name
    );

    let rowlist = sqlx::query_as!(
        FidTaskQueue,
        r#"
        DELETE FROM fidtaskqueue 
        WHERE id in ( 
            SELECT id FROM fidtaskqueue WHERE task_id = $1 LIMIT $2 FOR UPDATE SKIP LOCKED 
        ) RETURNING id, fid as "fid: _", data, task_id"#,
        task.id,
        FID_LIMIT,
    )
    .fetch_all(&mut trans)
    .await?;

    tracing::debug!(
        "send_work({}, {}, {}) found {} fids",
        fqdn,
        fsname,
        task.name,
        rowlist.len()
    );

    if rowlist.is_empty() {
        return trans.commit().map_ok(|_| 0).err_into().await;
    }

    let fidlist: Vec<FidItem> = rowlist
        .into_iter()
        .map(|ft| FidItem {
            fid: ft.fid.to_string(),
            data: ft.data,
        })
        .collect();

    let completed = fidlist.len();
    let mut failed = 0;
    let args = TaskAction(fsname.to_string(), taskargs, fidlist);

    // send fids to actions runner
    // action names on Agents are "action.ACTION_NAME"
    for action in task.actions.iter().map(|a| format!("action.{}", a)) {
        match action_client
            .invoke_rust_agent(fqdn, &action, &args, None)
            .await
        {
            Err(e) => {
                tracing::info!("Failed to send {} to {}: {:?}", &action, fqdn, e);

                return trans.rollback().map_ok(|_| 0).err_into().await;
            }
            Ok(res) => {
                let agent_result: AgentResult = serde_json::from_value(res)?;

                match agent_result {
                    Ok(data) => {
                        tracing::debug!("Success {} on {}: {:?}", action, fqdn, data);

                        let errors: Vec<FidError> = serde_json::from_value(data)?;
                        failed += errors.len();

                        if task.keep_failed {
                            let task_id = task.id;

                            for err in errors.iter() {
                                let fid = match LustreFid::from_str(&err.fid) {
                                    Ok(x) => x,
                                    Err(e) => {
                                        tracing::info!("Could not convert FidError {:?} to LustreFid. Error: {:?}", err, e);
                                        continue;
                                    }
                                };

                                // #FIXME: This would be better as a bulk insert
                                if let Err(e) = trans
                                    .execute(sqlx::query!(
                                        r#"
                                                INSERT INTO fidtaskerror (fid, task_id, data, errno)
                                                VALUES ($1, $2, $3, $4)"#,
                                        fid as LustreFid,
                                        task_id,
                                        err.data,
                                        err.errno
                                    ))
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
                        tracing::info!("Failed {} on {}: {}", &action, fqdn, err);

                        return trans.rollback().map_ok(|_| 0).err_into().await;
                    }
                }
            }
        }
    }

    trans.commit().await?;

    if completed > 0 || failed > 0 {
        sqlx::query!(
            r#"
            UPDATE task
            SET 
                fids_completed = fids_completed + $1,
                fids_failed = fids_failed + $2
            WHERE id = $3"#,
            completed as i64,
            failed as i64,
            task.id
        )
        .execute(pg_pool)
        .await?;
    }

    Ok(completed as i64)
}

async fn run_tasks(
    action_client: &Client,
    fqdn: &str,
    worker: &LustreClient,
    xs: Vec<Task>,
    pool: &PgPool,
) {
    let fsname = &worker.filesystem;
    let host_id = worker.host_id;

    let xs = xs.into_iter().map(|task| async move {
        for _ in 0..10_u8 {
            let rc = send_work(action_client, &pool, &fqdn, &fsname, &task, host_id)
                .inspect_err(|e| tracing::warn!("send_work({}) failed {:?}", task.name, e))
                .await?;

            tracing::trace!("send_work({}) completed, rc: {}", task.name, rc);

            if rc < FID_LIMIT {
                break;
            }
        }

        Ok::<_, error::EmfTaskRunnerError>(())
    });

    join_all(xs).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    emf_tracing::init();

    let pg_pool = get_db_pool(
        *POOL_LIMIT,
        emf_manager_env::get_port("TASK_RUNNER_SERVICE_PORT"),
    )
    .await?;
    let active_clients = Arc::new(Mutex::new(HashSet::new()));
    let mut interval = time::interval(DELAY);

    let action_client = Client::default();

    // Task Runner Loop
    loop {
        interval.tick().await;

        tracing::trace!("Pool State: {:?}", pg_pool);

        let ids: Vec<i32> = {
            let xs = active_clients.lock().await;
            xs.iter().copied().collect()
        };

        if ids.len() as u32 >= *POOL_LIMIT {
            tracing::info!("No more capacity to service tasks. Active workers: {:?}, Connection Limit: {}. Will try again next tick.", ids, *POOL_LIMIT);
            continue;
        }

        tracing::trace!("checking workers for ids: {:?}", ids);

        let workers = available_workers(&pg_pool, ids).await?;

        tracing::trace!("got workers: {:?}", workers);

        {
            let mut x = active_clients.lock().await;

            x.extend(workers.iter().map(|w| w.id));

            tracing::trace!("Active Clients {:?}", x);
        }

        let xs = workers.into_iter().map(|worker| {
            let pg_pool = pg_pool.clone();
            let active_clients = Arc::clone(&active_clients);
            let worker_id = worker.id;
            let action_client = action_client.clone();

            async move {
                let tasks = tasks_per_worker(&pg_pool, &worker).await?;
                let fqdn = worker_fqdn(&pg_pool, &worker).await?;

                tracing::trace!("Starting run tasks for {}", &fqdn);

                run_tasks(&action_client, &fqdn, &worker, tasks, &pg_pool).await;

                tracing::trace!("Completed run tasks for {}", &fqdn);

                Ok::<_, error::EmfTaskRunnerError>(())
            }
            .then(move |x| async move {
                tracing::trace!("Attempting to take lock for release");

                {
                    let mut c = active_clients.lock().await;
                    tracing::trace!("Took lock for release");

                    c.remove(&worker_id);

                    tracing::trace!("Released Client {:?}. Active Clients {:?}", worker_id, c);
                }

                x
            })
        });

        tokio::spawn(join_all(xs));
    }
}
