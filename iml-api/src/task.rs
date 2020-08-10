// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{command::get_command, error::ImlApiError};
use futures::TryFutureExt;
use iml_postgres::{sqlx, PgPool};
use iml_rabbit::Connection;
use iml_wire_types::{ApiList, CmdWrapper, Task};
use warp::Filter;

async fn create_task(
    client: Connection,
    pool: PgPool,
    task: serde_json::Value,
) -> Result<impl warp::Reply, warp::Rejection> {
    // Return value: [ task_id, command_id ]
    let xs: Vec<i32> = iml_job_scheduler_rpc::call(&client, "create_task", vec![task], None)
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

    let command = get_command(&pool, xs[1]).await?;

    Ok(warp::reply::json(&CmdWrapper { command }))
}

async fn remove_task(
    client: Connection,
    pool: PgPool,
    ids: Vec<i32>,
) -> Result<impl warp::Reply, warp::Rejection> {
    // Return value: [ task_id, command_id ]
    let xs: Vec<i32> = iml_job_scheduler_rpc::call(&client, "remove_task", ids, None)
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

    let command = get_command(&pool, xs[1]).await?;

    Ok(warp::reply::json(&CmdWrapper { command }))
}

async fn get_tasks(pool: PgPool) -> Result<impl warp::Reply, warp::Rejection> {
    let xs = sqlx::query_as!(Task, "SELECT * FROM chroma_core_task")
        .fetch_all(&pool)
        .map_err(ImlApiError::SqlxError)
        .await?;

    Ok(warp::reply::json(&ApiList::new(xs)))
}

pub(crate) fn endpoint(
    client_filter: impl Filter<Extract = (Connection,), Error = warp::Rejection> + Clone + Send,
    pool_filter: impl Filter<Extract = (PgPool,), Error = std::convert::Infallible> + Clone + Send,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("task")
        .and(warp::get().and(pool_filter.clone()).and_then(get_tasks))
        .or(warp::post()
            .and(client_filter.clone())
            .and(pool_filter.clone())
            .and(warp::body::json())
            .and_then(create_task))
        .or(warp::delete()
            .and(client_filter)
            .and(pool_filter)
            .and(warp::body::json())
            .and_then(remove_task))
}
