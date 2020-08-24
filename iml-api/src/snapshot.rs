// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::error;
use iml_action_client::invoke_rust_agent;
use iml_postgres::{sqlx, PgPool};
use iml_rabbit::Connection;
use iml_wire_types::snapshot;
use warp::Filter;

async fn get_snapshots_internal(
    args: snapshot::List,
    conn: Connection,
    pool: PgPool,
) -> Result<serde_json::Value, error::ImlApiError> {
    drop(conn);

    let mgs_id = sqlx::query!(
        r#"
        select mgs_id from chroma_core_managedfilesystem where name=$1
        "#,
        args.fsname
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => error::ImlApiError::FileSystemNotFound(args.fsname.clone()),
        _ => e.into(),
    })?
    .mgs_id;

    let mgs_uuid = sqlx::query!(
        r#"
        select uuid from chroma_core_managedtarget where id=$1
        "#,
        mgs_id
    )
    .fetch_one(&pool)
    .await?
    .uuid;

    let active_mgs_host_id = sqlx::query!(
        r#"
        select active_host_id from targets where uuid=$1
        "#,
        mgs_uuid
    )
    .fetch_one(&pool)
    .await?
    .active_host_id;

    let active_mgs_host_fqdn = sqlx::query!(
        r#"
        select fqdn from chroma_core_managedhost where id=$1
        "#,
        active_mgs_host_id
    )
    .fetch_one(&pool)
    .await?
    .fqdn;

    tracing::info!("{}", active_mgs_host_fqdn);

    let snapshots = invoke_rust_agent(active_mgs_host_fqdn, "snapshot_list", args).await?;

    Ok(snapshots)
}

async fn get_snapshots(
    args: snapshot::List,
    conn: Connection,
    pool: PgPool,
) -> Result<impl warp::Reply, warp::Rejection> {
    let snapshots = get_snapshots_internal(args, conn, pool)
        .await
        .map_err(|e| match e {
            error::ImlApiError::FileSystemNotFound(_) => warp::reject::not_found(),
            _ => e.into(),
        })?;

    Ok(warp::reply::json(&snapshots))
}

async fn create_snapshot_internal(
    args: snapshot::Create,
    conn: Connection,
    pool: PgPool,
) -> Result<serde_json::Value, error::ImlApiError> {
    drop(conn);

    let mgs_id = sqlx::query!(
        r#"
        select mgs_id from chroma_core_managedfilesystem where name=$1
        "#,
        args.fsname
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => error::ImlApiError::FileSystemNotFound(args.fsname.clone()),
        _ => e.into(),
    })?
    .mgs_id;

    let mgs_uuid = sqlx::query!(
        r#"
        select uuid from chroma_core_managedtarget where id=$1
        "#,
        mgs_id
    )
    .fetch_one(&pool)
    .await?
    .uuid;

    let active_mgs_host_id = sqlx::query!(
        r#"
        select active_host_id from targets where uuid=$1
        "#,
        mgs_uuid
    )
    .fetch_one(&pool)
    .await?
    .active_host_id;

    let active_mgs_host_fqdn = sqlx::query!(
        r#"
        select fqdn from chroma_core_managedhost where id=$1
        "#,
        active_mgs_host_id
    )
    .fetch_one(&pool)
    .await?
    .fqdn;

    tracing::info!("{}", active_mgs_host_fqdn);

    let result = invoke_rust_agent(active_mgs_host_fqdn, "snapshot_create", args).await?;

    Ok(result)
}

async fn create_snapshot(
    args: snapshot::Create,
    conn: Connection,
    pool: PgPool,
) -> Result<impl warp::Reply, warp::Rejection> {
    let results = create_snapshot_internal(args, conn, pool)
        .await
        .map_err(|e| match e {
            error::ImlApiError::FileSystemNotFound(_) => warp::reject::not_found(),
            _ => e.into(),
        })?;

    Ok(warp::reply::json(&results))
}

async fn destroy_snapshot_internal(
    args: snapshot::Destroy,
    conn: Connection,
    pool: PgPool,
) -> Result<serde_json::Value, error::ImlApiError> {
    drop(conn);

    let mgs_id = sqlx::query!(
        r#"
        select mgs_id from chroma_core_managedfilesystem where name=$1
        "#,
        args.fsname
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => error::ImlApiError::FileSystemNotFound(args.fsname.clone()),
        _ => e.into(),
    })?
    .mgs_id;

    let mgs_uuid = sqlx::query!(
        r#"
        select uuid from chroma_core_managedtarget where id=$1
        "#,
        mgs_id
    )
    .fetch_one(&pool)
    .await?
    .uuid;

    let active_mgs_host_id = sqlx::query!(
        r#"
        select active_host_id from targets where uuid=$1
        "#,
        mgs_uuid
    )
    .fetch_one(&pool)
    .await?
    .active_host_id;

    let active_mgs_host_fqdn = sqlx::query!(
        r#"
        select fqdn from chroma_core_managedhost where id=$1
        "#,
        active_mgs_host_id
    )
    .fetch_one(&pool)
    .await?
    .fqdn;

    tracing::info!("{}", active_mgs_host_fqdn);

    let result = invoke_rust_agent(active_mgs_host_fqdn, "snapshot_destroy", args).await?;

    Ok(result)
}

async fn destroy_snapshot(
    args: snapshot::Destroy,
    conn: Connection,
    pool: PgPool,
) -> Result<impl warp::Reply, warp::Rejection> {
    let results = destroy_snapshot_internal(args, conn, pool)
        .await
        .map_err(|e| match e {
            error::ImlApiError::FileSystemNotFound(_) => warp::reject::not_found(),
            _ => e.into(),
        })?;

    Ok(warp::reply::json(&results))
}

pub(crate) fn endpoint(
    client_filter: impl Filter<Extract = (Connection,), Error = warp::Rejection> + Clone + Send,
    pool_filter: impl Filter<Extract = (PgPool,), Error = std::convert::Infallible> + Clone + Send,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let list = warp::path!("snapshot")
        .and(warp::get())
        .and(warp::query())
        .and(client_filter.clone())
        .and(pool_filter.clone())
        .and_then(get_snapshots);

    let create = warp::path!("snapshot")
        .and(warp::post())
        .and(warp::query())
        .and(client_filter.clone())
        .and(pool_filter.clone())
        .and_then(create_snapshot);

    let destroy = warp::path!("snapshot" / "destroy")
        .and(warp::post())
        .and(warp::query())
        .and(client_filter)
        .and(pool_filter)
        .and_then(destroy_snapshot);

    list.or(create).or(destroy)
}
