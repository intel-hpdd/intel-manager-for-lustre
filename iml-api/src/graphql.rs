// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::error::ImlApiError;
use futures::TryStreamExt;
use iml_postgres::{sqlx, PgPool};
use iml_wire_types::snapshot::{Detail, List, Snapshot, Status};
use itertools::Itertools;
use juniper::{
    http::{graphiql::graphiql_source, GraphQLRequest},
    EmptyMutation, EmptySubscription, GraphQLEnum, RootNode,
};
use std::ops::Deref;
use std::{collections::HashSet, convert::Infallible, sync::Arc};
use warp::Filter;

#[derive(juniper::GraphQLObject)]
/// A Corosync Node found in `crm_mon`
struct CorosyncNode {
    /// The name of the node
    name: String,
    /// The id of the node as reported by `crm_mon`
    id: String,
    /// Id of the cluster this node belongs to
    cluster_id: i32,
    online: bool,
    standby: bool,
    standby_onfail: bool,
    maintenance: bool,
    pending: bool,
    unclean: bool,
    shutdown: bool,
    expected_up: bool,
    is_dc: bool,
    resources_running: i32,
    r#type: String,
}

#[derive(juniper::GraphQLObject)]
/// A Lustre Target
struct Target {
    /// The target's state. One of "mounted" or "unmounted"
    state: String,
    /// The target name
    name: String,
    /// The `host.id` of the host running this target
    active_host_id: Option<i32>,
    /// The list of `hosts.id`s the target can be mounted on.
    ///
    /// *Note*. This list represents where the backing storage can be mounted,
    /// it does not represent any HA configuration.
    host_ids: Vec<i32>,
    /// The list of `filesystem.name`s this target belongs to.
    /// Only an `MGS` may have more than one filesystem.
    filesystems: Vec<String>,
    /// Then underlying device UUID
    uuid: String,
    /// Where this target is mounted
    mount_path: Option<String>,
}

#[derive(juniper::GraphQLObject)]
/// A Lustre Target and it's corresponding resource
struct TargetResource {
    /// The id of the cluster
    cluster_id: i32,
    /// The filesystem associated with this target
    fs_name: String,
    /// The name of this target
    name: String,
    /// The corosync resource id associated with this target
    resource_id: String,
    /// The list of host ids this target could possibly run on
    cluster_hosts: Vec<i32>,
}

pub(crate) struct QueryRoot;

#[derive(GraphQLEnum)]
enum SortDir {
    Asc,
    Desc,
}

impl Default for SortDir {
    fn default() -> Self {
        Self::Asc
    }
}

impl Deref for SortDir {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

#[juniper::graphql_object(Context = Context)]
impl QueryRoot {
    #[graphql(arguments(
        limit(description = "paging limit, defaults to 20"),
        offset(description = "Offset into items, defaults to 0"),
        dir(description = "Sort direction, defaults to asc")
    ))]
    async fn corosync_nodes(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        dir: Option<SortDir>,
    ) -> juniper::FieldResult<Vec<CorosyncNode>> {
        let dir = dir.unwrap_or_default();

        let xs = sqlx::query_as!(
            CorosyncNode,
            r#"
                SELECT
                (n.id).name AS "name!",
                (n.id).id AS "id!",
                cluster_id,
                online,
                standby,
                standby_onfail,
                maintenance,
                pending,
                unclean,
                shutdown,
                expected_up,
                is_dc,
                resources_running,
                type
                FROM corosync_node n
                ORDER BY
                    CASE WHEN $1 = 'asc' THEN n.id END ASC,
                    CASE WHEN $1 = 'desc' THEN n.id END DESC
                OFFSET $2 LIMIT $3"#,
            dir.deref(),
            offset.unwrap_or(0) as i64,
            limit.unwrap_or(20) as i64
        )
        .fetch_all(&context.client)
        .await?;

        Ok(xs)
    }
    #[graphql(arguments(
        limit(description = "paging limit, defaults to 20"),
        offset(description = "Offset into items, defaults to 0"),
        dir(description = "Sort direction, defaults to asc")
    ))]
    /// Fetch the list of known targets
    async fn targets(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        dir: Option<SortDir>,
    ) -> juniper::FieldResult<Vec<Target>> {
        let dir = dir.unwrap_or_default();

        let xs = sqlx::query_as!(
            Target,
            r#"
                SELECT * FROM targets t
                ORDER BY
                    CASE WHEN $3 = 'asc' THEN t.name END ASC,
                    CASE WHEN $3 = 'desc' THEN t.name END DESC
                OFFSET $1 LIMIT $2"#,
            offset.unwrap_or(0) as i64,
            limit.unwrap_or(20) as i64,
            dir.deref()
        )
        .fetch_all(&context.client)
        .await?;

        Ok(xs)
    }

    /// Given a `fs_name`, produce a list of `TargetResource`.
    /// Each `TargetResource` will list the host ids it's capable of
    /// running on, taking bans into account.
    #[graphql(arguments(fs_name(description = "The filesystem to list `TargetResource`s for"),))]
    async fn get_fs_target_resources(
        context: &Context,
        fs_name: String,
    ) -> juniper::FieldResult<Vec<TargetResource>> {
        let xs = get_fs_target_resources(&context.client, fs_name).await?;

        Ok(xs)
    }

    /// Given a `fs_name`, produce a distinct grouping
    /// of cluster nodes that make up the filesystem.
    /// This is useful to find nodes capable of running resources associated
    /// with the given `fs_name`.
    #[graphql(arguments(fs_name(description = "The filesystem to search cluster nodes for"),))]
    async fn get_fs_cluster_hosts(
        context: &Context,
        fs_name: String,
    ) -> juniper::FieldResult<Vec<Vec<i32>>> {
        let xs = get_fs_target_resources(&context.client, fs_name)
            .await?
            .into_iter()
            .group_by(|x| x.cluster_id);

        let xs = xs.into_iter().fold(vec![], |mut acc, (_, xs)| {
            let xs: HashSet<i32> = xs.into_iter().map(|x| x.cluster_hosts).flatten().collect();

            acc.push(xs.into_iter().collect());

            acc
        });

        Ok(xs)
    }
    #[graphql(arguments(
        limit(description = "paging limit, defaults to 20"),
        offset(description = "Offset into items, defaults to 0"),
        dir(description = "Sort direction, defaults to asc"),
        args(description = "Snapshot listing arguments")
    ))]
    /// Fetch the list of snapshots
    async fn snapshots(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        dir: Option<SortDir>,
        args: List,
    ) -> juniper::FieldResult<Vec<Snapshot>> {
        let dir = dir.unwrap_or_default();

        if args.detail {
            let xs = sqlx::query!(
                r#"
                    SELECT filesystem_name, snapshot_name, create_time, modify_time, snapshot_fsname, mounted, comment FROM snapshot s
                    WHERE filesystem_name = $4 AND ($5::text IS NULL OR snapshot_name = $5)
                    ORDER BY
                        CASE WHEN $3 = 'asc' THEN s.create_time END ASC,
                        CASE WHEN $3 = 'desc' THEN s.create_time END DESC
                    OFFSET $1 LIMIT $2"#,
                offset.unwrap_or(0) as i64,
                limit.unwrap_or(20) as i64,
                dir.deref(),
                args.fsname,
                args.name,
            )
            .fetch_all(&context.client)
            .await?;

            let snapshots: Vec<_> = xs
                .into_iter()
                .map(|x| Snapshot {
                    snapshot_name: x.snapshot_name,
                    filesystem_name: x.filesystem_name,
                    details: vec![Detail {
                        comment: x.comment,
                        create_time: x.create_time,
                        modify_time: x.modify_time,
                        snapshot_fsname: x.snapshot_fsname,
                        snapshot_role: None,
                        status: x.mounted.map(|b| {
                            if b {
                                Status::Mounted
                            } else {
                                Status::NotMounted
                            }
                        }),
                    }],
                })
                .collect();

            Ok(snapshots)
        } else {
            let xs = sqlx::query!(
                r#"
                    SELECT filesystem_name, snapshot_name FROM snapshot s
                    WHERE filesystem_name = $4 AND ($5::text IS NULL OR snapshot_name = $5)
                    ORDER BY
                        CASE WHEN $3 = 'asc' THEN s.create_time END ASC,
                        CASE WHEN $3 = 'desc' THEN s.create_time END DESC
                    OFFSET $1 LIMIT $2"#,
                offset.unwrap_or(0) as i64,
                limit.unwrap_or(20) as i64,
                dir.deref(),
                args.fsname,
                args.name,
            )
            .fetch_all(&context.client)
            .await?;

            let snapshots: Vec<_> = xs
                .into_iter()
                .map(|x| Snapshot {
                    snapshot_name: x.snapshot_name,
                    filesystem_name: x.filesystem_name,
                    details: vec![],
                })
                .collect();

            Ok(snapshots)
        }
    }
}

pub(crate) type Schema =
    RootNode<'static, QueryRoot, EmptyMutation<Context>, EmptySubscription<Context>>;

pub(crate) struct Context {
    pub(crate) client: PgPool,
}

pub(crate) async fn graphql(
    schema: Arc<Schema>,
    ctx: Arc<Context>,
    req: GraphQLRequest,
) -> Result<impl warp::Reply, warp::Rejection> {
    let res = req.execute(&schema, &ctx).await;
    let json = serde_json::to_string(&res).map_err(ImlApiError::SerdeJsonError)?;

    Ok(json)
}

pub(crate) fn endpoint(
    schema_filter: impl Filter<Extract = (Arc<Schema>,), Error = Infallible> + Clone + Send,
    ctx_filter: impl Filter<Extract = (Arc<Context>,), Error = Infallible> + Clone + Send,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let graphql_route = warp::path!("graphql")
        .and(warp::post())
        .and(schema_filter)
        .and(ctx_filter)
        .and(warp::body::json())
        .and_then(graphql);

    let graphiql_route = warp::path!("graphiql")
        .and(warp::get())
        .map(|| warp::reply::html(graphiql_source("graphql", None)));

    graphql_route.or(graphiql_route)
}

async fn get_fs_target_resources(
    pool: &PgPool,
    fs_name: String,
) -> Result<Vec<TargetResource>, ImlApiError> {
    let banned_resources = sqlx::query!(r#"
            SELECT b.id, b.resource, b.node, b.cluster_id, nh.host_id, t.mount_point
            FROM corosync_resource_bans b
            INNER JOIN corosync_node_managed_host nh ON (nh.corosync_node_id).name = b.node
            AND nh.cluster_id = b.cluster_id
            INNER JOIN corosync_target_resource t ON t.id = b.resource AND b.cluster_id = t.cluster_id
        "#).fetch_all(pool).await?;

    let xs = sqlx::query!(r#"
            SELECT rh.cluster_id, r.id, t.name, t.mount_path, array_agg(DISTINCT rh.host_id) AS "cluster_hosts!"
            FROM targets t
            INNER JOIN corosync_target_resource r ON r.mount_point = t.mount_path
            INNER JOIN corosync_target_resource_managed_host rh ON rh.corosync_resource_id = r.id AND rh.host_id = ANY(t.host_ids)
            WHERE $1 = ANY(t.filesystems)
            GROUP BY rh.cluster_id, t.name, r.id, t.mount_path
        "#, &fs_name)
            .fetch(pool)
            .map_ok(|mut x| {
                let xs:HashSet<_> = banned_resources
                    .iter()
                    .filter(|y| {
                        y.cluster_id == x.cluster_id && y.resource == x.id &&  x.mount_path == y.mount_point
                    })
                    .map(|y| y.host_id)
                    .collect();

                x.cluster_hosts.retain(|id| !xs.contains(id));

                x
            })
            .map_ok(|x| {
                TargetResource {
                    cluster_id: x.cluster_id,
                    fs_name: fs_name.to_string(),
                    name: x.name,
                    resource_id: x.id,
                    cluster_hosts: x.cluster_hosts
                }
            }).try_collect()
            .await?;

    Ok(xs)
}
