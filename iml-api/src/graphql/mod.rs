// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

mod stratagem;
mod task;

use crate::{
    command::get_command,
    error::ImlApiError,
    timer::{configure_snapshot_timer, remove_snapshot_timer},
};
use chrono::{DateTime, Utc};
use futures::{future::join_all, TryFutureExt, TryStreamExt};
use iml_postgres::{
    active_mgs_host_fqdn, fqdn_by_host_id, sqlx, sqlx::postgres::types::PgInterval, PgPool,
};
use iml_rabbit::{ImlRabbitError, Pool};
use iml_wire_types::{
    db::LogMessageRecord,
    graphql_duration::GraphQLDuration,
    logs::{LogResponse, Meta},
    snapshot::{ReserveUnit, Snapshot, SnapshotInterval, SnapshotRetention},
    task::Task,
    Command, EndpointName, HotpoolConfiguration, Job, LogMessage, LogSeverity, MessageClass,
    SortDir,
};
use itertools::Itertools;
use juniper::{
    http::{graphiql::graphiql_source, GraphQLRequest},
    EmptySubscription, FieldError, RootNode, Value,
};
use std::{
    collections::{HashMap, HashSet},
    convert::{Infallible, TryFrom as _, TryInto},
    ops::Deref,
    sync::Arc,
};
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

#[derive(Debug, juniper::GraphQLEnum)]
enum HotpoolState {
    Unconfigured,
    Configured,
    Stopped,
    Started,
    Removed,
}

impl std::fmt::Display for HotpoolState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Unconfigured => f.pad(&format!("{}", "unconfigured")),
            Self::Configured => f.pad(&format!("{}", "configured")),
            Self::Stopped => f.pad(&format!("{}", "stopped")),
            Self::Started => f.pad(&format!("{}", "started")),
            Self::Removed => f.pad(&format!("{}", "removed")),
        }
    }
}

#[derive(Debug, juniper::GraphQLObject)]
/// A Lustre Target
struct Target {
    /// The target's state. One of "mounted" or "unmounted"
    state: String,
    /// The target name
    name: String,
    /// The device path used to create the target mount
    dev_path: Option<String>,
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

struct BannedTargetResource {
    resource: String,
    cluster_id: i32,
    host_id: i32,
    mount_point: Option<String>,
}

#[derive(juniper::GraphQLObject)]
/// A Corosync banned resource
struct BannedResource {
    /// The resource id
    id: String,
    /// The id of the cluster in which the resource lives
    cluster_id: i32,
    /// The resource name
    resource: String,
    /// The node in which the resource lives
    node: String,
    /// The assigned weight of the resource
    weight: i32,
    /// Is master only
    master_only: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct SendJob<'a, T> {
    pub class_name: &'a str,
    pub args: T,
}

pub(crate) struct QueryRoot;

#[juniper::graphql_object(Context = Context)]
impl QueryRoot {
    fn stratagem(&self) -> stratagem::StratagemQuery {
        stratagem::StratagemQuery
    }
    fn task(&self) -> task::TaskQuery {
        task::TaskQuery
    }
    #[graphql(arguments(
        limit(description = "optional paging limit, defaults to all rows"),
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
                    CASE WHEN $1 = 'ASC' THEN n.id END ASC,
                    CASE WHEN $1 = 'DESC' THEN n.id END DESC
                OFFSET $2 LIMIT $3"#,
            dir.deref(),
            offset.unwrap_or(0) as i64,
            limit.map(|x| x as i64),
        )
        .fetch_all(&context.pg_pool)
        .await?;

        Ok(xs)
    }

    #[graphql(arguments(
        limit(description = "optional paging limit, defaults to all rows"),
        offset(description = "Offset into items, defaults to 0"),
        dir(description = "Sort direction, defaults to asc"),
        fs_name(description = "Targets associated with the specified filesystem"),
        exclude_unmounted(description = "Exclude unmounted targets, defaults to false"),
    ))]
    /// Fetch the list of known targets
    async fn targets(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        dir: Option<SortDir>,
        fs_name: Option<String>,
        exclude_unmounted: Option<bool>,
    ) -> juniper::FieldResult<Vec<Target>> {
        let dir = dir.unwrap_or_default();

        if let Some(ref fs_name) = fs_name {
            let _ = fs_id_by_name(&context.pg_pool, &fs_name).await?;
        }

        let xs: Vec<Target> = sqlx::query_as!(
            Target,
            r#"
                SELECT * from target t
                ORDER BY
                    CASE WHEN $3 = 'ASC' THEN t.name END ASC,
                    CASE WHEN $3 = 'DESC' THEN t.name END DESC
                OFFSET $1 LIMIT $2"#,
            offset.unwrap_or(0) as i64,
            limit.map(|x| x as i64),
            dir.deref()
        )
        .fetch_all(&context.pg_pool)
        .await?
        .into_iter()
        .filter(|x| match &fs_name {
            Some(fs) => x.filesystems.contains(&fs),
            None => true,
        })
        .filter(|x| match exclude_unmounted {
            Some(true) => x.state != "unmounted",
            Some(false) | None => true,
        })
        .collect();

        let target_resources = get_fs_target_resources(&context.pg_pool, None).await?;

        let xs: Vec<Target> = xs
            .into_iter()
            .map(|mut x| {
                let resource = target_resources
                    .iter()
                    .find(|resource| resource.name == x.name);

                if let Some(resource) = resource {
                    x.host_ids = resource.cluster_hosts.clone();
                }

                x
            })
            .collect();

        Ok(xs)
    }

    #[graphql(arguments(
        limit(description = "paging limit, defaults to 20"),
        offset(description = "Offset into items, defaults to 0"),
        dir(description = "Sort direction, defaults to asc"),
    ))]
    /// Fetch the list of known targets
    async fn hotpools(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        dir: Option<SortDir>,
    ) -> juniper::FieldResult<Vec<HotpoolConfiguration>> {
        let dir = dir.unwrap_or_default();

        let xs: Vec<HotpoolConfiguration> = sqlx::query_as!(
            HotpoolConfiguration,
            r#"
                SELECT h.id, f.name AS filesystem, h.state, h.state_modified_at, h.ha_label,
                a.hot_id, a.cold_id, a.extend_id, a.resync_id, a.minage, p.purge_id,
                h.version::integer as "version: i32",
                p.freehi::integer as "freehi: i32",
                p.freelo::integer as "freelo: i32"
                FROM chroma_core_hotpoolconfiguration h
                JOIN chroma_core_managedfilesystem f ON h.filesystem_id = f.id
                JOIN chroma_core_lamigoconfiguration a ON h.id = a.hotpool_id
                JOIN chroma_core_lpurgeconfiguration p ON h.id = p.hotpool_id
                WHERE h.not_deleted = 't'
                ORDER BY
                    CASE WHEN $3 = 'asc' THEN h.id END ASC,
                    CASE WHEN $3 = 'desc' THEN h.id END DESC
                OFFSET $1 LIMIT $2"#,
            offset.unwrap_or(0) as i64,
            limit.unwrap_or(20) as i64,
            dir.deref()
        )
        .fetch_all(&context.pg_pool)
        .await?
        .into_iter()
        .collect();

        Ok(xs)
    }

    /// Given a `fs_name`, produce a list of `TargetResource`.
    /// Each `TargetResource` will list the host ids it's capable of
    /// running on, taking bans into account.
    #[graphql(arguments(fs_name(description = "The filesystem to list `TargetResource`s for"),))]
    async fn get_fs_target_resources(
        context: &Context,
        fs_name: Option<String>,
    ) -> juniper::FieldResult<Vec<TargetResource>> {
        if let Some(ref fs_name) = fs_name {
            let _ = fs_id_by_name(&context.pg_pool, &fs_name).await?;
        }
        let xs = get_fs_target_resources(&context.pg_pool, fs_name).await?;

        Ok(xs)
    }

    async fn get_banned_resources(context: &Context) -> juniper::FieldResult<Vec<BannedResource>> {
        let xs = get_banned_resources(&context.pg_pool).await?;

        Ok(xs)
    }

    /// Given a `fs_name`, produce a distinct grouping
    /// of cluster nodes that make up the filesystem.
    /// This is useful to find nodes capable of running resources associated
    /// with the given `fs_name`.
    #[graphql(arguments(fs_name(description = "The filesystem to search cluster nodes for")))]
    async fn get_fs_cluster_hosts(
        context: &Context,
        fs_name: String,
    ) -> juniper::FieldResult<Vec<Vec<String>>> {
        let _ = fs_id_by_name(&context.pg_pool, &fs_name).await?;
        let xs = get_fs_cluster_hosts(&context.pg_pool, fs_name).await?;

        Ok(xs)
    }

    #[graphql(arguments(
        limit(description = "optional paging limit, defaults to all rows"),
        offset(description = "Offset into items, defaults to 0"),
        dir(description = "Sort direction, defaults to ASC"),
        fsname(description = "Filesystem the snapshot was taken from"),
        name(description = "Name of the snapshot"),
    ))]
    /// Fetch the list of snapshots
    async fn snapshots(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        dir: Option<SortDir>,
        fsname: String,
        name: Option<String>,
    ) -> juniper::FieldResult<Vec<Snapshot>> {
        let dir = dir.unwrap_or_default();

        let _ = fs_id_by_name(&context.pg_pool, &fsname).await?;

        let snapshots = sqlx::query_as!(
            Snapshot,
                r#"
                    SELECT filesystem_name, snapshot_name, create_time, modify_time, snapshot_fsname, mounted, comment FROM snapshot s
                    WHERE filesystem_name = $4 AND ($5::text IS NULL OR snapshot_name = $5)
                    ORDER BY
                        CASE WHEN $3 = 'ASC' THEN s.create_time END ASC,
                        CASE WHEN $3 = 'DESC' THEN s.create_time END DESC
                    OFFSET $1 LIMIT $2"#,
                offset.unwrap_or(0) as i64,
                limit.map(|x| x as i64),
                dir.deref(),
                fsname,
                name,
            )
            .fetch_all(&context.pg_pool)
            .await?;

        Ok(snapshots)
    }

    /// Fetch the list of commands
    #[graphql(arguments(
        limit(description = "optional paging limit, defaults to all rows"),
        offset(description = "Offset into items, defaults to 0"),
        dir(description = "Sort direction, defaults to ASC"),
        is_active(description = "Command status, active means not completed, default is true"),
        msg(description = "Substring of the command's message, null or empty matches all"),
    ))]
    async fn commands(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        dir: Option<SortDir>,
        is_active: Option<bool>,
        msg: Option<String>,
    ) -> juniper::FieldResult<Vec<Command>> {
        let dir = dir.unwrap_or_default();
        let is_completed = !is_active.unwrap_or(true);
        let commands: Vec<Command> = sqlx::query_as!(
            CommandTmpRecord,
            r#"
                SELECT
                    c.id AS id,
                    cancelled,
                    complete,
                    errored,
                    created_at,
                    array_agg(cj.job_id)::INT[] AS job_ids,
                    message
                FROM chroma_core_command c
                JOIN chroma_core_command_jobs cj ON c.id = cj.command_id
                WHERE ($4::BOOL IS NULL OR complete = $4)
                  AND ($5::TEXT IS NULL OR c.message ILIKE '%' || $5 || '%')
                GROUP BY c.id
                ORDER BY
                    CASE WHEN $3 = 'ASC' THEN c.id END ASC,
                    CASE WHEN $3 = 'DESC' THEN c.id END DESC
                OFFSET $1 LIMIT $2
            "#,
            offset.unwrap_or(0) as i64,
            limit.map(|x| x as i64),
            dir.deref(),
            is_completed,
            msg,
        )
        .fetch_all(&context.pg_pool)
        .map_ok(|xs: Vec<CommandTmpRecord>| {
            xs.into_iter().map(to_command).collect::<Vec<Command>>()
        })
        .await?;
        Ok(commands)
    }

    /// Fetch the list of commands by ids, the returned
    /// collection is guaranteed to match the input.
    /// If a command not found, `None` is returned for that index.
    #[graphql(arguments(
        limit(description = "paging limit, defaults to 20"),
        offset(description = "Offset into items, defaults to 0"),
        ids(description = "The list of command ids to fetch, ids may be empty"),
    ))]
    async fn commands_by_ids(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        ids: Vec<i32>,
    ) -> juniper::FieldResult<Vec<Option<Command>>> {
        let ids: &[i32] = &ids[..];
        let unordered_cmds: Vec<Command> = sqlx::query_as!(
            CommandTmpRecord,
            r#"
                SELECT
                    c.id AS id,
                    cancelled,
                    complete,
                    errored,
                    created_at,
                    array_agg(cj.job_id)::INT[] AS job_ids,
                    message
                FROM chroma_core_command c
                JOIN chroma_core_command_jobs cj ON c.id = cj.command_id
                WHERE (c.id = ANY ($3::INT[]))
                GROUP BY c.id
                OFFSET $1 LIMIT $2
            "#,
            offset.unwrap_or(0) as i64,
            limit.unwrap_or(20) as i64,
            ids,
        )
        .fetch_all(&context.pg_pool)
        .map_ok(|xs: Vec<CommandTmpRecord>| {
            xs.into_iter().map(to_command).collect::<Vec<Command>>()
        })
        .await?;
        let mut hm = unordered_cmds
            .into_iter()
            .map(|c| (c.id, c))
            .collect::<HashMap<i32, Command>>();
        let commands = ids
            .iter()
            .map(|id| hm.remove(id))
            .collect::<Vec<Option<Command>>>();

        Ok(commands)
    }

    /// List all snapshot intervals
    async fn snapshot_intervals(context: &Context) -> juniper::FieldResult<Vec<SnapshotInterval>> {
        let xs: Vec<SnapshotInterval> = sqlx::query!("SELECT * FROM snapshot_interval")
            .fetch(&context.pg_pool)
            .map_ok(|x| SnapshotInterval {
                id: x.id,
                filesystem_name: x.filesystem_name,
                use_barrier: x.use_barrier,
                interval: x.interval.into(),
                last_run: x.last_run,
            })
            .try_collect()
            .await?;

        Ok(xs)
    }
    /// List all snapshot retention policies. Snapshots will automatically be deleted (starting with the oldest)
    /// when free space falls below the defined reserve value and its associated unit.
    async fn snapshot_retention_policies(
        context: &Context,
    ) -> juniper::FieldResult<Vec<SnapshotRetention>> {
        let xs: Vec<SnapshotRetention> = sqlx::query_as!(
            SnapshotRetention,
            r#"
                SELECT
                    id,
                    filesystem_name,
                    reserve_value,
                    reserve_unit as "reserve_unit:ReserveUnit",
                    last_run,
                    keep_num
                FROM snapshot_retention
            "#
        )
        .fetch(&context.pg_pool)
        .try_collect()
        .await?;

        Ok(xs)
    }

    #[graphql(arguments(
        limit(description = "optional paging limit, defaults to 100",),
        offset(description = "Offset into items, defaults to 0"),
        dir(description = "Sort direction, defaults to asc"),
        message(
            description = "Pattern to search for in message. Uses Postgres pattern matching  (https://www.postgresql.org/docs/9.6/functions-matching.html)"
        ),
        fqdn(
            description = "Pattern to search for in FQDN. Uses Postgres pattern matching  (https://www.postgresql.org/docs/9.6/functions-matching.html)"
        ),
        tag(
            description = "Pattern to search for in tag. Uses Postgres pattern matching  (https://www.postgresql.org/docs/9.6/functions-matching.html)"
        ),
        start_datetime(description = "Start of the time period of logs"),
        end_datetime(description = "End of the time period of logs"),
        message_class(description = "Array of log message classes"),
        severity(description = "Upper bound of log severity"),
    ))]
    /// Returns aggregated journal entries for all nodes the agent runs on.
    async fn logs(
        context: &Context,
        limit: Option<i32>,
        offset: Option<i32>,
        dir: Option<SortDir>,
        message: Option<String>,
        fqdn: Option<String>,
        tag: Option<String>,
        start_datetime: Option<chrono::DateTime<Utc>>,
        end_datetime: Option<chrono::DateTime<Utc>>,
        message_class: Option<Vec<MessageClass>>,
        severity: Option<LogSeverity>,
    ) -> juniper::FieldResult<LogResponse> {
        let dir = dir.unwrap_or_default();

        let message_class: Vec<_> = message_class
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec![MessageClass::Normal])
            .into_iter()
            .map(|i| i as i16)
            .collect();

        let severity = severity.unwrap_or(LogSeverity::Informational) as i16;

        let results = sqlx::query_as!(
            LogMessageRecord,
            r#"
                    SELECT * FROM chroma_core_logmessage t
                    WHERE ($4::TEXT IS NULL OR t.message LIKE $4)
                      AND ($5::TEXT IS NULL OR t.fqdn LIKE $5)
                      AND ($6::TEXT IS NULL OR t.tag LIKE $6)
                      AND ($7::TIMESTAMPTZ IS NULL OR t.datetime >= $7)
                      AND ($8::TIMESTAMPTZ IS NULL OR t.datetime < $8)
                      AND ARRAY[t.message_class] <@ $9
                      AND t.severity <= $10
                    ORDER BY
                        CASE WHEN $3 = 'ASC' THEN t.datetime END ASC,
                        CASE WHEN $3 = 'DESC' THEN t.datetime END DESC
                    OFFSET $1 LIMIT $2"#,
            offset.unwrap_or(0) as i64,
            limit.map(|x| x as i64).unwrap_or(100),
            dir.deref(),
            message,
            fqdn,
            tag,
            start_datetime,
            end_datetime,
            &message_class,
            severity,
        )
        .fetch_all(&context.pg_pool)
        .await?;
        let xs: Vec<LogMessage> = results
            .into_iter()
            .map(|x| x.try_into())
            .collect::<Result<_, _>>()?;

        let total_count = sqlx::query!(
            "SELECT total_rows FROM rowcount WHERE table_name = 'chroma_core_logmessage';"
        )
        .fetch_one(&context.pg_pool)
        .await?
        .total_rows
        .ok_or_else(|| FieldError::new("Number of rows doesn't fit in i32", Value::null()))?;

        Ok(LogResponse {
            data: xs,
            meta: Meta {
                total_count: total_count.try_into()?,
            },
        })
    }
}

struct SnapshotIntervalName {
    id: i32,
    fs_name: String,
    timestamp: DateTime<Utc>,
}

fn parse_snapshot_name(name: &str) -> Option<SnapshotIntervalName> {
    match name.trim().splitn(3, "-").collect::<Vec<&str>>().as_slice() {
        [id, fs, ts] => {
            let ts = ts.parse::<DateTime<Utc>>().ok()?;
            let id = id.parse::<i32>().ok()?;

            Some(SnapshotIntervalName {
                id,
                fs_name: fs.to_string(),
                timestamp: ts,
            })
        }
        _ => None,
    }
}

pub(crate) struct MutationRoot;

#[juniper::graphql_object(Context = Context)]
impl MutationRoot {
    fn stratagem(&self) -> stratagem::StratagemMutation {
        stratagem::StratagemMutation
    }
    fn task(&self) -> task::TaskMutation {
        task::TaskMutation
    }
    #[graphql(arguments(
        fsname(description = "Filesystem to snapshot"),
        name(description = "Name of the snapshot"),
        comment(description = "A description for the purpose of the snapshot"),
        use_barrier(
            description = "Set write barrier before creating snapshot. The default value is `false`"
        )
    ))]
    /// Creates a snapshot of an existing Lustre filesystem. Returns a `Command` to track progress.
    /// For the `Command` to succeed, the filesystem being snapshoted must be available.
    async fn create_snapshot(
        context: &Context,
        fsname: String,
        name: String,
        comment: Option<String>,
        use_barrier: Option<bool>,
    ) -> juniper::FieldResult<Command> {
        let _ = fs_id_by_name(&context.pg_pool, &fsname).await?;
        let name = name.trim();
        validate_snapshot_name(name)?;

        let snapshot_interval_name = parse_snapshot_name(name);
        if let Some(data) = snapshot_interval_name {
            sqlx::query!(
                r#"
                UPDATE snapshot_interval
                SET last_run=$1
                WHERE id=$2 AND filesystem_name=$3
            "#,
                data.timestamp,
                data.id,
                data.fs_name,
            )
            .execute(&context.pg_pool)
            .await?;
        }

        let active_mgs_host_fqdn = active_mgs_host_fqdn(&fsname, &context.pg_pool)
            .await?
            .ok_or_else(|| {
                FieldError::new("Filesystem not found or MGS is not mounted", Value::null())
            })?;

        let kwargs: HashMap<String, String> = vec![("message".into(), "Creating snapshot".into())]
            .into_iter()
            .collect();

        let jobs = serde_json::json!([{
            "class_name": "CreateSnapshotJob",
            "args": {
                "fsname": fsname,
                "name": name,
                "comment": comment,
                "fqdn": active_mgs_host_fqdn,
                "use_barrier": use_barrier.unwrap_or(false),
            }
        }]);
        let command_id: i32 = iml_job_scheduler_rpc::call(
            &context.rabbit_pool.get().await?,
            "run_jobs",
            vec![jobs],
            Some(kwargs),
        )
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

        get_command(&context.pg_pool, command_id)
            .await
            .map_err(|e| e.into())
    }

    #[graphql(arguments(
        fsname(description = "Filesystem snapshot was taken from"),
        name(description = "Name of the snapshot"),
        force(description = "Destroy the snapshot by force"),
    ))]
    /// Destroys an existing snapshot of an existing Lustre filesystem. Returns a `Command` to track progress.
    /// For the `Command` to succeed, the filesystem must be available.
    async fn destroy_snapshot(
        context: &Context,
        fsname: String,
        name: String,
        force: bool,
    ) -> juniper::FieldResult<Command> {
        let _ = fs_id_by_name(&context.pg_pool, &fsname).await?;
        let name = name.trim();
        validate_snapshot_name(name)?;

        let active_mgs_host_fqdn = active_mgs_host_fqdn(&fsname, &context.pg_pool)
            .await?
            .ok_or_else(|| {
                FieldError::new("Filesystem not found or MGS is not mounted", Value::null())
            })?;

        let kwargs: HashMap<String, String> =
            vec![("message".into(), "Destroying snapshot".into())]
                .into_iter()
                .collect();

        let jobs = serde_json::json!([{
            "class_name": "DestroySnapshotJob",
            "args": {
                "fsname": fsname,
                "name": name,
                "force": force,
                "fqdn": active_mgs_host_fqdn,
            }
        }]);
        let command_id: i32 = iml_job_scheduler_rpc::call(
            &context.rabbit_pool.get().await?,
            "run_jobs",
            vec![jobs],
            Some(kwargs),
        )
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

        get_command(&context.pg_pool, command_id)
            .await
            .map_err(|e| e.into())
    }

    #[graphql(arguments(
        fsname(description = "Filesystem snapshot was taken from"),
        name(description = "Name of the snapshot"),
    ))]
    /// Mounts an existing snapshot of an existing Lustre filesystem. Returns a `Command` to track progress.
    /// For the `Command` to succeed, the filesystem must be available.
    async fn mount_snapshot(
        context: &Context,
        fsname: String,
        name: String,
    ) -> juniper::FieldResult<Command> {
        let name = name.trim();
        validate_snapshot_name(name)?;

        let active_mgs_host_fqdn = active_mgs_host_fqdn(&fsname, &context.pg_pool)
            .await?
            .ok_or_else(|| {
                FieldError::new("Filesystem not found or MGS is not mounted", Value::null())
            })?;

        let kwargs: HashMap<String, String> = vec![("message".into(), "Mounting snapshot".into())]
            .into_iter()
            .collect();

        let jobs = serde_json::json!([{
            "class_name": "MountSnapshotJob",
            "args": {
                "fsname": fsname,
                "name": name,
                "fqdn": active_mgs_host_fqdn,
            }
        }]);
        let command_id: i32 = iml_job_scheduler_rpc::call(
            &context.rabbit_pool.get().await?,
            "run_jobs",
            vec![jobs],
            Some(kwargs),
        )
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

        get_command(&context.pg_pool, command_id)
            .await
            .map_err(|e| e.into())
    }
    #[graphql(arguments(
        fsname(description = "Filesystem snapshot was taken from"),
        name(description = "Name of the snapshot"),
    ))]
    /// Unmounts an existing snapshot of an existing Lustre filesystem. Returns a `Command` to track progress.
    /// For the `Command` to succeed, the filesystem must be available.
    async fn unmount_snapshot(
        context: &Context,
        fsname: String,
        name: String,
    ) -> juniper::FieldResult<Command> {
        let _ = fs_id_by_name(&context.pg_pool, &fsname).await?;
        let name = name.trim();
        validate_snapshot_name(name)?;

        let active_mgs_host_fqdn = active_mgs_host_fqdn(&fsname, &context.pg_pool)
            .await?
            .ok_or_else(|| {
                FieldError::new("Filesystem not found or MGS is not mounted", Value::null())
            })?;

        let kwargs: HashMap<String, String> =
            vec![("message".into(), "Unmounting snapshot".into())]
                .into_iter()
                .collect();

        let jobs = serde_json::json!([{
            "class_name": "UnmountSnapshotJob",
            "args": {
                "fsname": fsname,
                "name": name,
                "fqdn": active_mgs_host_fqdn,
            }
        }]);
        let command_id: i32 = iml_job_scheduler_rpc::call(
            &context.rabbit_pool.get().await?,
            "run_jobs",
            vec![jobs],
            Some(kwargs),
        )
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

        get_command(&context.pg_pool, command_id)
            .await
            .map_err(|e| e.into())
    }
    #[graphql(arguments(
        fsname(description = "The filesystem to create snapshots with"),
        interval(description = "How often a snapshot should be taken"),
        use_barrier(
            description = "Set write barrier before creating snapshot. The default value is `false`"
        ),
    ))]
    /// Creates a new snapshot interval.
    /// A recurring snapshot will be taken once the given `interval` expires for the given `fsname`.
    /// In order for the snapshot to be successful, the filesystem must be available.
    async fn create_snapshot_interval(
        context: &Context,
        fsname: String,
        interval: GraphQLDuration,
        use_barrier: Option<bool>,
    ) -> juniper::FieldResult<bool> {
        let _ = fs_id_by_name(&context.pg_pool, &fsname).await?;
        let maybe_id = sqlx::query!(
            r#"
                INSERT INTO snapshot_interval (
                    filesystem_name,
                    use_barrier,
                    interval
                )
                VALUES ($1, $2, $3)
                ON CONFLICT (filesystem_name, interval)
                DO NOTHING
                RETURNING id
            "#,
            fsname,
            use_barrier.unwrap_or_default(),
            PgInterval::try_from(interval.0)?,
        )
        .fetch_optional(&context.pg_pool)
        .await?
        .map(|x| x.id);

        if let Some(id) = maybe_id {
            configure_snapshot_timer(id, fsname, interval.0, use_barrier.unwrap_or_default())
                .await?;
        }

        Ok(true)
    }

    /// Removes an existing snapshot interval.
    /// This will also cancel any outstanding intervals scheduled by this rule.
    #[graphql(arguments(id(description = "The snapshot interval id"),))]
    async fn remove_snapshot_interval(context: &Context, id: i32) -> juniper::FieldResult<bool> {
        sqlx::query!("DELETE FROM snapshot_interval WHERE id=$1", id)
            .execute(&context.pg_pool)
            .await?;

        remove_snapshot_timer(id).await?;

        Ok(true)
    }
    #[graphql(arguments(
        fsname(description = "Filesystem name"),
        reserve_value(
            description = "Delete the oldest snapshot when available space falls below this value"
        ),
        reserve_unit(description = "The unit of measurement associated with the reserve_value"),
        keep_num(
            description = "The minimum number of snapshots to keep. This is to avoid deleting all snapshots while pursuiting the reserve goal"
        )
    ))]
    /// Creates a new snapshot retention policy for the given `fsname`.
    /// Snapshots will automatically be deleted (starting with the oldest)
    /// when free space falls below the defined reserve value and its associated unit.
    async fn create_snapshot_retention(
        context: &Context,
        fsname: String,
        reserve_value: i32,
        reserve_unit: ReserveUnit,
        keep_num: Option<i32>,
    ) -> juniper::FieldResult<bool> {
        let _ = fs_id_by_name(&context.pg_pool, &fsname).await?;
        sqlx::query!(
            r#"
                INSERT INTO snapshot_retention (
                    filesystem_name,
                    reserve_value,
                    reserve_unit,
                    keep_num
                )
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (filesystem_name)
                DO UPDATE SET
                reserve_value = EXCLUDED.reserve_value,
                reserve_unit = EXCLUDED.reserve_unit,
                keep_num = EXCLUDED.keep_num
            "#,
            fsname,
            reserve_value,
            reserve_unit as ReserveUnit,
            keep_num.unwrap_or(0)
        )
        .execute(&context.pg_pool)
        .await?;

        Ok(true)
    }
    /// Remove an existing snapshot retention policy.
    #[graphql(arguments(id(description = "The snapshot retention policy id")))]
    async fn remove_snapshot_retention(context: &Context, id: i32) -> juniper::FieldResult<bool> {
        sqlx::query!("DELETE FROM snapshot_retention WHERE id=$1", id)
            .execute(&context.pg_pool)
            .await?;

        Ok(true)
    }

    #[graphql(arguments(
        fsname(description = "Filesystem"),
        hotpool(description = "Name of Hot ostpool"),
        coldpool(description = "Name of Cold ostpool"),
        extendlayout(description = "Options to lfs mirror extend or hot -> cold files"),
        minage(description = "Minimum age of file before mirroring"),
        freehi(description = "Percent of free space when lpurge stops"),
        freelo(description = "Percent of free space when lpurge starts"),
    ))]
    /// Create Hotpool setups
    async fn create_hotpool(
        context: &Context,
        fsname: String,
        hotpool: String,
        coldpool: String,
        extendlayout: Option<String>,
        minage: i32,
        freehi: i32,
        freelo: i32,
    ) -> juniper::FieldResult<Command> {
        // Sanity check freehi and freelo
        if freehi > 99 || freehi <= 0 {
            return Err(FieldError::new(
                "Freehi out of range (0, 100)",
                Value::null(),
            ));
        }
        if freelo > 99 || freelo <= 0 {
            return Err(FieldError::new(
                "Freehi out of range (0, 100)",
                Value::null(),
            ));
        }
        if freehi < freelo {
            return Err(FieldError::new("Freehi less than freelo", Value::null()));
        }

        let fsid = sqlx::query!(
            r#"
                SELECT id FROM chroma_core_managedfilesystem WHERE name=$1 and not_deleted = 't'
            "#,
            fsname,
        )
        .fetch_optional(&context.pg_pool)
        .await?
        .map(|x| x.id)
        .ok_or_else(|| FieldError::new("Filesystem not found", Value::null()))?;

        let coldid = poolid(fsid, coldpool, &context.pg_pool)
            .await?
            .ok_or_else(|| FieldError::new("Cold OstPool not found", Value::null()))?;

        let hotid = poolid(fsid, hotpool, &context.pg_pool)
            .await?
            .ok_or_else(|| FieldError::new("Hot OstPool not found", Value::null()))?;

        let mut hp_data: HashMap<String, String> = vec![
            ("filesystem".into(), format!("{}", fsid)),
            ("hotpool".into(), format!("{}", hotid)),
            ("coldpool".into(), format!("{}", coldid)),
            ("minage".into(), format!("{}", minage)),
            ("freehi".into(), format!("{}", freehi)),
            ("freelo".into(), format!("{}", freelo)),
        ]
        .into_iter()
        .collect();

        if let Some(value) = extendlayout {
            hp_data.insert("extendlayout".into(), value);
        }

        let command_id: i32 = iml_job_scheduler_rpc::call(
            &context.rabbit_pool.get().await?,
            "create_hotpool",
            vec![hp_data],
            None,
        )
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

        get_command(&context.pg_pool, command_id)
            .await
            .map_err(|e| e.into())
    }

    #[graphql(arguments(
        fsname(description = "Filesystem name"),
        state(description = "New state to transition to"),
    ))]
    async fn set_hotpool_state(
        context: &Context,
        fsname: String,
        state: HotpoolState,
    ) -> juniper::FieldResult<Command> {
        let hpid = hpid(fsname, &context.pg_pool).await?.ok_or_else(|| {
            FieldError::new(
                "Hotpool Configuration not found for filesystem",
                Value::null(),
            )
        })?;

        let obj = serde_json::json!([(
            (
                "chroma_core".to_string(),
                "hotpoolconfiguration".to_string()
            ),
            hpid,
            state.to_string()
        )]);
        let kwargs: HashMap<String, String> = vec![
            (
                "message".into(),
                format!("Setting Hotpool state to {}", &state),
            ),
            ("run".into(), "True".into()),
        ]
        .into_iter()
        .collect();

        let command_id: i32 = iml_job_scheduler_rpc::call(
            &context.rabbit_pool.get().await?,
            "set_state",
            vec![obj],
            Some(kwargs),
        )
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

        get_command(&context.pg_pool, command_id)
            .await
            .map_err(|e| e.into())
    }
    #[graphql(arguments(fsname(description = "Filesystem"),))]
    async fn destroy_hotpool(context: &Context, fsname: String) -> juniper::FieldResult<Command> {
        let hpid = hpid(fsname, &context.pg_pool)
            .await?
            .ok_or_else(|| FieldError::new("Hotpool Configuration not found", Value::null()))?;

        let command_id: i32 = iml_job_scheduler_rpc::call(
            &context.rabbit_pool.get().await?,
            "remove_hotpool",
            vec![hpid],
            None,
        )
        .map_err(ImlApiError::ImlJobSchedulerRpcError)
        .await?;

        get_command(&context.pg_pool, command_id)
            .await
            .map_err(|e| e.into())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct CommandTmpRecord {
    pub cancelled: bool,
    pub complete: bool,
    pub created_at: DateTime<Utc>,
    pub errored: bool,
    pub id: i32,
    pub job_ids: Option<Vec<i32>>,
    pub message: String,
}

fn to_command(x: CommandTmpRecord) -> Command {
    Command {
        id: x.id,
        cancelled: x.cancelled,
        complete: x.complete,
        errored: x.errored,
        created_at: x.created_at.format("%Y-%m-%dT%T%.6f").to_string(),
        jobs: {
            x.job_ids
                .unwrap_or_default()
                .into_iter()
                .map(|job_id: i32| format!("/api/{}/{}/", Job::<()>::endpoint_name(), job_id))
                .collect::<Vec<_>>()
        },
        logs: "".to_string(),
        message: x.message.clone(),
        resource_uri: format!("/api/{}/{}/", Command::endpoint_name(), x.id),
    }
}

pub(crate) type Schema = RootNode<'static, QueryRoot, MutationRoot, EmptySubscription<Context>>;

pub(crate) struct Context {
    pub(crate) pg_pool: PgPool,
    pub(crate) rabbit_pool: Pool,
}

impl juniper::Context for Context {}

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
        .and(schema_filter.clone())
        .and(ctx_filter)
        .and(warp::body::json())
        .and_then(graphql);

    let graphiql_route = warp::path!("graphiql")
        .and(warp::get())
        .map(|| warp::reply::html(graphiql_source("graphql", None)));

    let graphql_schema_route = warp::path!("graphql_schema")
        .and(warp::get())
        .and(schema_filter)
        .map(|schema: Arc<Schema>| schema.as_schema_language());

    graphql_route.or(graphiql_route).or(graphql_schema_route)
}

async fn hpid(fsname: String, pgpool: &PgPool) -> Result<Option<i32>, iml_postgres::sqlx::Error> {
    let xs = sqlx::query!(
        r#"
            SELECT hp.id AS id FROM chroma_core_hotpoolconfiguration hp
            INNER JOIN chroma_core_managedfilesystem fs ON hp.filesystem_id = fs.id
            WHERE hp.not_deleted = 't' AND fs.not_deleted = 't' AND fs.name=$1
        "#,
        fsname
    )
    .fetch_optional(pgpool)
    .await?
    .map(|x| x.id);

    Ok(xs)
}

async fn poolid(
    fsid: i32,
    poolname: String,
    pgpool: &PgPool,
) -> Result<Option<i32>, iml_postgres::sqlx::Error> {
    let rc = sqlx::query!(
        r#"
            SELECT id FROM chroma_core_ostpool WHERE filesystem_id=$1 AND name=$2 AND not_deleted = 't'
        "#,
        fsid,
        poolname,
    )
    .fetch_optional(pgpool)
    .await?
        .map(|x| x.id);
    Ok(rc)
}

async fn get_fs_target_resources(
    pool: &PgPool,
    fs_name: Option<String>,
) -> Result<Vec<TargetResource>, ImlApiError> {
    let banned_resources = get_banned_targets(pool).await?;

    let xs = sqlx::query!(r#"
            SELECT rh.cluster_id, r.id, t.name, t.mount_path, t.filesystems, array_agg(DISTINCT rh.host_id) AS "cluster_hosts!"
            FROM target t
            INNER JOIN corosync_resource r ON r.mount_point = t.mount_path
            INNER JOIN corosync_resource_managed_host rh ON rh.corosync_resource_id = r.id AND rh.host_id = ANY(t.host_ids)
            WHERE CARDINALITY(t.filesystems) > 0
            GROUP BY rh.cluster_id, t.name, r.id, t.mount_path, t.filesystems
        "#)
            .fetch(pool)
            .try_filter(|x| {
                let fs_name2 = fs_name.clone();
                let filesystems = x.filesystems.clone();
                async move {
                    match fs_name2 {
                        None => true,
                        Some(fs) => filesystems.contains(&fs)
                    }
                }
            })
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
                    fs_name: fs_name.as_ref().unwrap_or_else(|| &x.filesystems[0]).to_string(),
                    name: x.name,
                    resource_id: x.id,
                    cluster_hosts: x.cluster_hosts
                }
            }).try_collect()
            .await?;

    Ok(xs)
}

async fn get_fs_cluster_hosts(
    pool: &PgPool,
    fs_name: String,
) -> Result<Vec<Vec<String>>, ImlApiError> {
    let xs = get_fs_target_resources(pool, Some(fs_name))
        .await?
        .into_iter()
        .group_by(|x| x.cluster_id);

    let xs: Vec<Vec<i32>> = xs.into_iter().fold(vec![], |mut acc, (_, xs)| {
        let xs: HashSet<i32> = xs.map(|x| x.cluster_hosts).flatten().collect();

        acc.push(xs.into_iter().collect());

        acc
    });
    let xs = xs.into_iter().map(|idset| async move {
        let fqdns = idset
            .into_iter()
            .map(|x| async move { fqdn_by_host_id(pool, x).await });
        join_all(fqdns)
            .await
            .into_iter()
            .filter_map(|x| x.ok())
            .collect()
    });

    Ok(join_all(xs).await)
}

async fn get_banned_targets(pool: &PgPool) -> Result<Vec<BannedTargetResource>, ImlApiError> {
    let xs = sqlx::query!(
        r#"
            SELECT b.id, b.resource, b.node, b.cluster_id, nh.host_id, t.mount_point
            FROM corosync_resource_bans b
            INNER JOIN corosync_node_managed_host nh ON (nh.corosync_node_id).name = b.node
            AND nh.cluster_id = b.cluster_id
            INNER JOIN corosync_resource t ON t.id = b.resource AND b.cluster_id = t.cluster_id
            WHERE t.mount_point != NULL
        "#
    )
    .fetch(pool)
    .map_ok(|x| BannedTargetResource {
        resource: x.resource,
        cluster_id: x.cluster_id,
        host_id: x.host_id,
        mount_point: x.mount_point,
    })
    .try_collect()
    .await?;

    Ok(xs)
}

async fn get_banned_resources(pool: &PgPool) -> Result<Vec<BannedResource>, ImlApiError> {
    let xs = sqlx::query_as!(
        BannedResource,
        r#"
            SELECT id, cluster_id, resource, node, weight, master_only
            FROM corosync_resource_bans
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(xs)
}

fn validate_snapshot_name(x: &str) -> Result<(), FieldError> {
    if x.contains(' ') {
        Err(FieldError::new(
            "Snapshot name cannot contain spaces",
            Value::null(),
        ))
    } else if x.contains('*') {
        Err(FieldError::new(
            "Snapshot name cannot contain * character",
            Value::null(),
        ))
    } else if x.contains('/') {
        Err(FieldError::new(
            "Snapshot name cannot contain / character",
            Value::null(),
        ))
    } else if x.contains('&') {
        Err(FieldError::new(
            "Snapshot name cannot contain & character",
            Value::null(),
        ))
    } else {
        Ok(())
    }
}

async fn fs_id_by_name(pool: &PgPool, name: &str) -> Result<i32, juniper::FieldError> {
    sqlx::query!(
        "SELECT id FROM chroma_core_managedfilesystem WHERE name=$1 and not_deleted = 't'",
        name
    )
    .fetch_optional(pool)
    .await?
    .map(|x| x.id)
    .ok_or_else(|| FieldError::new(format!("Filesystem {} not found", name), Value::null()))
}

async fn insert_task(
    name: &str,
    state: &str,
    single_runner: bool,
    keep_failed: bool,
    actions: &[String],
    args: serde_json::Value,
    fs_id: i32,
    pool: &PgPool,
) -> Result<Task, ImlApiError> {
    let x = sqlx::query_as!(
        Task,
        r#"
                INSERT INTO chroma_core_task (
                    name,
                    start,
                    state,
                    fids_total,
                    fids_completed,
                    fids_failed,
                    data_transfered,
                    single_runner,
                    keep_failed,
                    actions,
                    args,
                    filesystem_id
                )
                VALUES (
                    $1,
                    now(),
                    $2,
                    0,
                    0,
                    0,
                    0,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7
                )
                RETURNING *
            "#,
        name,
        state,
        single_runner,
        keep_failed,
        actions,
        args,
        fs_id
    )
    .fetch_one(pool)
    .await?;

    Ok(x)
}

async fn run_jobs<T: std::fmt::Debug + serde::Serialize>(
    msg: impl ToString,
    jobs: Vec<SendJob<'_, T>>,
    rabbit_pool: &Pool,
) -> Result<i32, ImlApiError> {
    let kwargs: HashMap<String, String> = vec![("message".into(), msg.to_string())]
        .into_iter()
        .collect();

    let id: i32 = iml_job_scheduler_rpc::call(
        &rabbit_pool.get().await.map_err(ImlRabbitError::PoolError)?,
        "run_jobs",
        vec![jobs],
        Some(kwargs),
    )
    .map_err(ImlApiError::ImlJobSchedulerRpcError)
    .await?;

    Ok(id)
}

fn create_task_job<'a>(task_id: i32) -> SendJob<'a, HashMap<String, serde_json::Value>> {
    SendJob {
        class_name: "CreateTaskJob".into(),
        args: vec![("task_id".into(), serde_json::json!(task_id))]
            .into_iter()
            .collect(),
    }
}
