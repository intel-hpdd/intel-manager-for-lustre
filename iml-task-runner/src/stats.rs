// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::error::ImlTaskRunnerError;
use bigdecimal::{BigDecimal, ToPrimitive};
use iml_influx::{Client, Point, Points, Precision, Value};
use iml_manager_client::Url;
use iml_manager_env::{get_influxdb_addr, get_influxdb_metrics_db};
use iml_postgres::sqlx::{self, PgPool};
use iml_tracing::tracing;
use std::time::Duration;
use tokio::time;

struct TaskStats {
    actions: Vec<String>,
    fs_name: String,
    fids_total: BigDecimal,
    fids_completed: BigDecimal,
    fids_failed: BigDecimal,
}

const DELAY: Duration = Duration::from_secs(60);

pub(crate) async fn collector(pool: PgPool) -> Result<(), ImlTaskRunnerError> {
    let mut interval = time::interval(DELAY);

    let influx_url: String = format!("http://{}", get_influxdb_addr());
    tracing::debug!("influx_url: {}", &influx_url);

    loop {
        interval.tick().await;

        let stats: Vec<TaskStats> = sqlx::query_as!(
            TaskStats,
            r#"SELECT COALESCE(SUM(fids_total), 0) AS "fids_total!",
COALESCE(SUM(fids_completed), 0) AS "fids_completed!",
COALESCE(SUM(fids_failed), 0) AS "fids_failed!",
actions,
fs_name FROM chroma_core_task GROUP BY actions,fs_name"#
        )
        .fetch_all(&pool)
        .await?;

        if stats.is_empty() {
            continue;
        }

        let client = Client::new(
            Url::parse(&influx_url).expect("Influx URL is invalid."),
            get_influxdb_metrics_db(),
        );

        let xs = stats
            .iter()
            .filter_map(|stat| {
                if let Some(action) = stat.actions.first() {
                    Some(
                        Point::new("task")
                            .add_tag("action", Value::String(action.to_string()))
                            .add_tag("filesystem", Value::String(stat.fs_name.clone()))
                            .add_field(
                                "fids_completed",
                                Value::Integer(stat.fids_completed.to_i64().unwrap_or(0)),
                            )
                            .add_field(
                                "fids_failed",
                                Value::Integer(stat.fids_failed.to_i64().unwrap_or(0)),
                            )
                            .add_field(
                                "fids_total",
                                Value::Integer(stat.fids_total.to_i64().unwrap_or(0)),
                            ),
                    )
                } else {
                    None
                }
            })
            .collect();
        let points = Points::create_new(xs);

        if let Err(e) = client
            .write_points(points, Some(Precision::Seconds), None)
            .await
        {
            tracing::error!("Error writing series to influxdb: {}", e);
        }
    }
}
