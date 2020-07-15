// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::TryStreamExt;
use iml_postgres::{alert, get_db_pool, sqlx};
use iml_service_queue::service_queue::consume_data;
use iml_wire_types::{db::ManagedHostRecord, time::State, AlertRecordType, AlertSeverity};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    iml_tracing::init();

    let pool = get_db_pool(5).await?;

    let rabbit_pool = iml_rabbit::connect_to_rabbit(1);

    let conn = iml_rabbit::get_conn(rabbit_pool).await?;

    let ch = iml_rabbit::create_channel(&conn).await?;

    let mut s = consume_data::<State>(&ch, "rust_agent_ntp_rx");

    while let Some((fqdn, state)) = s.try_next().await? {
        tracing::debug!("fqdn: {:?} state: {:?}", fqdn, state);

        let host: Option<ManagedHostRecord> = sqlx::query_as!(
            ManagedHostRecord,
            "select * from chroma_core_managedhost where fqdn = $1 and not_deleted = 't'",
            fqdn.to_string()
        )
        .fetch_optional(&pool)
        .await?;

        let host = match host {
            Some(x) => x,
            None => {
                tracing::warn!("Host '{}' is unknown", fqdn);

                continue;
            }
        };

        match state {
            State::Synced => {
                alert::lower(
                    &pool,
                    vec![
                        AlertRecordType::TimeOutOfSyncAlert,
                        AlertRecordType::NoTimeSyncAlert,
                        AlertRecordType::MultipleTimeSyncAlert,
                        AlertRecordType::UnknownTimeSyncAlert,
                    ],
                    host.id,
                )
                .await?;
            }
            State::None => {
                alert::lower(
                    &pool,
                    vec![
                        AlertRecordType::TimeOutOfSyncAlert,
                        AlertRecordType::MultipleTimeSyncAlert,
                        AlertRecordType::UnknownTimeSyncAlert,
                    ],
                    host.id,
                )
                .await?;

                if host.is_setup() {
                    alert::raise(
                        &pool,
                        AlertRecordType::NoTimeSyncAlert,
                        format!("No running time sync clients found on {}", fqdn),
                        host.content_type_id.expect("Host has no content_type_id"),
                        None,
                        AlertSeverity::ERROR,
                        host.id,
                    )
                    .await?;
                }
            }
            State::Multiple => {
                alert::lower(
                    &pool,
                    vec![
                        AlertRecordType::TimeOutOfSyncAlert,
                        AlertRecordType::NoTimeSyncAlert,
                        AlertRecordType::UnknownTimeSyncAlert,
                    ],
                    host.id,
                )
                .await?;

                if host.is_setup() {
                    alert::raise(
                        &pool,
                        AlertRecordType::MultipleTimeSyncAlert,
                        format!("Multiple running time sync clients found on {}", fqdn),
                        host.content_type_id.expect("Host has no content_type_id"),
                        None,
                        AlertSeverity::ERROR,
                        host.id,
                    )
                    .await?;
                }
            }
            State::Unsynced(_) => {
                alert::lower(
                    &pool,
                    vec![
                        AlertRecordType::MultipleTimeSyncAlert,
                        AlertRecordType::NoTimeSyncAlert,
                        AlertRecordType::UnknownTimeSyncAlert,
                    ],
                    host.id,
                )
                .await?;

                if host.is_setup() {
                    alert::raise(
                        &pool,
                        AlertRecordType::TimeOutOfSyncAlert,
                        format!("Time is out of sync on server {}", fqdn),
                        host.content_type_id.expect("Host has no content_type_id"),
                        None,
                        AlertSeverity::ERROR,
                        host.id,
                    )
                    .await?;
                }
            }
            State::Unknown => {
                alert::lower(
                    &pool,
                    vec![
                        AlertRecordType::MultipleTimeSyncAlert,
                        AlertRecordType::NoTimeSyncAlert,
                        AlertRecordType::TimeOutOfSyncAlert,
                    ],
                    host.id,
                )
                .await?;

                if host.is_setup() {
                    alert::raise(
                        &pool,
                        AlertRecordType::UnknownTimeSyncAlert,
                        format!("Unable to determine time sync status on {}", fqdn),
                        host.content_type_id.expect("Host has no content_type_id"),
                        None,
                        AlertSeverity::ERROR,
                        host.id,
                    )
                    .await?;
                }
            }
        };
    }

    Ok(())
}
