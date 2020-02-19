// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::future::try_join_all;
use futures_util::stream::TryStreamExt;
use lustre_collector::types::Record;
use iml_service_queue::service_queue::consume_data;
use tracing_subscriber::{fmt::Subscriber, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();

    let mut s = consume_data::<Record>("rust_agent_stats_rx");

    while let Some(xs) = s.try_next().await? {
        tracing::info!("Incoming stats: {:?}");
    }

    Ok(())
}
