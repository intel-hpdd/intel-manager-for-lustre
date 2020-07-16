// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use env::{MANAGER_URL, PEM};
use futures::{FutureExt, TryFutureExt};
use iml_agent::{
    agent_error::Result,
    daemon_plugins, env,
    http_comms::{agent_client::AgentClient, crypto_client, session},
    poller, reader,
};

#[tokio::main]
async fn main() -> Result<()> {
    iml_tracing::init();

    tracing::info!("Starting Rust agent_daemon");

    let message_endpoint = MANAGER_URL.join("/agent2/message/")?;

    let start_time = chrono::Utc::now().format("%Y-%m-%dT%T%.6f%:zZ").to_string();

    let identity = crypto_client::get_id(&PEM)?;
    let client = crypto_client::create_client(identity)?;

    let agent_client =
        AgentClient::new(start_time.clone(), message_endpoint.clone(), client.clone());

    let registry = daemon_plugins::plugin_registry();
    let registry_keys: Vec<iml_wire_types::PluginName> = registry.keys().cloned().collect();
    let sessions = session::Sessions::new(&registry_keys);

    tokio::spawn(
        reader::create_reader(sessions.clone(), agent_client.clone(), registry)
            .map_err(|e| {
                tracing::error!("{}", e);
            })
            .map(drop),
    );

    poller::create_poller(agent_client, sessions).await;

    Ok(())
}
