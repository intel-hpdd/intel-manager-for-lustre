// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    agent_error::ImlAgentError,
    daemon_plugins::{get_plugin, DaemonPlugins},
    http_comms::{
        agent_client::AgentClient,
        session::{Session, Sessions},
    },
};
use futures::{FutureExt, TryFutureExt};
use iml_wire_types::ManagerMessage;
use std::time::Duration;
use tokio::time::delay_for;
use tracing::{error, warn};

async fn get_delivery(
    sessions: Sessions,
    agent_client: AgentClient,
    registry: &DaemonPlugins,
) -> Result<(), ImlAgentError> {
    let msgs = agent_client.clone().get().map_ok(|x| x.messages).await?;

    for x in msgs {
        let sessions2 = sessions.clone();
        let agent_client2 = agent_client.clone();

        tracing::debug!("--> Delivery from manager {:?}", x);

        match x {
            ManagerMessage::SessionCreateResponse {
                plugin, session_id, ..
            } => {
                let plugin_instance = get_plugin(&plugin, &registry)?;
                let mut s = Session::new(plugin.clone(), session_id, plugin_instance);
                let (rx, fut) = s.start();

                sessions2.insert_session(plugin.clone(), s, rx).await?;

                let agent_client3 = agent_client2.clone();

                tokio::spawn(
                    async move {
                        if let Some((info, output)) = fut.await? {
                            agent_client3.send_data(info, output).await?;
                        }

                        Ok(())
                    }
                    .then(move |r: Result<(), ImlAgentError>| async move {
                        match r {
                            Ok(_) => (),
                            Err(e) => {
                                tracing::warn!("Error during session start {:?}", e);

                                sessions2
                                    .terminate_session(&plugin)
                                    .await
                                    .unwrap_or_else(|e| {
                                        tracing::warn!("Error terminating session, {}", e)
                                    });
                            }
                        }
                    }),
                );
            }
            ManagerMessage::Data { plugin, body, .. } => {
                tokio::spawn(
                    async move {
                        let r = { sessions2.message(&plugin, body) };

                        if let Some(x) = r.await {
                            let agent_client3 = agent_client2.clone();

                            let (info, x) = x?;

                            agent_client3.send_data(info, x).await?;
                        }

                        Ok(())
                    }
                    .map_err(|e: ImlAgentError| error!("{}", e)),
                );
            }
            ManagerMessage::SessionTerminate { plugin, .. } => {
                sessions.terminate_session(&plugin).await?
            }
            ManagerMessage::SessionTerminateAll { .. } => sessions.terminate_all_sessions().await?,
        }
    }

    Ok(())
}

/// Continually polls the manager for any incoming commands using a loop.
pub async fn create_reader(
    sessions: Sessions,
    agent_client: AgentClient,
    registry: DaemonPlugins,
) -> Result<(), ImlAgentError> {
    loop {
        match get_delivery(sessions.clone(), agent_client.clone(), &registry).await {
            Ok(_) => continue,
            Err(ImlAgentError::Reqwest(e)) => {
                warn!("Got a manager read Error {:?}. Will retry in 5 seconds.", e);

                sessions.terminate_all_sessions().await?;

                delay_for(Duration::from_secs(5)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
