// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    agent_error::ImlAgentError,
    http_comms::{
        agent_client::AgentClient,
        session::{Sessions, State},
    },
};
use futures::{
    future::{self, Either},
    Future, FutureExt, TryFutureExt,
};
use iml_wire_types::PluginName;
use std::{
    ops::DerefMut,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::interval;
use tracing::error;

/// Given a `Session` wrapped in some `State`
/// this function will handle the state and move it to it's next state.
///
fn handle_state(
    state: &mut State,
    agent_client: AgentClient,
    sessions: Sessions,
    name: PluginName,
    now: Instant,
) -> impl Future<Output = Result<(), ImlAgentError>> {
    tracing::trace!("handling state for {:?}: {:?}, ", name, state);

    match state {
        State::Active(a) if a.instant <= now => {
            let (rx, fut) = a.session.poll();

            a.in_flight = Some(rx);

            Either::Left(
                fut.and_then(move |x| async move {
                    if let Some((info, output)) = x {
                        agent_client.send_data(info, output).await?;
                    }

                    Ok(())
                })
                .then(move |r| async move {
                    match r {
                        Ok(_) => {
                            sessions.reset_active(&name).await;
                            Ok(())
                        }
                        Err(_) => sessions.terminate_session(&name).await,
                    }
                }),
            )
        }
        _ => Either::Right(future::ok(())),
    }
}

/// Given some `Sessions`, this fn will poll them once per second.
///
/// A `Session` or other `State` will only be handled if their internal timers have passed the tick of this
/// internal interval `Stream`.
pub async fn create_poller(agent_client: AgentClient, sessions: Sessions) {
    let mut s = interval(Duration::from_secs(1));

    loop {
        let now = s.tick().await.into_std();
        tracing::trace!("interval triggered for {:?}", now);

        for (name, locked) in sessions.0.iter() {
            let locked = Arc::clone(locked);
            let mut write_lock = locked.write().await;

            let state: &mut State = write_lock.deref_mut();

            match state {
                State::Empty(wait) if *wait <= now => {
                    state.convert_to_pending();

                    let sessions = sessions.clone();
                    let name = name.clone();

                    tracing::info!("sending session create request for {}", name);

                    let r = agent_client.create_session(name.clone());

                    tokio::spawn(async move {
                        if let Err(e) = r.await {
                            tracing::info!("session create request for {} failed: {:?}", name, e);

                            sessions.reset_empty(&name).await;
                        };

                        Ok::<_, ImlAgentError>(())
                    });
                }
                _ => (),
            };
        }

        for (name, state) in sessions.0.iter() {
            let fut = handle_state(
                Arc::clone(&state).write().await.deref_mut(),
                agent_client.clone(),
                sessions.clone(),
                name.clone(),
                now,
            );

            tokio::spawn(async move {
                if let Err(e) = fut.await {
                    error!("{}", e);
                };
            });
        }
    }
}
