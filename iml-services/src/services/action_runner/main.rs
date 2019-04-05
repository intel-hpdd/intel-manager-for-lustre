// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::{future::Future, lazy, stream::Stream, sync::oneshot};
use http::{Response, StatusCode};
use hyper::Body;
use iml_manager_messaging::send_agent_message;
use iml_rabbit::{connect_to_rabbit, get_cloned_conns, TcpClient};
use iml_services::{
    service_queue::consume_service_queue,
    services::action_runner::{
        data::{
            await_session, insert_action_in_flight, ActionInFlight, ManagerCommand, SessionToRpcs,
            Sessions, Shared,
        },
        error::ActionRunnerError,
    },
};
use iml_wire_types::{Action, ActionResult, Fqdn, Id, ManagerMessage, PluginMessage, PluginName};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    os::unix::{io::FromRawFd, net::UnixListener as NetUnixListener},
    sync::Arc,
    time::Duration,
};
use tokio::{net::UnixListener, reactor::Handle};
use warp::{self, Filter as _};

fn main() {
    env_logger::init();

    let (exit, valve) = tokio_runtime_shutdown::shared_shutdown();

    let sessions: Shared<Sessions> = Arc::new(Mutex::new(HashMap::new()));
    let rpcs: Shared<SessionToRpcs> = Arc::new(Mutex::new(HashMap::new()));

    fn msg_factory<'a>(
        session_id: &'a Id,
        fqdn: &'a Fqdn,
    ) -> impl Fn(serde_json::Value) -> ManagerMessage + 'a {
        move |body| ManagerMessage::Data {
            session_id: session_id.clone(),
            fqdn: fqdn.clone(),
            plugin: PluginName("action_runner".to_string()),
            body,
        }
    }

    fn hande_agent_data(
        client: TcpClient,
        m: PluginMessage,
        sessions: &Shared<Sessions>,
        rpcs: Shared<SessionToRpcs>,
    ) {
        match m {
            PluginMessage::SessionCreate {
                fqdn, session_id, ..
            } => {
                let mut sessions = sessions.lock();

                if let Some(old_id) = sessions.insert(fqdn.clone(), session_id.clone()) {
                    if let Some(xs) = rpcs.lock().remove(&old_id) {
                        let create_msg = msg_factory(&session_id, &fqdn);

                        for action_in_flight in xs.values() {
                            let msg =
                                create_msg(serde_json::to_value(&action_in_flight.action).unwrap());

                            send_agent_message(client.clone(), msg);
                        }
                        rpcs.lock().insert(session_id.clone(), xs);
                    };
                };
            }
            PluginMessage::SessionTerminate {
                fqdn, session_id, ..
            } => {
                if let Some(old_id) = sessions.lock().remove(&fqdn) {
                    if let Some(mut xs) = rpcs.lock().remove(&old_id) {
                        for (_, action_in_flight) in xs.drain() {
                            let msg = Err(format!(
                                "Communications error, Node: {}, Reason: session terminated",
                                fqdn
                            ));

                            action_in_flight.complete(msg);
                        }
                    }
                }
            }
            PluginMessage::Data {
                fqdn,
                session_id,
                body,
                ..
            } => match sessions.lock().get(&fqdn) {
                Some(held_session) if held_session == &session_id => {
                    log::info!("good session {:?}/{:?}", fqdn, session_id);

                    match rpcs.lock().get_mut(&session_id) {
                        Some(rs) => {
                            let result: Result<ActionResult, String> =
                                serde_json::from_value(body).unwrap();

                            let result = result.unwrap();

                            match rs.remove(&result.id) {
                                Some(action_in_flight) => {
                                    action_in_flight.complete(result.result);
                                }
                                None => {
                                    log::error!(
                                        "Response received from UNKNOWN RPC of (id: {})",
                                        result.id
                                    );
                                }
                            }
                        }
                        None => {}
                    }
                }
                Some(held_session) => {
                    log::info!(
                        "cancelling session {:?}/{:?} (replaced by {:?})",
                        fqdn,
                        held_session,
                        session_id
                    );

                    if let Some(old_id) = sessions.lock().remove(&fqdn) {
                        if let Some(mut xs) = rpcs.lock().remove(&old_id) {
                            for (_, action_in_flight) in xs.drain() {
                                let msg = Err(format!(
                                    "Communications error, Node: {}, Reason: session terminated",
                                    fqdn
                                ));

                                action_in_flight.complete(msg);
                            }
                        }
                    }
                }
                None => {
                    log::info!("unknown session {:?}/{:?}", fqdn, session_id);
                }
            },
        };
    }

    tokio::run(lazy(move || {
        let addr = unsafe { NetUnixListener::from_raw_fd(3) };

        let listener = UnixListener::from_std(addr, &Handle::default())
            .expect("Unable to bind Unix Domain Socket fd");

        let log = warp::log("iml_action_runner::request_socket");

        let sessions2 = Arc::clone(&sessions);
        let session_filter = warp::any().map(move || Arc::clone(&sessions2));

        let rpcs2 = Arc::clone(&rpcs);
        let rpc_filter = warp::any().map(move || Arc::clone(&rpcs2));

        let (tx, fut) = get_cloned_conns(connect_to_rabbit());

        tokio::spawn(fut);

        let client_filter = warp::any().and_then(move || {
            let (tx2, rx2) = oneshot::channel();

            tx.unbounded_send(tx2).unwrap();

            rx2.map_err(warp::reject::custom)
        });

        let deps = session_filter.and(rpc_filter).and(client_filter);

        let routes = warp::post2()
            .and(deps)
            .and(warp::body::json())
            .and_then(
                |s: Shared<Sessions>,
                 r: Shared<SessionToRpcs>,
                 client: TcpClient,
                 m: ManagerCommand| {
                    await_session(m.fqdn.clone(), s, Duration::from_secs(30))
                        .from_err()
                        .and_then(move |id| {
                            let (tx, rx) = oneshot::channel();

                            let action_id = m.action_id.clone();

                            let fqdn = m.fqdn.clone();

                            let action: Action = m.into();

                            let msg = ManagerMessage::Data {
                                session_id: id.clone(),
                                fqdn,
                                plugin: PluginName("action_runner".to_string()),
                                body: action.clone().into(),
                            };

                            send_agent_message(client.clone(), msg)
                                .map(|_| {
                                    let af = ActionInFlight::new(action, tx);

                                    insert_action_in_flight(id, action_id, af, r);
                                })
                                .and_then(|_| rx.from_err())
                        })
                        .map_err(warp::reject::custom)
                },
            )
            .map(|_| {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::wrap_stream(futures::stream::once::<
                        String,
                        ActionRunnerError,
                    >(Ok("1".to_string()))))
            })
            .with(log);

        tokio::spawn(warp::serve(routes).serve_incoming(valve.wrap(listener.incoming())));

        iml_rabbit::connect_to_rabbit()
            .and_then(move |client| {
                exit.wrap(valve.wrap(consume_service_queue(
                    client.clone(),
                    "rust_agent_action_runner_rx",
                )))
                .for_each(move |m: PluginMessage| {
                    log::info!("Got some actiony data: {:?}", m);

                    hande_agent_data(client.clone(), m, &sessions, Arc::clone(&rpcs));

                    Ok(())
                })
            })
            .map_err(|e| log::error!("An error occured (agent side): {:?}", e))
    }));
}
