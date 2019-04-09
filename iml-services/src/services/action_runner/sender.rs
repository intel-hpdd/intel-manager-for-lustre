// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::services::action_runner::data::{
    await_session, create_data_message, has_action_in_flight, insert_action_in_flight,
    remove_action_in_flight, ActionInFlight, SessionToRpcs, Sessions, Shared,
};
use futures::{
    future::{self, Either},
    sync::oneshot,
    Future,
};
use iml_manager_messaging::send_agent_message;
use iml_rabbit::{connect_to_rabbit, get_cloned_conns, TcpClient};
use iml_wire_types::{Action, ActionId, Fqdn, Id, ManagerMessage};
use std::{sync::Arc, time::Duration};
use warp::{self, Filter};

/// Attempts to cancel an `ActionInFlight`.
///
/// This fn will complete the `ActionInFlight` if it was successful.
/// It will also remove the `ActionInFlight` from the rpcs on success.
///
/// If unsuccessful, this fn will keep the `ActionInFlight` within the rpcs.
fn cancel_running_action(
    client: TcpClient,
    msg: ManagerMessage,
    session_id: Id,
    action_id: ActionId,
    session_to_rpcs: Shared<SessionToRpcs>,
) -> impl Future<Item = Result<serde_json::Value, String>, Error = failure::Error> {
    let has_action_in_flight =
        { has_action_in_flight(&session_id, &action_id, &session_to_rpcs.lock()) };

    if has_action_in_flight {
        Either::A(
            send_agent_message(client.clone(), "", msg)
                .inspect(move |_| {
                    if let Some(action_in_flight) = remove_action_in_flight(
                        &session_id,
                        &action_id,
                        &mut session_to_rpcs.lock(),
                    ) {
                        action_in_flight
                            .complete(Ok(serde_json::Value::Null))
                            .unwrap();
                    }
                })
                .map(|_| Ok(serde_json::Value::Null)),
        )
    } else {
        log::info!(
            "Action {:?} not found, perhaps it was already cancelled.",
            action_id
        );
        Either::B(future::ok(Ok(serde_json::Value::Null)))
    }
}

pub fn create_client_filter() -> (
    impl Future<Item = (), Error = ()>,
    impl Filter<Extract = (TcpClient,), Error = warp::Rejection> + Clone,
) {
    let (tx, fut) = get_cloned_conns(connect_to_rabbit());

    let filter = warp::any().and_then(move || {
        let (tx2, rx2) = oneshot::channel();

        tx.unbounded_send(tx2).unwrap();

        rx2.map_err(warp::reject::custom)
    });

    (fut, filter)
}

pub fn sender(
    exchange_name: impl Into<String>,
    sessions: Shared<Sessions>,
    session_to_rpcs: Shared<SessionToRpcs>,
    client_filter: impl Filter<Extract = (TcpClient,), Error = warp::Rejection> + Clone + Send,
) -> impl Filter<Extract = (Result<serde_json::Value, String>,), Error = warp::Rejection> + Clone {
    let exchange_name = exchange_name.into();

    let sessions_filter = warp::any().map(move || Arc::clone(&sessions));
    let session_to_rpcs_filter = warp::any().map(move || Arc::clone(&session_to_rpcs));
    let exchange_name_filter = warp::any().map(move || exchange_name.clone());

    let deps = sessions_filter
        .and(session_to_rpcs_filter)
        .and(client_filter)
        .and(exchange_name_filter);

    warp::post2().and(deps).and(warp::body::json()).and_then(
        move |s: Shared<Sessions>,
              r: Shared<SessionToRpcs>,
              client: TcpClient,
              exchange_name: String,
              (fqdn, action): (Fqdn, Action)| {
            await_session(fqdn.clone(), s, Duration::from_secs(30))
                .from_err()
                .and_then(move |session_id| {
                    let msg = create_data_message(session_id.clone(), fqdn, action.clone());

                    match action {
                        Action::ActionCancel { id } => Either::A(cancel_running_action(
                            client.clone(),
                            msg,
                            session_id,
                            id,
                            r,
                        )),
                        action => {
                            let (tx, rx) = oneshot::channel();

                            Either::B(
                                send_agent_message(client.clone(), exchange_name, msg)
                                    .map(move |_| {
                                        let action_id: ActionId = action.get_id().clone();
                                        let af = ActionInFlight::new(action, tx);

                                        insert_action_in_flight(
                                            session_id,
                                            action_id,
                                            af,
                                            &mut r.lock(),
                                        );
                                    })
                                    .and_then(|_| rx.from_err()),
                            )
                        }
                    }
                })
                .map_err(warp::reject::custom)
        },
    )
}
