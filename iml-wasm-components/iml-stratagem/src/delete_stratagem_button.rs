// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use bootstrap_components::bs_button;
use futures::Future;
use iml_environment::csrf_token;
use seed::{class, dom_types::At, fetch, i, prelude::*};

#[derive(Debug, Default)]
pub struct Model {
    pub config_id: u32,
}

#[derive(Clone, Debug)]
pub enum Msg {
    DeleteStratagem,
    StratagemDeleted(fetch::FetchObject<iml_wire_types::Command>),
    OnFetchError(seed::fetch::FailReason<iml_wire_types::Command>),
}

pub fn update(msg: Msg, model: &mut Model, orders: &mut impl Orders<Msg>) {
    let orders = orders.skip();

    match msg {
        Msg::DeleteStratagem => {
            orders.perform_cmd(delete_stratagem(model.config_id));
        }
        Msg::StratagemDeleted(fetch_object) => match fetch_object.response() {
            Ok(response) => {
                log::trace!("Response data: {:#?}", response.data);
            }
            Err(fail_reason) => {
                orders.send_msg(Msg::OnFetchError(fail_reason));
            }
        },
        Msg::OnFetchError(fail_reason) => {
            log::warn!("Fetch error: {:#?}", fail_reason);
        }
    }

    log::trace!("Model: {:#?}", model);
}

fn delete_stratagem(config_id: u32) -> impl Future<Item = Msg, Error = Msg> {
    let url = format!("/api/stratagem_configuration/{}", config_id);

    seed::fetch::Request::new(url)
        .method(seed::fetch::Method::Delete)
        .header(
            "X-CSRFToken",
            &csrf_token().expect("Couldn't get csrf token."),
        )
        .fetch_json(Msg::StratagemDeleted)
}

pub fn view() -> Node<Msg> {
    bs_button::btn(
        class![bs_button::BTN_DANGER, "delete-button"],
        vec![Node::new_text("Delete"), i![class!["fas fa-times-circle"]]],
    )
    .add_listener(simple_ev(Ev::Click, Msg::DeleteStratagem))
}
