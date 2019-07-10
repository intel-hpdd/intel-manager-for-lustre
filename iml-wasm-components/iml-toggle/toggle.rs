// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use bootstrap_components::bs_button;
use seed::{class, i, prelude::*};

#[derive(Clone, Debug)]
pub struct Active(pub bool);

pub fn toggle(active: bool) -> El<Active> {
    let mut cfg = class![bs_button::BTN_INFO, bs_button::SMALL];

    if active {
        cfg.merge(class!["active"]);
    }

    let mut btn = bs_button::btn(
        cfg,
        if active {
            vec![i![class!["fa", "fa-check"]], El::new_text("Enabled")]
        } else {
            vec![El::new_text("Disabled")]
        },
    );

    btn.listeners
        .push(mouse_ev(Ev::Click, move |_| Active(!active)));

    btn
}
