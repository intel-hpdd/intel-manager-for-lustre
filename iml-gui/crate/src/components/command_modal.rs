// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    components::{font_awesome, modal},
    extensions::{MergeAttrs as _, NodeExt as _, RequestExt as _},
    generated::css_classes::C,
    key_codes, sleep_with_handle, GMsg,
};
use futures::channel::oneshot;
use iml_wire_types::{ApiList, Command, EndpointName, Job, Step};
use seed::{prelude::*, *};
use std::{collections::HashSet, sync::Arc, time::Duration};
use std::collections::HashMap;

/// The component polls `/api/command/` endpoint and this constant defines how often it does.
const POLL_INTERVAL: Duration = Duration::from_millis(1000);

type Job0 = Job<Option<()>>;
#[derive(Debug, Hash, PartialEq, Eq)]
pub enum Idx {
    Command(u32),
    Job(u32),
    Step(u32),
}

#[derive(Clone, Debug)]
pub enum Input {
    Commands(Vec<Arc<Command>>),
    Ids(Vec<u32>),
}

#[derive(Default, Debug)]
pub struct Model {
    pub commands_cancel: Option<oneshot::Sender<()>>,
    pub commands_loading: bool,
    pub commands: Vec<Arc<Command>>,

    pub jobs_cancel: Option<oneshot::Sender<()>>,
    pub jobs_loading: bool,
    pub jobs: Vec<Arc<Job0>>,
    pub jobs_children: HashMap<u32, Vec<u32>>,

    pub steps_cancel: Option<oneshot::Sender<()>>,
    pub steps_loading: bool,
    pub steps: Vec<Arc<Step>>,

    pub opens: HashSet<Idx>,
    pub modal: modal::Model,
}

/// `Msg::FireCommands(..)` adds new commands to the polling list
/// `Msg::Fetch` spawns a future to make the api call
/// `Msg::Fetched(..)` wraps the result like
/// ```norun
/// { "meta": { .. }, "objects": [ cmd0, cmd1, ..., cmd9 ] }
/// ```
#[derive(Clone, Debug)]
pub enum Msg {
    Modal(modal::Msg),
    FireCommands(Input),
    FetchCommands,
    FetchedCommands(Box<fetch::ResponseDataResult<ApiList<Command>>>),
    FetchJobs,
    FetchedJobs(Box<fetch::ResponseDataResult<ApiList<Job0>>>),
    FetchSteps,
    FetchedSteps(Box<fetch::ResponseDataResult<ApiList<Step>>>),
    Open(u32),
    Close(u32),
    Noop,
}

pub fn update(msg: Msg, model: &mut Model, orders: &mut impl Orders<Msg, GMsg>) {
    // log!("command_modal::update", msg);
    match msg {
        Msg::Modal(msg) => {
            modal::update(msg, &mut model.modal, &mut orders.proxy(Msg::Modal));
        }
        Msg::FireCommands(cmds) => {
            model.modal.open = true;

            match cmds {
                Input::Commands(cmds) => {
                    // use the (little) optimization: if the commands all finished,
                    // then don't fetch anything
                    model.commands = cmds;
                    if !is_all_finished(&model.commands) {
                        orders.send_msg(Msg::FetchCommands);
                    }
                }
                Input::Ids(ids) => {
                    // we have ids only, so we need to populate the vector first
                    model.commands_loading = true;
                    orders.perform_cmd(fetch_command_status(ids));
                }
            }
        }
        Msg::FetchCommands => {
            model.commands_cancel = None;
            if !is_all_finished(&model.commands) {
                let ids = model.commands.iter().map(|x| x.id).collect();
                orders.skip().perform_cmd(fetch_command_status(ids));
            }
        }
        Msg::FetchedCommands(cmd_status_result) => {
            model.commands_loading = false;
            match *cmd_status_result {
                Ok(api_list) => {
                    model.commands = api_list.objects.into_iter().map(Arc::new).collect();
                }
                Err(e) => {
                    error!("Failed to perform fetch_command_status {:#?}", e);
                    orders.skip();
                }
            }
            if !is_all_finished(&model.commands) {
                let (cancel, fut) = sleep_with_handle(POLL_INTERVAL, Msg::FetchCommands, Msg::Noop);
                model.commands_cancel = Some(cancel);
                orders.perform_cmd(fut);
            }
        }
        Msg::FetchJobs => {
            model.jobs_cancel = None;
            model.jobs_loading = true;
            let ids = model.jobs.iter().map(|x| x.id).collect();
            orders.skip().perform_cmd(fetch_job_status(ids));
        }
        Msg::FetchedJobs(job_status_result) => {
            model.jobs_loading = false;
            match *job_status_result {
                Ok(api_list) => {
                    model.jobs = api_list.objects.into_iter().map(Arc::new).collect();
                }
                Err(e) => {
                    error!("Failed to perform fetch_job_status {:#?}", e);
                    orders.skip();
                }
            }
        }
        Msg::FetchSteps => {
            model.steps_cancel = None;
            model.steps_loading = true;
            let ids = model.jobs.iter().map(|x| x.id).collect();
            orders.skip().perform_cmd(fetch_job_status(ids));
        }
        Msg::FetchedSteps(step_status_result) => {
            model.steps_loading = false;
            match *step_status_result {
                Ok(api_list) => {
                    model.steps = api_list.objects.into_iter().map(Arc::new).collect();
                }
                Err(e) => {
                    error!("Failed to perform fetch_job_status {:#?}", e);
                    orders.skip();
                }
            }
        }
        Msg::Open(x) => {
            model.opens.insert(Idx::Command(x));
        }
        Msg::Close(x) => {
            model.opens.remove(&Idx::Command(x));
        }
        Msg::Noop => {}
    }
}

async fn fetch_command_status(command_ids: Vec<u32>) -> Result<Msg, Msg> {
    // e.g. GET /api/command/?id__in=1&id__in=2&id__in=11&limit=0
    let err_msg = format!("Bad query for commands: {:?}", command_ids);
    let mut ids: Vec<_> = command_ids.into_iter().map(|x| ("id__in", x)).collect();
    ids.push(("limit", 0));
    Request::api_query(Command::endpoint_name(), &ids)
        .expect(&err_msg)
        .fetch_json_data(|x| Msg::FetchedCommands(Box::new(x)))
        .await
}

async fn fetch_job_status(job_ids: Vec<u32>) -> Result<Msg, Msg> {
    // e.g. GET /api/job/?id__in=1&id__in=2&id__in=11&limit=0
    let err_msg = format!("Bad query for commands: {:?}", job_ids);
    let mut ids: Vec<_> = job_ids.into_iter().map(|x| ("id__in", x)).collect();
    ids.push(("limit", 0));
    Request::api_query(Job0::endpoint_name(), &ids)
        .expect(&err_msg)
        .fetch_json_data(|x| Msg::FetchedJobs(Box::new(x)))
        .await
}

async fn fetch_step_status(step_ids: Vec<u32>) -> Result<Msg, Msg> {
    // e.g. GET /api/step/?id__in=1&id__in=2&id__in=11&limit=0
    let err_msg = format!("Bad query for commands: {:?}", step_ids);
    let mut ids: Vec<_> = step_ids.into_iter().map(|x| ("id__in", x)).collect();
    ids.push(("limit", 0));
    Request::api_query(Step::endpoint_name(), &ids)
        .expect(&err_msg)
        .fetch_json_data(|x| Msg::FetchedSteps(Box::new(x)))
        .await
}

const fn is_finished(cmd: &Command) -> bool {
    cmd.complete
}

fn is_all_finished(cmds: &[Arc<Command>]) -> bool {
    cmds.iter().all(|cmd| is_finished(cmd))
}

pub(crate) fn view(model: &Model) -> Node<Msg> {
    if !model.modal.open {
        empty![]
    } else {
        modal::bg_view(
            true,
            Msg::Modal,
            modal::content_view(
                Msg::Modal,
                if model.commands_loading {
                    vec![
                        modal::title_view(Msg::Modal, span!["Loading Command"]),
                        div![
                            class![C.my_12, C.text_center, C.text_gray_500],
                            font_awesome(class![C.w_12, C.h_12, C.inline, C.pulse], "spinner")
                        ],
                        modal::footer_view(vec![close_button()]).merge_attrs(class![C.pt_8]),
                    ]
                } else {
                    vec![
                        modal::title_view(Msg::Modal, plain!["Commands"]),
                        div![
                            class![C.py_8],
                            model
                                .commands
                                .iter()
                                .map(|x| { command_item_view(x, model.opens.contains(&Idx::Command(x.id))) })
                        ],
                        modal::footer_view(vec![close_button()]).merge_attrs(class![C.pt_8]),
                    ]
                },
            ),
        )
        .with_listener(keyboard_ev(Ev::KeyDown, move |ev| match ev.key_code() {
            key_codes::ESC => Msg::Modal(modal::Msg::Close),
            _ => Msg::Noop,
        }))
        .merge_attrs(class![C.text_black])
    }
}

fn command_item_view(x: &Command, open: bool) -> Node<Msg> {
    let border = if !open {
        C.border_transparent
    } else if x.cancelled {
        C.border_gray_500
    } else if x.errored {
        C.border_red_500
    } else if x.complete {
        C.border_green_500
    } else {
        C.border_transparent
    };

    let (open_icon, m) = if open {
        ("chevron-circle-up", Msg::Close(x.id))
    } else {
        ("chevron-circle-down", Msg::Open(x.id))
    };

    div![
        class![C.border_b, C.last__border_b_0],
        div![
            class![
                border,
                C.border_l_2,
                C.px_2
                C.py_5,
                C.text_gray_700,
            ],
            header![
                class![
                    C.flex,
                    C.justify_between,
                    C.items_center,
                    C.cursor_pointer,
                    C.select_none,
                    C.py_5
                ],
                simple_ev(Ev::Click, m),
                span![class![C.font_thin, C.text_xl], status_icon(x), &x.message],
                font_awesome(
                    class![C.w_4, C.h_4, C.inline, C.text_gray_700, C.text_blue_500],
                    open_icon
                ),
            ],
            ul![
                class![C.pl_8, C.hidden => !open],
                li![class![C.pb_2], "Started at: ", x.created_at],
                li![class![C.pb_2], "Status: ", status_text(x)],
            ]
        ]
    ]
}

fn status_text(cmd: &Command) -> &'static str {
    if cmd.cancelled {
        "Cancelled"
    } else if cmd.errored {
        "Errored"
    } else if cmd.complete {
        "Complete"
    } else {
        "Running"
    }
}

fn status_icon<T>(cmd: &Command) -> Node<T> {
    let cls = class![C.w_4, C.h_4, C.inline, C.mr_4];

    if cmd.cancelled {
        font_awesome(cls, "ban").merge_attrs(class![C.text_gray_500])
    } else if cmd.errored {
        font_awesome(cls, "bell").merge_attrs(class![C.text_red_500])
    } else if cmd.complete {
        font_awesome(cls, "check").merge_attrs(class![C.text_green_500])
    } else {
        font_awesome(cls, "spinner").merge_attrs(class![C.text_gray_500, C.pulse])
    }
}

fn close_button() -> Node<Msg> {
    button![
        class![
            C.bg_transparent,
            C.py_2,
            C.px_4,
            C.rounded_full,
            C.text_blue_500,
            C.hover__bg_gray_100,
            C.hover__text_blue_400,
        ],
        simple_ev(Ev::Click, modal::Msg::Close),
        "Close",
    ]
    .map_msg(Msg::Modal)
}
