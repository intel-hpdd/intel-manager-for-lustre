use crate::{
    breakpoints,
    components::{activity_indicator, breadcrumbs, font_awesome, logo},
    ctx_help::CtxHelp,
    generated::css_classes::C,
    MergeAttrs, Model, Msg, Route,
    Visibility::*,
    CTX_HELP,
};
use seed::{prelude::*, virtual_dom::Attrs, *};

fn menu_icon<T>(icon_name: &str) -> Node<T> {
    font_awesome(class![C.h_6, C.w_6, C.mr_3, C.inline, C._mt_1], icon_name)
}

fn nav_manage_dropdown(open: bool) -> Node<Msg> {
    if !open {
        return empty![];
    }

    let cls = class![
        C.my_2,
        C.px_4,
        C.py_2,
        C.bg_menu_active,
        C.block,
        C.cursor_pointer,
        C.hover__text_white,
    ];

    div![
        class![
            C.lg__absolute,
            C.lg__border,
            C.lg__w_full,
            C.lg__left_0,
            C.border_gray_600,
            C.mt_px,
            C.text_gray_500,
            C.rounded,
            C.bg_menu,
            C.p_4,
            C.z_20
        ],
        style! { "top" => "110%" },
        ul![
            li![a![
                &cls,
                Route::Server.to_string(),
                attrs! {
                    At::Href => Route::Server.to_href(),
                },
            ]],
            li![
                a![&cls, Route::PowerControl.to_string()],
                attrs! {
                    At::Href => Route::PowerControl.to_href(),
                },
            ],
            li![
                a![&cls, Route::Filesystem.to_string()],
                attrs! {
                    At::Href => Route::Filesystem.to_href(),
                },
            ],
            li![a![&cls, "HSM"]],
            li![a![&cls, "Storage"]],
            li![
                a![&cls, Route::User.to_string()],
                attrs! {
                    At::Href => Route::User.to_href(),
                },
            ],
            li![
                a![&cls, Route::Volume.to_string()],
                attrs! {
                    At::Href => Route::Volume.to_href(),
                },
            ],
            li![
                a![&cls, Route::Mgt.to_string()],
                attrs! {
                    At::Href => Route::Mgt.to_href(),
                },
            ]
        ]
    ]
}

fn main_menu_items(model: &Model) -> Node<Msg> {
    let menu_class = class![
        C.block,
        C.lg__h_16,
        C.lg__inline_block,
        C.lg__flex,
        C.lg__flex_auto,
        C.lg__flex_col,
        C.lg__justify_center,
        C.lg__py_0,
        C.p_6,
        C.text_gray_500,
        C.hover__bg_menu_active,
        C.border_b_2,
        C.border_transparent,
        C.group,
    ];

    div![
        class![C.text_base, C.lg__flex, C.lg__h_16,],
        a![
            &menu_class,
            class![C.bg_menu_active => model.route == Route::Dashboard],
            attrs! {
                At::Href => Route::Dashboard.to_href()
            },
            span![
                menu_icon("tachometer-alt"),
                span![
                    class![C.group_hover__text_active, C.text_active => model.route == Route::Dashboard],
                    Route::Dashboard.to_string(),
                ],
            ]
        ],
        a![
            &menu_class,
            class![
                C.lg__border_blue_400 => model.manage_menu_state.is_open(),
                C.relative
            ],
            simple_ev(Ev::Click, Msg::ManageMenuState),
            span![
                menu_icon("cog"),
                span![
                    class![C.group_hover__text_active],
                    "Management",
                    font_awesome(
                        class![C.fill_current, C.h_3, C.w_3, C.ml_1, C.inline, C._mt_1],
                        "chevron-down"
                    ),
                ],
            ],
            nav_manage_dropdown(model.manage_menu_state.is_open()),
        ],
        a![
            &menu_class,
            class![C.bg_menu_active => model.route == Route::Jobstats],
            attrs! {
                At::Href => Route::Jobstats.to_href()
            },
            span![
                menu_icon("signal"),
                span![
                    class![C.group_hover__text_active, C.text_active => model.route == Route::Jobstats],
                    Route::Jobstats.to_string(),
                ],
            ]
        ],
        a![
            &menu_class,
            class![C.bg_menu_active => model.route == Route::Logs],
            attrs! {
                At::Href => Route::Logs.to_href()
            },
            span![
                menu_icon("book"),
                span![
                    class![C.group_hover__text_active, C.bg_menu_active => model.route == Route::Logs],
                    Route::Logs.to_string(),
                ]
            ]
        ],
        context_sensitive_help_link(model, &menu_class),
        a![
            &menu_class,
            class![C.bg_menu_active => model.route == Route::Activity],
            attrs! {
                At::Href => Route::Activity.to_href(),
            },
            span![
                activity_indicator(&model.activity_health),
                span![
                    class![C.group_hover__text_active, C.bg_menu_active => model.route == Route::Activity],
                    Route::Activity.to_string(),
                ]
            ]
        ],
    ]
}

fn context_sensitive_help_link(model: &Model, menu_class: &Attrs) -> Node<Msg> {
    let attrs = attrs! {
       At::Target => "_blank", // open the link in a new tab
       At::Href => format!(
           "{}{}",
           CTX_HELP,
           model.route.help_link().unwrap_or_else(|| "".into())
       )
    };
    a![
        menu_class,
        attrs,
        span![
            menu_icon("question-circle"),
            span![class![C.group_hover__text_active], "Help"]
        ]
    ]
}

fn nav(model: &Model) -> Node<Msg> {
    nav![
        class![
            C.flex,
            C.bg_menu,
            C.items_center,
            C.px_5,
            C.lg__h_16,
            C.justify_between,
            C.flex_wrap
        ],
        div![
            class![C.flex, C.items_center, C.flex_shrink_0, C.text_white, C.mr_12,],
            logo().merge_attrs(class![C.h_8, C.w_8, C.mr_3]),
            span![class![C.font_semibold, C.text_3xl, C.tracking_tight], "IML"],
        ],
        div![
            class![C.block, C.lg__hidden,],
            button![
                class![
                    C.flex,
                    C.items_center,
                    C.px_3,
                    C.py_2,
                    C.border,
                    C.rounded,
                    C.border_gray_400,
                    C.text_gray_300,
                    C.hover__text_white,
                    C.hover__border_white,
                ],
                simple_ev(Ev::Click, Msg::ToggleMenu),
                svg![
                    class![C.fill_current, C.h_3, C.w_3],
                    attrs! {
                        At::ViewBox => "0 0 20 20",
                    },
                    title!["Menu",],
                    path![attrs! { At::D => "M0 3h20v2H0V3zm0 6h20v2H0V9zm0 6h20v2H0v-2z", },],
                ],
            ],
        ],
        if model.menu_visibility == Visible || model.breakpoint_size >= breakpoints::Size::LG {
            div![
                class![
                    C.w_full,
                    C.block,
                    C.flex_grow,
                    C.lg__flex,
                    C.lg__items_center,
                    C.lg__w_auto,
                    C.lg__h_16,
                ],
                main_menu_items(model),
                div![
                    class![C.text_base, C.lg__flex, C.lg__h_16, C.lg__flex_grow, C.lg__justify_end],
                    a![
                        class![
                            C.block,
                            C.lg__h_16,
                            C.lg__inline_block,
                            C.lg__flex,
                            C.lg__flex_auto,
                            C.lg__flex_col,
                            C.lg__flex_grow_0,
                            C.lg__justify_center,
                            C.lg__py_0,
                            C.p_6,
                            C.text_gray_300,
                            C.hover__text_white,
                            C.border_b_2,
                            C.border_transparent
                        ],
                        attrs! {
                            At::Href => Route::Login.to_href(),
                        },
                        Route::Login.to_string(),
                    ],
                ],
            ]
        } else {
            empty![]
        }
    ]
}

pub fn view(model: &Model) -> impl View<Msg> {
    vec![
        header![nav(model)],
        div![
            class![C.bg_menu_active, C.text_gray_300, C.text_base, C.text_center, C.py_2],
            breadcrumbs::view(&model.breadcrumbs).els()
        ],
    ]
}
