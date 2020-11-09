// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    api_utils::{graphql, wait_for_cmds_success},
    display_utils::{DisplayType, IntoDisplayType as _},
    error::ImlManagerCliError,
};
use console::Term;
use iml_graphql_queries::hotpool as hotpool_queries;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub enum HotpoolCommand {
    /// Create a new hotpool config for specified filesystem
    Create {
        fsname: String,
        /// Percent free space
        #[structopt(short = "H", long)]
        freehi: i32,
        #[structopt(short = "L", long)]
        freelo: i32,
        #[structopt(short, long)]
        minage: i32,
        #[structopt(short = "E", long)]
        extendlayout: Option<String>,
        #[structopt(short, long)]
        fast: String,
        #[structopt(short, long)]
        slow: String,
    },
    /// Start existing hotpool config
    Start { fsname: String },
    /// Stop hotpool
    Stop { fsname: String },
    /// Destroy hotpool config from filesystem
    Destroy { fsname: String },
    /// Show current hotpool configs
    List {
        /// Set the display type
        ///
        /// The display type can be one of the following:
        /// tabular: display content in a table format
        /// json: return data in json format
        /// yaml: return data in yaml format
        #[structopt(short = "d", long = "display", default_value = "tabular")]
        display_type: DisplayType,
    },
}

pub async fn hotpool_cli(command: HotpoolCommand) -> Result<(), ImlManagerCliError> {
    match command {
        HotpoolCommand::Create {
            fsname,
            freehi,
            freelo,
            minage,
            extendlayout,
            fast,
            slow,
        } => {
            let query = hotpool_queries::create::build(
                fsname,
                fast,
                slow,
                minage,
                freehi,
                freelo,
                extendlayout,
            );
            let resp: iml_graphql_queries::Response<hotpool_queries::create::Resp> =
                graphql(query).await?;
            let x = Result::from(resp)?.data.create_hotpool;
            wait_for_cmds_success(&[x]).await?;
        }
        HotpoolCommand::Start { fsname } => {
            let query = hotpool_queries::start::build(fsname);
            let resp: iml_graphql_queries::Response<hotpool_queries::start::Resp> =
                graphql(query).await?;
            let x = Result::from(resp)?.data.start_hotpool;
            wait_for_cmds_success(&[x]).await?;
        }
        HotpoolCommand::Stop { fsname } => {
            let query = hotpool_queries::stop::build(fsname);
            let resp: iml_graphql_queries::Response<hotpool_queries::stop::Resp> =
                graphql(query).await?;
            let x = Result::from(resp)?.data.stop_hotpool;
            wait_for_cmds_success(&[x]).await?;
        }
        HotpoolCommand::Destroy { fsname } => {
            let query = hotpool_queries::destroy::build(fsname);
            let resp: iml_graphql_queries::Response<hotpool_queries::destroy::Resp> =
                graphql(query).await?;
            let x = Result::from(resp)?.data.destroy_hotpool;
            wait_for_cmds_success(&[x]).await?;
        }
        HotpoolCommand::List { display_type } => {
            let query = hotpool_queries::list::build(None, None, Some(1_000));
            let resp: iml_graphql_queries::Response<hotpool_queries::list::Resp> =
                graphql(query).await?;
            let hps = Result::from(resp)?.data.hotpools;

            let x = hps.into_display_type(display_type);

            let term = Term::stdout();
            term.write_line(&x).unwrap();
        }
    }

    Ok(())
}
