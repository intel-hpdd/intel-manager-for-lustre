// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::channel::mpsc;
use iml_tracing::tracing;
use iml_wire_types::{ApiList, Command, EndpointName as _};
use std::{collections::HashSet, iter, time::Duration};
use tokio::time::delay_for;

#[derive(serde::Serialize)]
pub struct SendJob<T> {
    pub class_name: String,
    pub args: T,
}

#[derive(serde::Serialize)]
pub struct SendCmd<T> {
    pub jobs: Vec<SendJob<T>>,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CmdUtilError {
    #[error(transparent)]
    ImlManagerClientError(#[from] iml_manager_client::ImlManagerClientError),
}

pub enum Progress {
    Update(i32),
    Complete(Command),
}

pub async fn wait_for_cmds_progress(
    cmds: &[Command],
    tx: Option<mpsc::UnboundedSender<Progress>>,
) -> Result<Vec<Command>, CmdUtilError> {
    let mut state: HashSet<_> = cmds.iter().map(|x| x.id).collect();
    let mut settled_commands = vec![];

    loop {
        if state.is_empty() {
            tracing::debug!("All commands complete. Returning");
            return Ok(settled_commands);
        }

        delay_for(Duration::from_millis(1000)).await;

        let query: Vec<_> = state
            .iter()
            .map(|x| ["id__in".into(), x.to_string()])
            .chain(iter::once(["limit".into(), "0".into()]))
            .collect();

        let client = iml_manager_client::get_client()?;

        let cmds: ApiList<Command> =
            iml_manager_client::get(client, Command::endpoint_name(), query).await?;

        for cmd in cmds.objects {
            if cmd_finished(&cmd) {
                state.remove(&cmd.id);

                if let Some(tx) = tx.as_ref() {
                    let _ = tx.unbounded_send(Progress::Complete(cmd.clone()));
                }

                settled_commands.push(cmd);
            } else if let Some(tx) = tx.as_ref() {
                let _ = tx.unbounded_send(Progress::Update(cmd.id));
            }
        }
    }
}

fn cmd_finished(cmd: &Command) -> bool {
    cmd.complete
}
