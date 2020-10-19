// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::agent_error::ImlAgentError;
use futures::TryFutureExt;
use iml_cmd::{CheckedCommandExt, CmdError, Command};
use liblustreapi::LlapiFid;
use std::ffi::OsStr;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::delay_for;

/// Runs lctl with given arguments
pub async fn lctl<I, S>(args: I) -> Result<String, ImlAgentError>
where
    I: IntoIterator<Item = S> + Clone,
    S: AsRef<OsStr>,
{
    loop {
        let r = Command::new("/usr/sbin/lctl")
            .args(args.clone())
            .checked_output()
            .await;

        if let Err(CmdError::Output(o)) = &r {
            let stderr = String::from_utf8_lossy(&o.stderr);
            if stderr.find("Resource temporarily unavailable").is_some() {
                const DUR: Duration = Duration::from_secs(3);
                tracing::debug!("{}, waiting {:?} ...", stderr.trim(), DUR);
                delay_for(DUR).await;
                continue;
            }
        }
        break r
            .map_err(|e| e.into())
            .map(|o| String::from_utf8_lossy(&o.stdout).to_string());
    }
}

/// Returns LlapiFid for a given device or mount path
pub async fn search_rootpath(device: String) -> Result<LlapiFid, ImlAgentError> {
    spawn_blocking(move || LlapiFid::create(&device).map_err(ImlAgentError::from))
        .err_into()
        .await
        .and_then(std::convert::identity)
}
