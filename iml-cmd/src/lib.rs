// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::{future::BoxFuture, Future, FutureExt, TryFutureExt};
use std::{
    env, error, fmt, io,
    pin::Pin,
    process::{ExitStatus, Output},
};
pub use tokio::process::{Child, Command};

#[cfg(feature = "warp-errs")]
use warp::reject;

#[derive(Debug)]
pub enum CmdError {
    Io(io::Error),
    Output(Output),
    VarError(env::VarError),
}

#[cfg(feature = "warp-errs")]
impl reject::Reject for CmdError {}

impl fmt::Display for CmdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CmdError::Io(ref err) => write!(f, "{}", err),
            CmdError::Output(ref err) => write!(
                f,
                "{}, stdout: {}, stderr: {}",
                err.status,
                String::from_utf8_lossy(&err.stdout),
                String::from_utf8_lossy(&err.stderr)
            ),
            CmdError::VarError(ref err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for CmdError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            CmdError::Io(ref err) => Some(err),
            CmdError::Output(_) => None,
            CmdError::VarError(ref err) => Some(err),
        }
    }
}

impl From<io::Error> for CmdError {
    fn from(err: io::Error) -> Self {
        CmdError::Io(err)
    }
}

impl From<Output> for CmdError {
    fn from(output: Output) -> Self {
        CmdError::Output(output)
    }
}

impl From<env::VarError> for CmdError {
    fn from(err: env::VarError) -> Self {
        CmdError::VarError(err)
    }
}

fn handle_status(x: ExitStatus) -> Result<(), io::Error> {
    if x.success() {
        Ok(())
    } else {
        let err = io::Error::new(
            io::ErrorKind::Other,
            format!("process exited with code: {:?}", x.code()),
        );

        Err(err)
    }
}

pub trait CheckedCommandExt {
    fn checked_status(&mut self) -> BoxFuture<Result<(), CmdError>>;
    fn checked_output(&mut self) -> BoxFuture<Result<Output, CmdError>>;
}

impl CheckedCommandExt for Command {
    /// Similar to `status`, but returns `Err` if the exit code is non-zero.
    fn checked_status(&mut self) -> BoxFuture<Result<(), CmdError>> {
        tracing::debug!("Running cmd: {:?}", self);

        self.status()
            .and_then(|x| async move { handle_status(x) })
            .err_into()
            .boxed()
    }
    /// Similar to `output`, but returns `Err` if the exit code is non-zero.
    fn checked_output(&mut self) -> BoxFuture<Result<Output, CmdError>> {
        tracing::debug!("Running cmd: {:?}", self);

        self.output()
            .err_into()
            .and_then(|x| async {
                if x.status.success() {
                    Ok(x)
                } else {
                    Err(x.into())
                }
            })
            .boxed()
    }
}

pub trait CheckedChildExt {
    fn wait_with_checked_output(
        self,
    ) -> Pin<Box<dyn Future<Output = Result<Output, CmdError>> + Send>>;
}

impl CheckedChildExt for Child {
    fn wait_with_checked_output(
        self,
    ) -> Pin<Box<dyn Future<Output = Result<Output, CmdError>> + Send>> {
        tracing::debug!("Child waiting for output: {:?}", self);

        self.wait_with_output()
            .err_into()
            .and_then(|x| async {
                if x.status.success() {
                    Ok(x)
                } else {
                    Err(x.into())
                }
            })
            .boxed()
    }
}
