// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    agent_error::ImlAgentError,
    daemon_plugins::{DaemonPlugin, Output},
};
use async_trait::async_trait;
use futures::{
    channel::oneshot, future, lock::Mutex, Future, FutureExt, Stream, StreamExt, TryFutureExt,
    TryStreamExt,
};
use std::{pin::Pin, sync::Arc};
use stream_cancel::{StreamExt as _, Trigger, Tripwire};
use tokio::{io::AsyncWriteExt, net::UnixStream};
use tokio_util::codec::{FramedRead, LinesCodec};

/// Opens a persistent stream to device scanner.
fn device_stream() -> impl Stream<Item = Result<String, ImlAgentError>> {
    UnixStream::connect("/var/run/device-scanner.sock")
        .err_into()
        .and_then(|mut conn| async {
            conn.write_all(b"\"Stream\"\n")
                .err_into::<ImlAgentError>()
                .await?;

            Ok(conn)
        })
        .map_ok(|c| FramedRead::new(c, LinesCodec::new()).err_into())
        .try_flatten_stream()
}

pub fn create() -> impl DaemonPlugin {
    Devices {
        trigger: None,
        state: Arc::new(Mutex::new((None, None))),
    }
}

#[derive(Debug)]
pub struct Devices {
    trigger: Option<Trigger>,
    state: Arc<Mutex<(Output, Output)>>,
}

#[async_trait]
impl DaemonPlugin for Devices {
    fn start_session(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Output, ImlAgentError>> + Send>> {
        let (trigger, tripwire) = Tripwire::new();
        let (tx, rx) = oneshot::channel();

        self.trigger = Some(trigger);

        let state = Arc::clone(&self.state);

        tokio::spawn(
            device_stream()
                .boxed()
                .and_then(|x| future::ready(serde_json::from_str(&x).map_err(|e| e.into())))
                .into_future()
                .then(|(x, s)| {
                    let x = if let Some(x) = x {
                        x.map(move |y| {
                            let _ = tx.send(y);
                            s
                        })
                    } else {
                        Ok(s)
                    };

                    future::ready(x)
                })
                .try_flatten_stream()
                .take_until(tripwire)
                .try_for_each(move |x| {
                    let state = Arc::clone(&state);

                    async move {
                        let mut s = state.lock().await;

                        s.0 = s.1.take();
                        s.1 = x;

                        Ok(())
                    }
                })
                .map(|x| {
                    if let Err(e) = x {
                        tracing::error!("Error processing device output: {}", e);
                    }
                }),
        );

        Box::pin(rx.err_into())
    }
    fn update_session(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Output, ImlAgentError>> + Send>> {
        let state = Arc::clone(&self.state);

        async move {
            let s = state.lock().await.clone();

            if s.0 != s.1 {
                Ok(s.1)
            } else {
                Ok(None)
            }
        }
        .boxed()
    }
    async fn teardown(&mut self) -> Result<(), ImlAgentError> {
        self.trigger.take();

        Ok(())
    }
}
