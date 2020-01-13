// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    agent_error::ImlAgentError,
    daemon_plugins::{DaemonPlugin, Output},
    http_comms::mailbox_client::send,
};
use futures::stream::TryStreamExt;
use futures::{Future, FutureExt};
use futures_util::stream::StreamExt as ForEachStreamExt;
use inotify::{Inotify, WatchDescriptor, WatchMask};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
};
use stream_cancel::{StreamExt, Trigger, Tripwire};
use tokio::{fs, net::UnixListener};
use tokio_util::codec::{BytesCodec, FramedRead};

pub const CONF_FILE: &str = "/etc/iml/postman.conf";
pub const SOCK_DIR: &str = "/run/iml/";

pub struct POWD(pub Option<WatchDescriptor>);

pub struct PostOffice {
    // individual mailbox socket listeners
    routes: Arc<Mutex<HashMap<String, Trigger>>>,
    inotify: Arc<Mutex<Inotify>>,
    wd: Arc<Mutex<POWD>>,
}

pub fn create() -> impl DaemonPlugin {
    PostOffice {
        wd: Arc::new(Mutex::new(POWD(None))),
        inotify: Arc::new(Mutex::new(
            Inotify::init().expect("Failed to initialize inotify"),
        )),
        routes: Arc::new(Mutex::new(HashMap::new())),
    }
}

/// Return socket address for a given mailbox
pub fn socket_name(mailbox: &str) -> String {
    format!("{}/postman-{}.sock", SOCK_DIR, mailbox)
}

impl std::fmt::Debug for PostOffice {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PostOffice {{ {:?} }}", self.routes.lock().keys())
    }
}

fn start_route(mailbox: String) -> Trigger {
    let (trigger, tripwire) = Tripwire::new();
    let addr = socket_name(&mailbox);

    let rc = async move {
        let mut listener = UnixListener::bind(addr.clone()).unwrap();

        let mut incoming = listener.incoming().take_until(tripwire);
        tracing::debug!("Starting Route for {}", mailbox);
        while let Some(inbound) = incoming.next().await {
            if let Ok(inbound) = inbound {
                let stream = FramedRead::new(inbound, BytesCodec::new())
                    .map_ok(bytes::BytesMut::freeze)
                    .map_err(ImlAgentError::Io);
                let transfer = send(mailbox.clone(), stream).map(|r| {
                    if let Err(e) = r {
                        println!("Failed to transfer; error={}", e);
                    }
                });
                tokio::spawn(transfer);
            }
        }
        tracing::debug!("Ending Route for {}", mailbox);
        fs::remove_file(addr).await
    };
    tokio::spawn(rc);
    trigger
}

fn stop_route(trigger: Trigger) {
    drop(trigger);
}

impl DaemonPlugin for PostOffice {
    fn start_session(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Output, ImlAgentError>> + Send>> {
        let routes = Arc::clone(&self.routes);
        let inotify = Arc::clone(&self.inotify);
        let wd = Arc::clone(&self.wd);

        async move {
            if let Ok(file) = fs::read_to_string(CONF_FILE).await {
                let itr = file.lines().map(|mb| {
                    let trigger = start_route(mb.to_string());
                    (mb.to_string(), trigger)
                });
                routes.lock().extend(itr);
            } else {
                fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(CONF_FILE)
                    .await?;
            }

            wd.lock().0 = inotify
                .lock()
                .add_watch(CONF_FILE, WatchMask::MODIFY)
                .map_err(|e| tracing::error!("Failed to watch configuration: {}", e))
                .ok();

            let watcher = async move {
                let mut buffer = [0; 32];
                let mut stream = inotify.lock().event_stream(&mut buffer)?;

                while let Some(event_or_error) = stream.next().await {
                    tracing::debug!("event: {:?}", event_or_error);
                    match fs::read_to_string(CONF_FILE).await {
                        Ok(file) => {
                            let newset: HashSet<String> =
                                file.lines().map(|s| s.to_string()).collect();
                            let oldset: HashSet<String> = routes.lock().keys().cloned().collect();

                            let added = &newset - &oldset;
                            let itr = added.iter().map(|mb| {
                                let trigger = start_route(mb.to_string());
                                (mb.to_string(), trigger)
                            });
                            let mut rt = routes.lock();
                            rt.extend(itr);
                            for rm in &oldset - &newset {
                                if let Some(trigger) = rt.remove(&rm) {
                                    stop_route(trigger);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to open configuration {}: {}", CONF_FILE, e)
                        }
                    }
                }
                tracing::debug!("Ending Inotify Listen for {}", CONF_FILE);
                Ok::<_, ImlAgentError>(())
            };
            tokio::spawn(watcher);
            Ok(None)
        }
        .boxed()
    }

    fn teardown(&mut self) -> Result<(), ImlAgentError> {
        if let Some(wd) = self.wd.lock().0.clone() {
            let _ = self.inotify.lock().rm_watch(wd);
        }
        for (_, tx) in self.routes.lock().drain() {
            stop_route(tx);
        }

        Ok(())
    }
}
