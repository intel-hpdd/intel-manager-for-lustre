// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use bytes::buf::FromBuf;
use futures::{channel::mpsc, Future, Stream, StreamExt, TryStreamExt};
use std::{collections::HashMap, path::PathBuf, pin::Pin};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use warp::{body::BodyStream, filters::BoxedFilter, reject, Filter};

#[derive(Debug)]
pub enum Errors {
    IoError(std::io::Error),
    TrySendError(mpsc::TrySendError<String>),
}

impl reject::Reject for Errors {}

pub trait LineStream: Stream<Item = Result<String, warp::Rejection>> {}
impl<T: Stream<Item = Result<String, warp::Rejection>>> LineStream for T {}

fn streamer(s: BodyStream) -> Pin<Box<dyn Stream<Item = Result<String, warp::Rejection>> + Send>> {
    let s = s.map_ok(Vec::from_buf);

    iml_fs::read_lines(s)
        .map_err(Errors::IoError)
        .map_err(reject::custom)
        .boxed()
}

/// Warp Filter that streams a newline delimited body
pub fn line_stream(
) -> BoxedFilter<(Pin<Box<dyn Stream<Item = Result<String, warp::Rejection>> + Send>>,)> {
    warp::body::stream().map(streamer).boxed()
}

/// Holds all active streams that are currently writing to an address.
pub struct MailboxSenders(HashMap<PathBuf, mpsc::UnboundedSender<String>>);

impl Default for MailboxSenders {
    fn default() -> Self {
        MailboxSenders(HashMap::new())
    }
}

impl MailboxSenders {
    /// Adds a new address and tx handle to write lines with
    pub fn insert(&mut self, address: PathBuf, tx: mpsc::UnboundedSender<String>) {
        self.0.insert(address, tx);
    }
    /// Removes an address.
    ///
    /// Usually called when the associated rx stream has finished.
    pub fn remove(&mut self, address: &PathBuf) {
        self.0.remove(address);
    }
    /// Returns a cloned reference to a tx handle matching the provided address, if one exists.
    pub fn get(&mut self, address: &PathBuf) -> Option<mpsc::UnboundedSender<String>> {
        self.0.get(address).cloned()
    }
    /// Creates a new sender entry.
    ///
    /// Returns a pair of tx handle and a future that will write to a file.
    /// The returned future must be used, and should be spawned as a new task
    /// so it won't block the current task.
    pub fn create(
        &mut self,
        address: PathBuf,
    ) -> (
        mpsc::UnboundedSender<String>,
        impl Future<Output = Result<(), std::io::Error>>,
    ) {
        let (tx, rx) = mpsc::unbounded();

        self.insert(address.clone(), tx.clone());

        (tx, ingest_data(address, rx))
    }
}

/// Given an address and `mpsc::UnboundedReceiver` handle,
/// this fn will create or open an existing file in append mode.
///
/// It will then write any incoming lines from the passed `mpsc::UnboundedReceiver`
/// to that file.
pub async fn ingest_data(
    address: PathBuf,
    mut rx: mpsc::UnboundedReceiver<String>,
) -> Result<(), std::io::Error> {
    tracing::debug!("Starting ingest for {:?}", address);

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(address)
        .await?;

    while let Some(mut line) = rx.next().await {
        if !line.ends_with('\n') {
            line.extend(['\n'].iter());
        }

        tracing::debug!("handling line {:?}", line);

        file.write_all(line.as_bytes()).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_mailbox_senders() -> Result<(), Box<dyn std::error::Error>> {
        let tmp_dir = TempDir::new("test_mailbox")?;
        let address = tmp_dir.path().join("test_message_1");

        let mut mailbox_sender = MailboxSenders::default();

        let (tx, fut) = mailbox_sender.create(address.clone());

        tx.unbounded_send("foo\n".into())?;

        mailbox_sender
            .get(&address)
            .unwrap()
            .unbounded_send("bar".into())?;

        tx.unbounded_send("baz\n".into())?;

        mailbox_sender.remove(&address);

        drop(tx);

        fut.await?;

        let contents = fs::read_to_string(&address).unwrap();

        assert_eq!(contents, "foo\nbar\nbaz\n");

        Ok(())
    }
}
