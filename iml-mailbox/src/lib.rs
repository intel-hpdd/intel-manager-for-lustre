// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use bytes::buf::FromBuf;
use futures::{sync::mpsc, Future, Stream};
use std::{collections::HashMap, path::PathBuf};
use tokio::{fs::OpenOptions, io};
use warp::{body::BodyStream, filters::BoxedFilter, Filter};

pub trait LineStream: Stream<Item = String, Error = warp::Rejection> {}
impl<T: Stream<Item = String, Error = warp::Rejection>> LineStream for T {}

fn streamer(s: BodyStream) -> Box<dyn LineStream + Send> {
    let s = s.map(Vec::from_buf);

    let ls = iml_fs::read_lines(s).map_err(warp::reject::custom);

    Box::new(ls) as Box<dyn LineStream + Send>
}

/// Warp Filter that streams a newline delimited body
pub fn line_stream() -> BoxedFilter<(Box<dyn LineStream + Send>,)> {
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
    #[must_use]
    pub fn create(
        &mut self,
        address: PathBuf,
    ) -> (
        mpsc::UnboundedSender<String>,
        impl Future<Item = (), Error = std::io::Error>,
    ) {
        let (tx, rx) = mpsc::unbounded();

        self.insert(address.clone(), tx.clone());

        (tx, ingest_data(address.clone(), rx))
    }
}

/// Given an address and `mpsc::UnboundedReceiver` handle,
/// this fn will create or open an existing file in append mode.
///
/// It will then write any incoming lines from the passed `mpsc::UnboundedReceiver`
/// to that file.
pub fn ingest_data(
    address: PathBuf,
    rx: mpsc::UnboundedReceiver<String>,
) -> impl Future<Item = (), Error = std::io::Error> {
    log::debug!("Starting ingest for {:?}", address);
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(address)
        .and_then(|f| {
            rx.map_err(|_| unreachable!("mpsc::Receiver should never return Err"))
                .map(|mut line| {
                    if !line.ends_with('\n') {
                        line.extend(['\n'].iter());
                    }

                    log::debug!("handling line {:?}", line);

                    line
                })
                .fold(f, |file, line| io::write_all(file, line).map(|(f, _)| f))
                .map(|_| {})
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Async;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn test_line_stream() {
        let mut stream = warp::test::request()
            .body("foo\nbar\nbaz")
            .filter(&line_stream())
            .unwrap();

        assert_eq!(stream.poll().unwrap(), Async::Ready(Some("foo".into())));
        assert_eq!(stream.poll().unwrap(), Async::Ready(Some("bar".into())));
        assert_eq!(stream.poll().unwrap(), Async::Ready(Some("baz".into())));
        assert_eq!(stream.poll().unwrap(), Async::Ready(None));
    }

    #[test]
    fn test_mailbox_senders() {
        let tmp_dir = TempDir::new("test_mailbox").unwrap();
        let address = tmp_dir.path().join("test_message_1");

        let mut mailbox_sender = MailboxSenders::default();

        let (tx, fut) = mailbox_sender.create(address.clone());

        tx.unbounded_send("foo\n".into()).unwrap();
        mailbox_sender
            .get(&address)
            .unwrap()
            .unbounded_send("bar".into())
            .unwrap();

        tx.unbounded_send("baz\n".into()).unwrap();

        mailbox_sender.remove(&address);

        drop(tx);

        tokio::run(fut.map_err(|e| panic!(e)));

        let contents = fs::read_to_string(&address).unwrap();

        assert_eq!(contents, "foo\nbar\nbaz\n");
    }
}
