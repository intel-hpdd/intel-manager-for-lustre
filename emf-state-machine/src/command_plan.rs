// Copyright (c) 2021 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file

use crate::{
    input_document::{InputDocument, Step},
    Error,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use emf_postgres::PgPool;
use emf_tracing::tracing;
use emf_wire_types::{Command, CommandStep, State};
use futures::{StreamExt, TryFutureExt};
use petgraph::graph::NodeIndex;
use std::{cmp::max, collections::HashMap, convert::TryInto, ops::IndexMut};
use tokio::{
    io::{BufWriter, DuplexStream},
    sync::mpsc,
};
use tokio_util::io::ReaderStream;

pub type JobGraph = petgraph::graph::DiGraph<Step, ()>;
pub type JobGraphs = HashMap<String, JobGraph>;

pub fn build_job_graphs(input_doc: InputDocument) -> JobGraphs {
    input_doc
        .jobs
        .into_iter()
        .map(|(job_name, job)| {
            let g = job
                .steps
                .into_iter()
                .fold((JobGraph::new(), None), |(mut g, last), step| {
                    let node_idx = g.add_node(step);

                    if let Some(last_idx) = last {
                        g.add_edge(last_idx, node_idx, ());
                    };

                    (g, Some(node_idx))
                })
                .0;

            (job_name, g)
        })
        .collect()
}

pub async fn build_command(pg_pool: &PgPool, job_graphs: &JobGraphs) -> Result<Command, Error> {
    let plan: CommandPlan = job_graphs
        .iter()
        .map(|(k, job_graph)| {
            let command_graph = job_graph.map(|_, x| x.into(), |_, x| *x);

            (k.to_string(), command_graph)
        })
        .collect();

    let id = sqlx::query!(
        "INSERT INTO command_plan (plan) VALUES ($1) RETURNING id",
        serde_json::to_value(&plan)?
    )
    .fetch_one(pg_pool)
    .await?
    .id;

    Ok(Command {
        id,
        plan: (&plan).try_into()?,
        state: State::Pending,
    })
}

#[derive(Debug)]
pub(crate) enum Change {
    Started(DateTime<Utc>),
    Ended(DateTime<Utc>),
    State(State),
    Stdout(Bytes),
    Stderr(Bytes),
}

impl From<&Step> for CommandStep {
    fn from(step: &Step) -> Self {
        Self {
            action: step.action.into(),
            id: step.id.to_string(),
            state: State::Pending,
            started_at: None,
            finished_at: None,
            stdout: String::new(),
            stderr: String::new(),
        }
    }
}

type CommandGraph = petgraph::graph::DiGraph<CommandStep, ()>;

type CommandPlan = HashMap<String, CommandGraph>;

/// Used to send `Change`s for a given `CommandPlan`
pub(crate) type CommandPlanWriter = mpsc::UnboundedSender<(String, NodeIndex, Change)>;

pub(crate) trait CommandPlanWriterExt {
    /// Returns a new `CommandStepWriter` That can write changes scoped to a `CommandStep`
    fn get_command_state_writer(&self, job_name: &str, node_idx: NodeIndex) -> CommandStepWriter;
}

impl CommandPlanWriterExt for CommandPlanWriter {
    fn get_command_state_writer(&self, job_name: &str, node_idx: NodeIndex) -> CommandStepWriter {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outer_tx = self.clone();

        let job_name = job_name.to_string();

        tokio::spawn(async move {
            while let Some(change) = rx.recv().await {
                let _ = outer_tx.send((job_name.to_string(), node_idx, change));
            }
        });

        tx
    }
}

pub(crate) type CommandStepWriter = mpsc::UnboundedSender<Change>;

pub(crate) trait CommandStepWriterExt {
    /// Returns a pair of `OutputWriter`s that can write to stdout and stderr respectively for the associated `CommandStep`.
    fn get_output_handles(&self) -> (OutputWriter, OutputWriter);
}

impl CommandStepWriterExt for CommandStepWriter {
    fn get_output_handles(&self) -> (OutputWriter, OutputWriter) {
        let (stdout_tx, stdout_rx) = tokio::io::duplex(10_000);
        let (stderr_tx, stderr_rx) = tokio::io::duplex(10_000);

        let mut stdout_rx = ReaderStream::new(stdout_rx);
        let mut stderr_rx = ReaderStream::new(stderr_rx);

        let tx = self.clone();

        tokio::spawn(
            async move {
                loop {
                    tokio::select! {
                        Some(v) = stdout_rx.next() => tx.send(Change::Stdout(v?))?,
                        Some(v) = stderr_rx.next() => tx.send(Change::Stderr(v?))?,
                        else => break
                    }
                }

                Ok(())
            }
            .map_err(|e: Box<dyn std::error::Error>| {
                tracing::warn!("Could not persist stdout or stderr to DB {:?}", e)
            }),
        );

        (BufWriter::new(stdout_tx), BufWriter::new(stderr_tx))
    }
}

pub(crate) type OutputWriter = BufWriter<DuplexStream>;

pub(crate) async fn command_plan_writer(
    pool: &PgPool,
    plan_id: i32,
    job_graphs: &JobGraphs,
) -> Result<CommandPlanWriter, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();

    let mut command_plan: CommandPlan = job_graphs
        .iter()
        .map(|(k, job_graph)| {
            let command_graph = job_graph.map(|_, x| x.into(), |_, x| *x);

            (k.to_string(), command_graph)
        })
        .collect();

    let pool2 = pool.clone();

    tokio::spawn(async move {
        while let Some((job_name, node_idx, change)) = rx.recv().await {
            let mut x: &mut CommandStep = match command_plan
                .get_mut(&job_name)
                .map(|x| x.index_mut(node_idx))
            {
                Some(x) => x,
                None => {
                    tracing::warn!("Could not find node for {}.{:?}", job_name, node_idx);
                    continue;
                }
            };

            match change {
                Change::Started(started_at) => {
                    x.started_at = Some(started_at);
                }
                Change::Ended(finished_at) => {
                    x.finished_at = Some(finished_at);
                }
                Change::State(state) => {
                    x.state = state;
                }
                Change::Stdout(buf) => x.stdout += &String::from_utf8_lossy(&buf),
                Change::Stderr(buf) => x.stderr += &String::from_utf8_lossy(&buf),
            }

            let state = command_plan.values().fold(State::Pending, |state, x| {
                let sub_state = x
                    .node_indices()
                    .fold(State::Pending, |state, n| max(x[n].state, state));

                max(state, sub_state)
            });

            let x = match serde_json::to_value(&command_plan) {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("Could not serialize command plan {:?}", e);

                    continue;
                }
            };

            let r = sqlx::query!(
                "UPDATE command_plan SET plan = $1, state = $2 WHERE id = $3",
                x,
                state as State,
                plan_id
            )
            .execute(&pool2)
            .await;

            if let Err(e) = r {
                tracing::warn!("Could not save command plan {:?}", e);
            }
        }
    });

    Ok(tx)
}
