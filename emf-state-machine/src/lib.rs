// Copyright (c) 2021 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file

pub mod action_runner;
pub mod command_plan;
pub mod executor;
pub mod input_document;
pub mod state_schema;
pub mod transition_graph;

use crate::{
    input_document::InputDocumentErrors,
    state_schema::{ActionName, State},
};
use sqlx::migrate::MigrateError;
use std::collections::HashMap;
use validator::{Validate, ValidationErrors};
use warp::reject;

/// The transition graph is a graph containing states for nodes and actions for edges.
type TransitionGraph = petgraph::graph::DiGraph<State, ActionName>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),
    #[error(transparent)]
    MigrateError(#[from] MigrateError),
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    #[error(transparent)]
    SshError(#[from] emf_ssh::Error),
    #[error(transparent)]
    InputDocumentErrors(#[from] InputDocumentErrors),
    #[error(transparent)]
    CombineEasyError(#[from] combine::stream::easy::Errors<char, String, usize>),
}

impl reject::Reject for Error {}

pub(crate) trait ValidateAddon {
    fn validate(&self) -> Result<(), ValidationErrors>;
}

impl<T: Validate> ValidateAddon for HashMap<String, T> {
    fn validate(&self) -> Result<(), ValidationErrors> {
        for i in self.values() {
            i.validate()?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command_plan::build_job_graphs,
        executor::build_execution_graph,
        input_document::{deserialize_input_document, host, SshOpts, Step, StepPair},
        state_schema::Input,
    };
    use emf_wire_types::ComponentType;
    use futures::Future;
    use once_cell::sync::Lazy;
    use petgraph::visit::NodeIndexable;
    use std::{ops::Deref, pin::Pin, sync::Arc};
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn graph_execution_stacks_population() -> Result<(), Box<dyn std::error::Error>> {
        static GLOBAL_DATA: Lazy<Mutex<Vec<String>>> = Lazy::new(|| {
            let xs = vec![];
            Mutex::new(xs)
        });

        let input_document = r#"version: 1
jobs:
  test_job1:
    name: Test stack 1
    steps:
      - action: host.ssh_command
        id: command1
        inputs:
          host: node1
          run: command1
      - action: host.ssh_command
        id: command2
        inputs:
          host: node1
          run: command2
      - action: host.ssh_command
        id: command3
        inputs:
          host: node1
          run: command3"#;

        let doc = deserialize_input_document(input_document)?;
        let mut graph_map = build_job_graphs(doc);
        let mut graph = graph_map.remove("test_job1".into()).unwrap();

        let node4 = graph.add_node(Step {
            action: StepPair::new(
                ComponentType::Host,
                ActionName::Host(host::ActionName::SshCommand),
            ),
            id: "step4".into(),
            inputs: Input::Host(host::Input::SshCommand(host::SshCommand {
                host: "node1".into(),
                run: "command4".into(),
                ssh_opts: SshOpts::default(),
            })),
            outputs: None,
        });

        let node5 = graph.add_node(Step {
            action: StepPair::new(
                ComponentType::Host,
                ActionName::Host(host::ActionName::SshCommand),
            ),
            id: "step5".into(),
            inputs: Input::Host(host::Input::SshCommand(host::SshCommand {
                host: "node1".into(),
                run: "command5".into(),
                ssh_opts: SshOpts::default(),
            })),
            outputs: None,
        });

        graph.add_edge(graph.from_index(0), node4, ());
        graph.add_edge(node4, node5, ());

        fn invoke_box(
            input: &Input,
        ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
            Box::pin(async move {
                match input {
                    Input::Host(host::Input::SshCommand(x)) => {
                        GLOBAL_DATA.lock().await.push(x.run.to_string())
                    }
                    _ => panic!("Should have received host ssh command."),
                }

                Ok(())
            })
        }

        let stacks = build_execution_graph(Arc::new(graph), invoke_box);

        // There should be exactly two stacks
        assert_eq!(stacks.len(), 2);

        // Execute the first stack. We should see that the first three commands ran.
        let mut iter = stacks.into_iter();
        iter.next().unwrap().await;

        {
            let mut items = GLOBAL_DATA.lock().await;

            assert_eq!(
                items.deref(),
                &vec![
                    "command1".to_string(),
                    "command2".to_string(),
                    "command3".to_string(),
                ]
            );

            items.clear();
        }

        // Execute the second stack. We should see that commands 4, 5, and 6 ran.
        iter.next().unwrap().await;

        let items = GLOBAL_DATA.lock().await;

        assert_eq!(
            items.deref(),
            &vec!["command4".to_string(), "command5".to_string(),]
        );

        Ok(())
    }
}