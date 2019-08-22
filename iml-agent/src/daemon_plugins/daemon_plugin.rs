// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    agent_error::{ImlAgentError, NoPluginError, Result},
    daemon_plugins::{action_runner, stratagem},
};
use futures::{future, Future};
use iml_wire_types::{AgentResult, PluginName};
use std::collections::HashMap;
use tracing::info;

pub type OutputValue = serde_json::Value;
pub type Output = Option<OutputValue>;

pub fn as_output(x: impl serde::Serialize + Send + 'static) -> Result<Output> {
    Ok(Some(serde_json::to_value(x)?))
}

/// Plugin interface for extensible behavior
/// between the agent and manager.
///
/// Maintains internal state and sends and receives messages.
///
/// Implementors of this trait should add themselves
/// to the `plugin_registry` below.
pub trait DaemonPlugin: std::fmt::Debug {
    /// Returns full listing of information upon session esablishment
    fn start_session(&self) -> Box<dyn Future<Item = Output, Error = ImlAgentError> + Send> {
        Box::new(future::ok(None))
    }
    /// Return information needed to maintain a manager-agent session, i.e. what
    /// has changed since the start of the session or since the last update.
    ///
    /// If you need to refer to any data from the start_session call, you can
    /// store it as a property on this DaemonPlugin instance.
    ///
    /// This will never be called concurrently with respect to start_session, or
    /// before start_session.
    fn update_session(&self) -> Box<dyn Future<Item = Output, Error = ImlAgentError> + Send> {
        self.start_session()
    }
    /// Handle a message sent from the manager (may be called concurrently with respect to
    /// start_session and update_session).
    fn on_message(
        &self,
        _body: serde_json::Value,
    ) -> Box<dyn Future<Item = AgentResult, Error = ImlAgentError> + Send> {
        Box::new(future::ok(Ok(serde_json::Value::Null)))
    }
    fn teardown(&mut self) -> Result<()> {
        Ok(())
    }
}

pub type DaemonBox = Box<dyn DaemonPlugin + Send + Sync>;

type Callback = Box<dyn Fn() -> DaemonBox + Send + Sync>;

fn mk_callback<D: 'static>(f: &'static (impl Fn() -> D + Sync)) -> Callback
where
    D: DaemonPlugin + Send + Sync,
{
    Box::new(move || Box::new(f()) as DaemonBox)
}

pub type DaemonPlugins = HashMap<PluginName, Callback>;

/// Returns a `HashMap` of plugins available for usage.
pub fn plugin_registry() -> DaemonPlugins {
    let hm: DaemonPlugins = vec![
        ("action_runner".into(), mk_callback(&action_runner::create)),
        ("stratagem".into(), mk_callback(&stratagem::create)),
    ]
    .into_iter()
    .collect();

    info!("Loaded the following DaemonPlugins:");

    for PluginName(key) in hm.keys() {
        info!("{}", key)
    }

    hm
}

/// Get a plugin instance, if it exists
///
/// # Arguments
///
/// * `name` - The plugin to instantiate
/// * `registry` - Plugin registry to use
pub fn get_plugin(name: &PluginName, registry: &DaemonPlugins) -> Result<DaemonBox> {
    match registry.get(name) {
        Some(f) => Ok(f()),
        None => Err(NoPluginError(name.clone()).into()),
    }
}

#[cfg(test)]
pub mod test_plugin {
    use super::{as_output, DaemonPlugin, Output};
    use crate::agent_error::{ImlAgentError, Result};
    use futures::{future, Future};
    use iml_wire_types::AgentResult;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    pub struct TestDaemonPlugin(pub AtomicUsize);

    impl Default for TestDaemonPlugin {
        fn default() -> Self {
            Self(AtomicUsize::new(0))
        }
    }

    impl DaemonPlugin for TestDaemonPlugin {
        fn start_session(&self) -> Box<Future<Item = Output, Error = ImlAgentError> + Send> {
            Box::new(future::ok(self.0.fetch_add(1, Ordering::Relaxed)).and_then(as_output))
        }
        fn update_session(&self) -> Box<Future<Item = Output, Error = ImlAgentError> + Send> {
            self.start_session()
        }
        fn on_message(
            &self,
            body: serde_json::Value,
        ) -> Box<Future<Item = AgentResult, Error = ImlAgentError> + Send> {
            Box::new(future::ok(Ok(body)))
        }
        fn teardown(&mut self) -> Result<()> {
            self.0.store(0, Ordering::Relaxed);

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        get_plugin, mk_callback, test_plugin::TestDaemonPlugin, DaemonPlugin, DaemonPlugins,
    };
    use crate::agent_error::Result;
    use futures::Future;
    use serde_json::json;

    fn run<R: Send + 'static, E: Send + 'static>(
        fut: impl Future<Item = R, Error = E> + Send + 'static,
    ) -> std::result::Result<R, E> {
        tokio::runtime::Runtime::new().unwrap().block_on_all(fut)
    }

    #[test]
    fn test_daemon_plugin_start_session() -> Result<()> {
        let mut x = TestDaemonPlugin::default();

        let actual = run(x.start_session())?;

        // dbg!(actual);

        assert_eq!(actual, Some(json!(0)));

        assert_eq!(x.0.get_mut(), &mut 1);

        Ok(())
    }

    #[test]
    fn test_daemon_plugin_update_session() -> Result<()> {
        let mut x = TestDaemonPlugin::default();

        run(x.start_session())?;
        let actual = run(x.update_session())?;

        assert_eq!(actual, Some(json!(1)));

        assert_eq!(x.0.get_mut(), &mut 2);

        Ok(())
    }

    #[test]
    fn test_daemon_plugin_teardown_session() -> Result<()> {
        let mut x = TestDaemonPlugin::default();

        run(x.start_session())?;
        x.teardown()?;

        assert_eq!(x.0.get_mut(), &mut 0);

        Ok(())
    }

    #[test]
    fn test_daemon_plugin_get_from_registry() -> Result<()> {
        let registry: DaemonPlugins = vec![(
            "test_daemon_plugin".into(),
            mk_callback(&TestDaemonPlugin::default),
        )]
        .into_iter()
        .collect();

        let p1 = get_plugin(&"test_daemon_plugin".into(), &registry)?;

        let actual = run(p1.start_session())?;

        assert_eq!(actual, Some(json!(0)));

        let actual = run(p1.update_session())?;

        assert_eq!(actual, Some(json!(1)));

        let p2 = get_plugin(&"test_daemon_plugin".into(), &registry)?;

        let actual = run(p2.start_session())?;

        assert_eq!(actual, Some(json!(0)));

        Ok(())
    }
}
