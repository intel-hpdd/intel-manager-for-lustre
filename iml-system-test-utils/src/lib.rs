pub mod docker;
pub mod iml;
pub mod ssh;
pub mod vagrant;

use async_trait::async_trait;
use iml_cmd::CmdError;
use iml_systemd::SystemdError;
use iml_wire_types::Branding;
use ssh::create_iml_diagnostics;
use std::{io, time::Duration};
use tokio::{process::Command, time::delay_for};

pub struct SetupConfig {
    pub use_stratagem: bool,
    pub branding: Branding,
}

pub enum SetupConfigType {
    RpmSetup(SetupConfig),
    DockerSetup(SetupConfig),
}

impl From<&SetupConfigType> for String {
    fn from(config: &SetupConfigType) -> Self {
        match config {
            SetupConfigType::RpmSetup(c) => format!(
                r#"USE_STRATAGEM = {}
BRANDING = "{}""#,
                if c.use_stratagem { "True" } else { "False" },
                c.branding.to_string()
            ),
            SetupConfigType::DockerSetup(c) => format!(
                r#"USE_STRATAGEM={}
            BRANDING={}"#,
                c.use_stratagem,
                c.branding.to_string()
            ),
        }
    }
}

impl<'a> From<&'a SetupConfigType> for &'a SetupConfig {
    fn from(config: &'a SetupConfigType) -> Self {
        match config {
            SetupConfigType::RpmSetup(x) => x,
            SetupConfigType::DockerSetup(x) => x,
        }
    }
}

pub const STRATAGEM_SERVER_PROFILE: &str = r#"{
    "ui_name": "Stratagem Policy Engine Server",
    "ui_description": "A server running the Stratagem Policy Engine",
    "managed": false,
    "worker": false,
    "name": "stratagem_server",
    "initial_state": "monitored",
    "ntp": false,
    "corosync": false,
    "corosync2": false,
    "pacemaker": false,
    "repolist": [
      "base"
    ],
    "packages": [],
    "validation": [
      {
        "description": "A server running the Stratagem Policy Engine",
        "test": "distro_version < 8 and distro_version >= 7"
      }
    ]
  }
  "#;

pub const STRATAGEM_CLIENT_PROFILE: &str = r#"{
    "ui_name": "Stratagem Client Node",
    "managed": false,
    "worker": true,
    "name": "stratagem_client",
    "initial_state": "monitored",
    "ntp": false,
    "corosync": false,
    "corosync2": false,
    "pacemaker": false,
    "ui_description": "A client that can receive stratagem data",
    "packages": [
      "python2-iml-agent-management"
    ],
    "repolist": [
      "base"
    ]
  }
  "#;

pub async fn try_command_n_times(
    max_tries: u32,
    delay: u64,
    cmd: &mut Command,
) -> Result<(), CmdError> {
    let mut count = 1;
    let mut r = cmd.status().await?;

    // try to run the command max_tries times until it succeeds. There is a delay of 1 second.
    while !r.success() && count < max_tries {
        tracing::debug!("Trying command: {:?} - Attempt #{}", cmd, count + 1);
        count += 1;

        delay_for(Duration::from_secs(delay)).await;

        r = cmd.status().await?;
    }

    if r.success() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Command {:?} failed to succeed after {} attempts.",
                cmd, max_tries
            ),
        )
        .into())
    }
}

pub fn get_local_server_names<'a>(servers: &'a [&'a str]) -> Vec<String> {
    servers
        .iter()
        .map(move |x| format!("{}.local", x))
        .collect()
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SystemTestError {
    #[error(transparent)]
    CmdError(#[from] CmdError),
    #[error(transparent)]
    SystemdError(#[from] SystemdError),
}

pub trait ServerList {
    fn to_server_list(&self) -> Vec<&str>;
}

impl ServerList for Vec<(String, &[&str])> {
    fn to_server_list(&self) -> Vec<&str> {
        let server_set: Vec<_> = self.iter().flat_map(|(_, x)| *x).copied().collect();
        let mut xs: Vec<&str> = server_set.into_iter().collect();
        xs.dedup();

        xs
    }
}

#[async_trait]
pub trait WithSos {
    async fn handle_test_result(self, hosts: &[&str], prefix: &str) -> Result<(), SystemTestError>;
}

#[async_trait]
impl<T: Into<SystemTestError> + Send> WithSos for Result<(), T> {
    async fn handle_test_result(self, hosts: &[&str], prefix: &str) -> Result<(), SystemTestError> {
        create_iml_diagnostics(hosts, prefix).await?;

        self.map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_server_list() {
        let config = vagrant::ClusterConfig::default();
        let mds_servers = &config.mds_servers()[..];
        let oss_servers = &config.oss_servers()[..];
        let client_servers = &config.client_servers()[..];

        let xs: Vec<(String, &[&str])> = vec![
            ("stratagem_server".into(), mds_servers),
            ("base_monitored".into(), oss_servers),
            ("stratagem_client".into(), client_servers),
        ];

        let servers = xs.to_server_list();

        assert_eq!(servers, vec!["mds1", "mds2", "oss1", "oss2", "c1"]);
    }
}
