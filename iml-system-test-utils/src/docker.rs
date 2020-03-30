use crate::{iml, CheckedStatus, iml::IML_DOCKER_PATH};
use iml_systemd;
use iml_wire_types::Branding;
use std::{io, thread, time, str};
use tokio::{
    fs::{canonicalize, File},
    io::AsyncWriteExt,
    process::Command,
};

pub struct DockerSetup {
  pub use_stratagem: bool,
  pub branding: Branding,
}

pub async fn docker() -> Result<Command, io::Error> {
    let mut x = Command::new("docker");

    let path = canonicalize(IML_DOCKER_PATH).await?;

    x.current_dir(path);

    Ok(x)
}

pub async fn deploy_iml_stack() -> Result<(), io::Error> {
    iml_systemd::start_unit_and_wait("iml-docker.service".into(), 400).await?;

    Ok(())
}

pub async fn get_docker_service_count() -> Result<u16, io::Error> {
  let mut x = docker().await?;

  let services = x.arg("service").arg("ls").output().await?;

  let output = str::from_utf8(&services.stdout).expect("Couldn't read docker service list.");
  let cnt: u16 = output.lines().count() as u16 - 1; // subtract the header

  Ok(cnt)
} 

pub async fn remove_iml_stack() -> Result<(), io::Error> {
    iml_systemd::stop_unit("iml-docker.service".into()).await?;

    let mut x = docker().await?;

    x.arg("stack").arg("rm").arg("iml").checked_status().await?;

    // Make sure there are no services.
    let one_sec = time::Duration::from_millis(1000);
    let mut count = get_docker_service_count().await?;
    while count > 0 {
      println!("Waiting on all docker services to be shut down.");
      thread::sleep(one_sec);
      count = get_docker_service_count().await?;
    }
    
    Ok(())
}

pub async fn system_prune() -> Result<(), io::Error> {
    let mut x = docker().await?;

    x.arg("system")
        .arg("prune")
        .arg("--force")
        .arg("--all")
        .checked_status()
        .await?;

    Ok(())
}

pub async fn volume_prune() -> Result<(), io::Error> {
    let mut x = docker().await?;

    x.arg("volume")
        .arg("prune")
        .arg("--force")
        .checked_status()
        .await?;

    Ok(())
}

pub async fn iml_stack_loaded() -> Result<bool, io::Error> {
    let x = iml::list_servers().await?.status().await?;

    Ok(x.success())
}

pub async fn wait_for_iml_stack() -> Result<(), io::Error> {
    let mut x = iml_stack_loaded().await?;
    let one_sec = time::Duration::from_millis(1000);

    while !x {
        println!("Waiting on the docker stack to load.");
        thread::sleep(one_sec);
        x = iml_stack_loaded().await?;
    }

    Ok(())
}

pub async fn configure_docker_setup(setup: &DockerSetup) -> Result<(), io::Error> {
  let config = format!(r#"USE_STRATAGEM={}
BRANDING={}"#, setup.use_stratagem, setup.branding.to_string());

  let mut path = canonicalize(IML_DOCKER_PATH).await?;
  path.push("setup");

  let mut config_path = path.clone();
  config_path.push("config");

  let mut file = File::create(config_path).await?;
  file.write_all(config.as_bytes()).await?;

  if setup.use_stratagem {
    let mut server_profile_path = path.clone();
    server_profile_path.push("stratagem-server.profile");

    let stratagem_server_profile = r#"{
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
    let mut file = File::create(server_profile_path).await?;
    file.write_all(stratagem_server_profile.as_bytes()).await?;

    let mut client_profile_path = path.clone();
    client_profile_path.push("stratagem-client.profile");

    let stratagem_client_profile = r#"{
  "ui_name": "Stratagem Client Node",
  "managed": true,
  "worker": true,
  "name": "stratagem_client",
  "initial_state": "managed",
  "ntp": true,
  "corosync": false,
  "corosync2": false,
  "pacemaker": false,
  "ui_description": "A client that can receive stratagem data",
  "packages": [
    "python2-iml-agent-management",
    "lustre-client"
  ],
  "repolist": [
    "base",
    "lustre-client"
  ]
}
"#;
    let mut file = File::create(client_profile_path).await?;
    file.write_all(stratagem_client_profile.as_bytes()).await?;
  }

  Ok(())
}

pub async fn configure_docker_overrides() -> Result<(), io::Error> {
    let overrides = r#"version: "3.7"

services:
  job-scheduler:
    extra_hosts:
      - "mds1.local:10.73.10.11"
      - "mds2.local:10.73.10.12"
      - "oss1.local:10.73.10.21"
      - "oss2.local:10.73.10.22"
      - "c1.local:10.73.10.31"
    environment:
      - "NTP_SERVER_HOSTNAME=10.73.10.1"
  iml-warp-drive:
    environment:
      - RUST_LOG=debug
  iml-action-runner:
    environment:
      - RUST_LOG=debug
  iml-api:
    environment:
      - RUST_LOG=debug
  iml-ostpool:
    environment:
      - RUST_LOG=debug
  iml-stats:
    environment:
      - RUST_LOG=debug
  iml-agent-comms:
    environment:
      - RUST_LOG=debug
  device-aggregator:
    environment:
      - RUST_LOG=debug
"#;

    let mut path = canonicalize(IML_DOCKER_PATH).await?;
    path.push("docker-compose.overrides.yml");

    let mut file = File::create(path).await?;
    file.write_all(overrides.as_bytes()).await?;

    Ok(())
}