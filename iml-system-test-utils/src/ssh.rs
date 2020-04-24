// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::future::try_join_all;
use iml_cmd::{CheckedChildExt, CheckedCommandExt, CmdError};
use std::{
    process::{Output, Stdio},
    str,
};
use tokio::{fs::canonicalize, io::AsyncWriteExt, process::Command};

pub async fn ssh_exec<'a, 'b>(host: &'a str, cmd: &'b str) -> Result<(&'a str, Output), CmdError> {
    println!("Running command {} on {}", cmd, host);
    let path = canonicalize("../vagrant/").await?;

    let mut x = Command::new("ssh");
    x.current_dir(path);

    x.arg("-i")
        .arg("id_rsa")
        .arg("-o")
        .arg("StrictHostKeyChecking=no")
        .arg(host)
        .arg(cmd);

    let out = x.checked_output().await?;

    Ok((host, out))
}

async fn ssh_exec_parallel<'a, 'b>(
    servers: &[&'a str],
    cmd: &'b str,
) -> Result<Vec<(&'a str, Output)>, CmdError> {
    let remote_calls = servers.iter().map(|host| ssh_exec(host, cmd));

    let output = try_join_all(remote_calls).await?;

    for (host, out) in &output {
        println!(
            "ssh output {}: {}",
            host,
            str::from_utf8(&out.stdout).expect("Couldn't read output.")
        );
    }

    Ok(output)
}

pub async fn ssh_script<'a, 'b>(
    host: &'a str,
    script: &'b str,
    args: &[&'b str],
) -> Result<(&'a str, Output), CmdError> {
    let path = canonicalize("../vagrant/").await?;

    let mut script_path = path.clone();
    script_path.push(script);

    let script_content = iml_fs::read_file_to_end(script_path).await?;

    let mut x = Command::new("ssh");
    x.current_dir(path);

    let mut ssh_child = x
        .arg("-i")
        .arg("id_rsa")
        .arg("-o")
        .arg("StrictHostKeyChecking=no")
        .arg(host)
        .arg("bash -s")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .spawn()?;

    let ssh_stdin = ssh_child.stdin.as_mut().unwrap();
    ssh_stdin.write_all(&script_content).await?;

    let out = ssh_child.wait_with_checked_output().await?;

    Ok((host, out))
}

async fn ssh_script_parallel<'a, 'b>(
    servers: &[&'a str],
    script: &'b str,
    args: &[&'b str],
) -> Result<Vec<(&'a str, Output)>, CmdError> {
    let remote_calls = servers.iter().map(|host| ssh_script(host, script, args));

    let output = try_join_all(remote_calls).await?;

    for (host, out) in &output {
        println!(
            "ssh output {}: {}",
            host,
            str::from_utf8(&out.stdout).expect("Couldn't read output.")
        );
    }

    Ok(output)
}

pub async fn install_ldiskfs_no_iml<'a, 'b>(
    hosts: &[&'a str],
    lustre_version: &'b str,
) -> Result<Vec<(&'a str, Output)>, CmdError> {
    ssh_script_parallel(
        hosts,
        "scripts/install_ldiskfs_no_iml.sh",
        &[lustre_version],
    )
    .await
}

pub async fn install_zfs_no_iml<'a, 'b>(
    hosts: &[&'a str],
    lustre_version: &'b str,
) -> Result<Vec<(&'a str, Output)>, CmdError> {
    ssh_script_parallel(hosts, "scripts/install_zfs_no_iml.sh", &[lustre_version]).await
}

pub async fn yum_update<'a, 'b>(hosts: &'b [&'a str]) -> Result<Vec<(&'a str, Output)>, CmdError> {
    ssh_exec_parallel(hosts, "yum clean metadata; yum update -y").await
}

pub async fn configure_ntp_for_host_only_if<'a, 'b>(
    hosts: &'b [&'a str],
) -> Result<Vec<(&'a str, Output)>, CmdError> {
    ssh_script_parallel(hosts, "scripts/configure_ntp.sh", &["10.73.10.1"]).await
}

pub async fn configure_ntp_for_adm<'a, 'b>(
    hosts: &'b [&'a str],
) -> Result<Vec<(&'a str, Output)>, CmdError> {
    ssh_script_parallel(hosts, "scripts/configure_ntp.sh", &["adm.local"]).await
}

pub async fn wait_for_ntp<'a, 'b>(
    hosts: &'b [&'a str],
) -> Result<Vec<(&'a str, Output)>, CmdError> {
    ssh_script_parallel(hosts, "scripts/wait_for_ntp.sh", &[]).await
}
