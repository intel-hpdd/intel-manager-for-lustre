// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use chrono_humanize::{Accuracy, HumanTime, Tense};
use console::style;
use futures::{Future, FutureExt};
use iml_wire_types::{
    snapshot::{ReserveUnit, Snapshot, SnapshotInterval, SnapshotRetention},
    Command, Filesystem, Host, HotpoolConfiguration, OstPool, ServerProfile,
    StratagemConfiguration, StratagemReport,
};
use indicatif::ProgressBar;
use number_formatter::{format_bytes, format_number};
use prettytable::{Row, Table};
use std::{fmt::Display, io, str::FromStr};
use structopt::StructOpt;

pub fn wrap_fut<T>(msg: &str, fut: impl Future<Output = T>) -> impl Future<Output = T> {
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(100);
    pb.set_message(msg);

    fut.inspect(move |_| pb.finish_and_clear())
}

pub fn format_cmd_state(cmd: &Command) -> String {
    if cmd.errored {
        format_error(format!("{} errored", cmd.message))
    } else if cmd.cancelled {
        format_cancelled(&format!("{} cancelled", cmd.message))
    } else {
        format_success(format!("{} successful", cmd.message))
    }
}

pub fn display_cmd_state(cmd: &Command) {
    println!("{}", format_cmd_state(&cmd));
}

pub fn format_cancelled(message: impl Display) -> String {
    format!("🚫 {}", message)
}

pub fn display_cancelled(message: impl Display) {
    println!("{}", format_cancelled(&message));
}

pub fn format_success(message: impl Display) -> String {
    format!("{} {}", style("✔").green(), message)
}

pub fn display_success(message: impl Display) {
    println!("{}", format_success(message))
}

pub fn format_error(message: impl Display) -> String {
    format!("{} {}", style("✗").red(), message)
}

pub fn display_error(message: impl Display) {
    println!("{}", format_error(message))
}

pub fn generate_table<Rows, R>(columns: &[&str], rows: Rows) -> Table
where
    R: IntoIterator,
    R::Item: ToString,
    Rows: IntoIterator<Item = R>,
{
    let mut table = Table::new();

    table.set_titles(Row::from(columns));

    for r in rows {
        table.add_row(Row::from(r));
    }

    table
}

pub fn usage(
    used: Option<u64>,
    total: Option<u64>,
    formatter: fn(f64, Option<usize>) -> String,
) -> String {
    format!(
        "{} / {}",
        used.map(|u| formatter(u as f64, Some(0)))
            .as_deref()
            .unwrap_or("---"),
        total
            .map(|t| formatter(t as f64, Some(0)))
            .as_deref()
            .unwrap_or("---")
    )
}

pub trait IsEmpty {
    fn is_empty(&self) -> bool;
}

impl<T> IsEmpty for Vec<T> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

pub trait IntoTable {
    fn into_table(self) -> Table;
}

impl IntoTable for Vec<Snapshot> {
    fn into_table(self) -> Table {
        generate_table(
            &[
                "Filesystem",
                "Snapshot",
                "Creation Time",
                "State",
                "Comment",
            ],
            self.into_iter().map(|s| {
                vec![
                    s.filesystem_name,
                    s.snapshot_name,
                    s.create_time.to_rfc2822(),
                    match s.mounted {
                        Some(true) => "mounted",
                        Some(false) => "unmounted",
                        None => "---",
                    }
                    .to_string(),
                    s.comment.unwrap_or_else(|| "---".to_string()),
                ]
            }),
        )
    }
}

impl IntoTable for Vec<SnapshotInterval> {
    fn into_table(self) -> Table {
        generate_table(
            &["Id", "Filesystem", "Interval", "Use Barrier", "Last Run"],
            self.into_iter().map(|i| {
                vec![
                    i.id.to_string(),
                    i.filesystem_name,
                    chrono::Duration::from_std(i.interval.0)
                        .map(HumanTime::from)
                        .map(|x| x.to_text_en(Accuracy::Precise, Tense::Present))
                        .unwrap_or_else(|_| "---".to_string()),
                    i.use_barrier.to_string(),
                    i.last_run
                        .map(|t| t.to_rfc2822())
                        .unwrap_or_else(|| "---".to_string()),
                ]
            }),
        )
    }
}

impl IntoTable for Vec<SnapshotRetention> {
    fn into_table(self) -> Table {
        generate_table(
            &["Id", "Filesystem", "Reserve", "Keep", "Last Run"],
            self.into_iter().map(|r| {
                vec![
                    r.id.to_string(),
                    r.filesystem_name,
                    format!(
                        "{} {}",
                        r.reserve_value,
                        match r.reserve_unit {
                            ReserveUnit::Percent => "%",
                            ReserveUnit::Gibibytes => "GiB",
                            ReserveUnit::Tebibytes => "TiB",
                        }
                    ),
                    r.keep_num.to_string(),
                    r.last_run
                        .map(|t| t.to_rfc2822())
                        .unwrap_or_else(|| "---".to_string()),
                ]
            }),
        )
    }
}

impl IntoTable for Vec<StratagemReport> {
    fn into_table(self) -> Table {
        generate_table(
            &["Filename", "Size", "Modify Time"],
            self.into_iter()
                .map(|r| vec![r.filename, r.size.to_string(), r.modify_time.to_rfc2822()]),
        )
    }
}

impl IntoTable for Vec<Host> {
    fn into_table(self) -> Table {
        generate_table(
            &["Id", "FQDN", "State", "Nids"],
            self.into_iter().map(|h| {
                vec![
                    h.id.to_string(),
                    h.fqdn,
                    h.state,
                    h.nids.unwrap_or_default().join(" "),
                ]
            }),
        )
    }
}

impl IntoTable for Vec<StratagemConfiguration> {
    fn into_table(self) -> Table {
        generate_table(
            &["Id", "Filesystem", "State", "Interval", "Purge", "Report"],
            self.into_iter().map(|x| {
                vec![
                    x.id.to_string(),
                    x.filesystem,
                    x.state,
                    x.interval.to_string(),
                    x.purge_duration.map(|x| x.to_string()).unwrap_or_default(),
                    x.report_duration.map(|x| x.to_string()).unwrap_or_default(),
                ]
            }),
        )
    }
}

impl IntoTable for Vec<OstPool> {
    fn into_table(self) -> Table {
        generate_table(
            &["Filesystem", "Pool Name", "OST Count"],
            self.into_iter()
                .map(|x| vec![x.filesystem, x.name, x.osts.len().to_string()]),
        )
    }
}

impl IntoTable for Vec<Filesystem> {
    fn into_table(self) -> Table {
        generate_table(
            &[
                "Name", "State", "Space", "Inodes", "Clients", "MDTs", "OSTs",
            ],
            self.into_iter().map(|x| {
                vec![
                    x.label,
                    x.state,
                    usage(
                        x.bytes_free.map(|x| x as u64),
                        x.bytes_total.map(|x| x as u64),
                        format_bytes,
                    ),
                    usage(x.files_free, x.files_total, format_number),
                    format!("{}", x.client_count.unwrap_or(0)),
                    x.mdts.len().to_string(),
                    x.osts.len().to_string(),
                ]
            }),
        )
    }
}

impl IntoTable for Vec<HotpoolConfiguration> {
    fn into_table(self) -> Table {
        generate_table(
            &["id", "FS", "State", "Minage", "Free HI", "Free LO"],
            self.into_iter().map(|x| {
                vec![
                    x.id.to_string(),
                    x.filesystem,
                    x.state,
                    x.minage.to_string(),
                    x.freehi.unwrap_or(-1).to_string(),
                    x.freelo.unwrap_or(-1).to_string(),
                ]
            }),
        )
    }
}

impl IntoTable for Vec<ServerProfile> {
    fn into_table(self) -> Table {
        generate_table(
            &["Profile", "Name", "Description"],
            self.into_iter()
                .filter(|x| x.user_selectable)
                .map(|x| vec![x.name, x.ui_name, x.ui_description]),
        )
    }
}

pub trait IntoDisplayType {
    fn into_display_type(self, display_type: DisplayType) -> String;
}

impl<T> IntoDisplayType for T
where
    T: IsEmpty + IntoTable + serde::Serialize,
{
    fn into_display_type(self, display_type: DisplayType) -> String {
        match display_type {
            DisplayType::Json => {
                serde_json::to_string_pretty(&self).expect("Cannot serialize item to JSON")
            }
            DisplayType::Yaml => {
                serde_yaml::to_string(&self).expect("Cannot serialize item to YAML")
            }
            DisplayType::Tabular => {
                if self.is_empty() {
                    "".to_string()
                } else {
                    self.into_table().to_string()
                }
            }
        }
    }
}

#[derive(StructOpt, Debug)]
pub enum DisplayType {
    Json,
    Yaml,
    Tabular,
}

impl Default for DisplayType {
    fn default() -> Self {
        Self::Tabular
    }
}

impl FromStr for DisplayType {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "json" => Ok(Self::Json),
            "yaml" => Ok(Self::Yaml),
            "tabular" => Ok(Self::Tabular),
            _ => Err(Self::Err::new(
                io::ErrorKind::InvalidInput,
                "Couldn't parse display type.",
            )),
        }
    }
}
