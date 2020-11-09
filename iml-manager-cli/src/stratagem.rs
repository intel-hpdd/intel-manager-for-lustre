// Copyright (c) 2020 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use crate::{
    api_utils::{delete, first, get, graphql, post, wait_for_cmd_display},
    display_utils::{wrap_fut, DisplayType, IntoDisplayType as _},
    error::{
        DurationParseError, ImlManagerCliError, RunStratagemCommandResult,
        RunStratagemValidationError,
    },
    hotpool::{hotpool_cli, HotpoolCommand},
};
use console::Term;
use iml_graphql_queries::stratagem as stratagem_queries;
use iml_manager_client::ImlManagerClientError;
use iml_wire_types::{ApiList, CmdWrapper, EndpointName, Filesystem, StratagemConfiguration};
use structopt::{clap::arg_enum, StructOpt};

#[derive(Debug, StructOpt)]
pub enum StratagemCommand {
    /// Kickoff a Stratagem scan
    #[structopt(name = "scan")]
    Scan(StratagemScanData),
    /// Configure Stratagem scanning interval
    #[structopt(name = "interval")]
    Interval(IntervalCommand),
    /// Kickoff a Stratagem Filesync
    #[structopt(name = "filesync")]
    Filesync(StratagemFilesyncData),
    /// Kickoff a Stratagem Cloudsync
    #[structopt(name = "cloudsync")]
    Cloudsync(StratagemCloudsyncData),
    /// Work with Stratagem reports
    #[structopt(name = "report")]
    Report {
        #[structopt(subcommand)]
        command: Option<ReportCommand>,
    },
    #[structopt(name = "hotpool")]
    Hotpools(HotpoolCommand),
}

#[derive(Debug, StructOpt)]
pub enum ReportCommand {
    /// List all existing Stratagem reports (default)
    #[structopt(name = "list")]
    List {
        /// Display type: json, yaml, tabular
        #[structopt(short = "d", long = "display", default_value = "tabular")]
        display_type: DisplayType,
    },
    /// Delete Stratagem reports
    #[structopt(name = "remove")]
    Delete {
        /// Report name to delete
        #[structopt(required = true, min_values = 1)]
        name: Vec<String>,
    },
}

#[derive(Debug, StructOpt)]
pub enum IntervalCommand {
    /// List all existing Stratagem intervals
    #[structopt(name = "list")]
    List {
        /// Display type: json, yaml, tabular
        #[structopt(short = "d", long = "display", default_value = "tabular")]
        display_type: DisplayType,
    },
    /// Create Stratagem scan interval
    #[structopt(name = "create")]
    Create(IntervalCommandConfig),
    /// Remove a Stratagem interval
    #[structopt(name = "remove")]
    Remove(StratagemRemoveData),
}

#[derive(Debug, StructOpt, serde::Serialize)]
pub struct IntervalCommandConfig {
    /// Filesystem to configure
    filesystem: String,
    /// Interval to scan
    #[structopt(parse(try_from_str = parse_duration))]
    interval: u64,
    /// The report duration
    #[structopt(short = "r", long = "report", parse(try_from_str = parse_duration))]
    report_duration: Option<u64>,
    /// The purge duration
    #[structopt(short = "p", long = "purge", parse(try_from_str = parse_duration))]
    purge_duration: Option<u64>,
}

#[derive(Debug, StructOpt, serde::Serialize)]
pub struct StratagemRemoveData {
    /// Filesystem to unconfigure
    filesystem: String,
}

#[derive(serde::Serialize, StructOpt, Debug)]
pub struct StratagemScanData {
    /// The name of the filesystem to scan
    filesystem: String,
    /// The report duration, specified as a humantime string
    /// EX: 1hour
    #[structopt(short = "r", long = "report", min_values = 1)]
    report_duration: Option<Vec<String>>,
    /// The purge duration, specified as a humantime string
    /// EX: 1hour
    #[structopt(short = "p", long = "purge", min_values = 1)]
    purge_duration: Option<Vec<String>>,
}

arg_enum! {
    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
    #[serde(rename_all = "lowercase")]
    pub enum FilesyncAction {
    Push,
    }
}

arg_enum! {
    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
    #[serde(rename_all = "lowercase")]
    pub enum CloudsyncAction {
    Push,
    Pull,
    }
}

#[derive(serde::Serialize, StructOpt, Debug)]
pub struct StratagemFilesyncData {
    /// The name of the filesystem to scan
    filesystem: String,
    /// The remote filesystem
    #[structopt(short = "r", long = "remote")]
    remote: String,
    /// Match expression
    #[structopt(short = "e", long = "expression")]
    expression: String,
}

#[derive(serde::Serialize, StructOpt, Debug)]
pub struct StratagemCloudsyncData {
    /// action, either push or pull
    action: CloudsyncAction,
    /// The name of the filesystem to scan
    filesystem: String,
    /// The s3 instance
    #[structopt(short = "r", long = "remote")]
    remote: String,
    /// Match expression
    #[structopt(short = "e", long = "expression")]
    expression: String,
}

fn parse_duration(src: &str) -> Result<u64, ImlManagerCliError> {
    if src.len() < 2 {
        return Err(DurationParseError::InvalidValue.into());
    }

    let mut val = String::from(src);
    let unit = val.pop();

    let val = val.parse::<u64>()?;

    match unit {
        Some('h') => Ok(val * 3_600_000),
        Some('d') => Ok(val * 86_400_000),
        Some('m') => Ok(val * 60_000),
        Some('s') => Ok(val * 1_000),
        Some('1'..='9') => Err(DurationParseError::NoUnit.into()),
        _ => Err(DurationParseError::InvalidUnit.into()),
    }
}

/// Given some params, does a fetch for the item in the API
async fn fetch_one<T: EndpointName + std::fmt::Debug + serde::de::DeserializeOwned>(
    params: impl serde::Serialize,
) -> Result<T, ImlManagerCliError> {
    let x = get(T::endpoint_name(), params).await?;

    first(x)
}

async fn get_stratagem_config_by_fs_name(
    name: &str,
) -> Result<StratagemConfiguration, ImlManagerCliError> {
    let fs: Filesystem = fetch_one(serde_json::json!({
        "name": name,
        "dehydrate__mgt": false
    }))
    .await?;

    fetch_one(serde_json::json!({ "filesystem": fs.id })).await
}

async fn handle_cmd_resp(
    resp: iml_manager_client::Response,
) -> Result<CmdWrapper, ImlManagerCliError> {
    let status = resp.status();

    let body = resp.text().await.map_err(ImlManagerClientError::from)?;

    if status.is_success() {
        Ok(serde_json::from_str::<CmdWrapper>(&body)?)
    } else if status.is_client_error() {
        tracing::debug!("body: {:?}", body);

        serde_json::from_str::<RunStratagemValidationError>(&body)
            .map(std::convert::Into::into)
            .map(Err)?
    } else if status.is_server_error() {
        Err(RunStratagemValidationError {
            code: RunStratagemCommandResult::ServerError,
            message: "Internal server error.".into(),
        }
        .into())
    } else {
        Err(RunStratagemValidationError {
            code: RunStratagemCommandResult::UnknownError,
            message: "Unknown error.".into(),
        }
        .into())
    }
}

fn list_stratagem_configurations(
    stratagem_configs: Vec<StratagemConfiguration>,
    display_type: DisplayType,
) {
    let term = Term::stdout();

    tracing::debug!("Stratagem Configurations: {:?}", stratagem_configs);

    let x = stratagem_configs.into_display_type(display_type);

    term.write_line(&x).unwrap();
}

async fn report_cli(cmd: ReportCommand) -> Result<(), ImlManagerCliError> {
    match cmd {
        ReportCommand::List { display_type } => {
            let query = stratagem_queries::list_reports::build();

            let resp: iml_graphql_queries::Response<stratagem_queries::list_reports::Resp> =
                graphql(query).await?;
            let reports = Result::from(resp)?.data.stratagem.stratagem_reports;

            let x = reports.into_display_type(display_type);

            let term = Term::stdout();
            term.write_line(&x).unwrap();

            Ok(())
        }
        ReportCommand::Delete { name } => {
            for n in name {
                let query = stratagem_queries::delete_report::build(n);

                let _resp: iml_graphql_queries::Response<stratagem_queries::delete_report::Resp> =
                    graphql(query).await?;
            }
            Ok(())
        }
    }
}

async fn interval_cli(cmd: IntervalCommand) -> Result<(), ImlManagerCliError> {
    match cmd {
        IntervalCommand::List { display_type } => {
            let r: ApiList<StratagemConfiguration> = wrap_fut(
                "Finding existing intervals...",
                get(
                    StratagemConfiguration::endpoint_name(),
                    serde_json::json!({ "limit": 0 }),
                ),
            )
            .await?;

            if r.objects.is_empty() {
                println!("No Stratagem intervals found");
            } else {
                list_stratagem_configurations(r.objects, display_type);
            }
            Ok(())
        }
        IntervalCommand::Create(c) => {
            let r = post(StratagemConfiguration::endpoint_name(), c).await?;

            let CmdWrapper { command } = handle_cmd_resp(r).await?;

            wait_for_cmd_display(command).await?;
            Ok(())
        }
        IntervalCommand::Remove(StratagemRemoveData { filesystem }) => {
            let x = get_stratagem_config_by_fs_name(&filesystem).await?;

            let r = delete(
                &format!("{}/{}", StratagemConfiguration::endpoint_name(), x.id),
                Vec::<(String, String)>::new(),
            )
            .await?;

            let CmdWrapper { command } = handle_cmd_resp(r).await?;

            wait_for_cmd_display(command).await?;
            Ok(())
        }
    }
}

pub async fn stratagem_cli(command: StratagemCommand) -> Result<(), ImlManagerCliError> {
    match command {
        StratagemCommand::Hotpools(command) => {
            hotpool_cli(command).await?;
        }
        StratagemCommand::Scan(data) => {
            let query = stratagem_queries::fast_file_scan::build(
                &data.filesystem,
                data.report_duration.map(|xs| xs.join(" ")),
                data.purge_duration.map(|xs| xs.join(" ")),
            );

            let resp: iml_graphql_queries::Response<stratagem_queries::fast_file_scan::Resp> =
                graphql(query).await?;

            let command = Result::from(resp)?.data.stratagem.run_fast_file_scan;

            wait_for_cmd_display(command).await?;
        }
        StratagemCommand::Filesync(data) => {
            let query = stratagem_queries::filesync::build(
                &data.filesystem,
                data.remote,
                data.expression,
                "push",
            );

            let resp: iml_graphql_queries::Response<stratagem_queries::filesync::Resp> =
                graphql(query).await?;

            let command = Result::from(resp)?.data.stratagem.run_filesync;

            wait_for_cmd_display(command).await?;
        }
        StratagemCommand::Cloudsync(data) => {
            let query = stratagem_queries::cloudsync::build(
                &data.filesystem,
                data.remote,
                data.expression,
                data.action,
            );

            let resp: iml_graphql_queries::Response<stratagem_queries::cloudsync::Resp> =
                graphql(query).await?;

            let command = Result::from(resp)?.data.stratagem.run_cloudsync;

            tracing::debug!("run_cloudsync: {:?}", command);

            wait_for_cmd_display(command).await?;
        }
        StratagemCommand::Interval(cmd) => interval_cli(cmd).await?,
        StratagemCommand::Report { command } => {
            report_cli(command.unwrap_or(ReportCommand::List {
                display_type: DisplayType::Tabular,
            }))
            .await?
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_with_days() {
        match parse_duration("273d") {
            Ok(x) => assert_eq!(x, 23_587_200_000),
            _ => panic!("Duration parser should not have errored!"),
        }
    }

    #[test]
    fn test_parse_duration_with_hours() {
        match parse_duration("273h") {
            Ok(x) => assert_eq!(x, 982_800_000),
            _ => panic!("Duration parser should not have errored!"),
        }
    }

    #[test]
    fn test_parse_duration_with_minutes() {
        match parse_duration("273m") {
            Ok(x) => assert_eq!(x, 16_380_000),
            _ => panic!("Duration parser should not have errored!"),
        }
    }

    #[test]
    fn test_parse_duration_with_seconds() {
        match parse_duration("273s") {
            Ok(x) => assert_eq!(x, 273_000),
            _ => panic!("Duration parser should not have errored!"),
        }
    }

    #[test]
    fn test_parse_duration_with_invalid_unit() {
        match parse_duration("273x") {
            Ok(_) => panic!("Duration should have Errored with InvalidUnit"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Invalid unit. Valid units include 'h' and 'd'."
            ),
        }
    }

    #[test]
    fn test_parse_duration_without_unit() {
        match parse_duration("273") {
            Ok(_) => panic!("Duration should have Errored with NoUnit"),
            Err(e) => assert_eq!(e.to_string(), "No unit specified."),
        }
    }

    #[test]
    fn test_parse_duration_with_insufficient_data() {
        match parse_duration("2") {
            Ok(_) => panic!("Duration should have Errored with InvalidValue"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Invalid value specified. Must be a valid integer."
            ),
        }
    }

    #[test]
    fn test_parse_duration_with_invalid_data() {
        match parse_duration("abch") {
            Ok(_) => panic!("Duration should have Errored with invalid digit"),
            Err(e) => assert_eq!(e.to_string(), "invalid digit found in string"),
        }
    }
}
