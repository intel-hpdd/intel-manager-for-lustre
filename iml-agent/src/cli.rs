// Copyright (c) 2019 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

use futures::Future;
use iml_agent::action_plugins::stratagem::{
    action_purge, action_warning,
    server::{generate_cooked_config, trigger_scan, Counter, StratagemCounters},
};
use prettytable::{cell, row, Table};
use spinners::{Spinner, Spinners};
use std::{
    convert::TryInto,
    fs::File,
    io::{self, BufRead, BufReader},
    process::exit,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub enum StratagemCommand {
    /// Kickoff a Stratagem scan
    #[structopt(name = "scan")]
    Scan {
        /// The full path of the device to scan
        #[structopt(short = "d", long = "device")]
        device_path: String,
    },
}

#[derive(Debug, StructOpt)]
pub struct FidInput {
    #[structopt(short = "i")]
    /// File to read from, "-" for stdin, or unspecified for on cli
    input: Option<String>,

    #[structopt(name = "FSNAME")]
    /// Lustre filesystem name, or mountpoint
    fsname: String,

    #[structopt(name = "FIDS")]
    /// Optional list of FIDs to purge
    fidlist: Vec<String>,
}

#[derive(Debug, StructOpt)]
pub enum StratagemClientCommand {
    #[structopt(name = "warning")]
    /// Run warning action
    Warning {
        #[structopt(short = "o")]
        /// File to write to, or "-" or unspecified for stdout
        output: Option<String>,

        #[structopt(flatten)]
        fidopts: FidInput,
    },

    #[structopt(name = "purge")]
    /// Run purge action
    Purge {
        #[structopt(flatten)]
        fidopts: FidInput,
    },
}

#[derive(StructOpt, Debug)]
#[structopt(name = "iml-agent")]
/// The Integrated Manager for Lustre Agent CLI
pub enum App {
    #[structopt(name = "stratagem")]
    /// Work with Stratagem server
    StratagemServer {
        #[structopt(subcommand)]
        command: StratagemCommand,
    },

    #[structopt(name = "stratagem_client")]
    /// Work with Stratagem client
    StratagemClient {
        #[structopt(subcommand)]
        command: StratagemClientCommand,
    },
}

/// Takes an asynchronous computation (Future), runs it to completion
/// and returns the result.
///
/// Even though the action is asynchronous, this fn will block until
/// the future resolves.
fn run_cmd<R: Send + 'static, E: Send + 'static>(
    fut: impl Future<Item = R, Error = E> + Send + 'static,
) -> std::result::Result<R, E> {
    tokio::runtime::Runtime::new().unwrap().block_on_all(fut)
}

fn input_to_iter(input: Option<String>, fidlist: Vec<String>) -> Box<Iterator<Item = String>> {
    match input {
        None => {
            if fidlist.is_empty() {
                Box::new(
                    BufReader::new(io::stdin())
                        .lines()
                        .map(|x| x.expect("Failed to readline from stdin")),
                )
            } else {
                Box::new(fidlist.into_iter())
            }
        }
        Some(name) => {
            let buf: Box<BufRead> = match name.as_ref() {
                "-" => Box::new(BufReader::new(io::stdin())),
                _ => {
                    let f = match File::open(&name) {
                        Ok(x) => x,
                        Err(e) => {
                            log::error!("Failed to open {}: {}", &name, e);
                            exit(exitcode::CANTCREAT);
                        }
                    };
                    Box::new(BufReader::new(f))
                }
            };
            Box::new(
                buf.lines()
                    .map(|x| x.expect("Failed to readline from file")),
            )
        }
    }
}

fn humanize(s: &str) -> String {
    s.replace('_', " ")
}

/// Takes a `Vec` of `StratagemCounters` and
/// prints a histogram and table for each one.
///
/// If a `StratagemClassifyCounter` is encountered, this
/// fn will recurse and print the nested counter before the parent.
fn print_counters(xs: Vec<StratagemCounters>) {
    log::info!("Looking at: {:?}", xs);

    let mut table = Table::new();
    table.add_row(row!["Name", "Count"]);

    let mut h = v_hist::init();
    h.max_width = 50;

    for x in xs {
        add_counter_entry(&x, &mut table, &mut h);

        if let StratagemCounters::StratagemClassifyCounter(x) = x {
            print_counters(
                x.classify
                    .counters
                    .into_iter()
                    .map(StratagemCounters::StratagemCounter)
                    .collect(),
            );
        }
    }

    h.draw();

    println!("\n\n");

    table.printstd();
}

fn add_counter_entry(x: impl Counter, t: &mut Table, h: &mut v_hist::Histogram) {
    let name = humanize(&x.name());
    let count: usize = x
        .count()
        .try_into()
        .expect("Conversion to usize for counter failed");

    t.add_row(row![name.clone(), count]);

    h.add_entry(name, count);
}

fn main() {
    env_logger::init();

    let matches = App::from_args();

    match matches {
        App::StratagemClient { command: cmd } => match cmd {
            StratagemClientCommand::Purge { fidopts: opt } => {
                let device = opt.fsname;
                let input = input_to_iter(opt.input, opt.fidlist);

                if action_purge::purge_files(&device, input).is_err() {
                    exit(exitcode::OSERR);
                }
            }
            StratagemClientCommand::Warning {
                output: out,
                fidopts: opt,
            } => {
                let device = opt.fsname;
                let output: Box<io::Write> = match out {
                    Some(file) => Box::new(File::create(file).expect("Failed to create file")),
                    None => Box::new(io::stdout()),
                };
                let input = input_to_iter(opt.input, opt.fidlist);

                if action_warning::write_records(&device, input, output).is_err() {
                    exit(exitcode::IOERR);
                }
            }
        },
        App::StratagemServer { command } => match command {
            StratagemCommand::Scan { device_path } => {
                let cyan = termion::color::Fg(termion::color::Cyan);
                let green = termion::color::Fg(termion::color::Green);
                let reset = termion::color::Fg(termion::color::Reset);

                let s = format!(
                    "{}Scanning{} {}{}{}...",
                    cyan,
                    reset,
                    termion::style::Bold,
                    device_path,
                    reset,
                );
                let s_len = s.len();

                let sp = Spinner::new(Spinners::Dots9, s);

                let data = generate_cooked_config(device_path);

                let result = run_cmd(trigger_scan(data));

                sp.stop();
                println!("{}", termion::clear::CurrentLine);
                print!("{}", termion::cursor::Left(s_len as u16));

                match result {
                    Ok((results_dir, output, _)) => {
                        println!(
                            "{}✔ Scan finished{}. Results located in {}",
                            green, reset, results_dir
                        );

                        for x in output.group_counters {
                            println!(
                                "\n\n\n{}{}Group:{} {}\n",
                                cyan,
                                termion::style::Bold,
                                reset,
                                humanize(&x.name)
                            );

                            print_counters(x.counters);
                        }
                    }
                    Err(e) => {
                        eprintln!("{}", e);

                        exit(exitcode::SOFTWARE);
                    }
                };
            }
        },
    }
}
