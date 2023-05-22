// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg_attr(coverage, feature(no_coverage))]

use std::str::FromStr;

use anyhow::Result;
use clap::{command, Arg, Command, Parser};
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter, EnumString, IntoStaticStr};
use tracing::Level;

#[cfg(enable_task_local_alloc)]
risingwave_common::enable_task_local_jemalloc_on_unix!();

#[cfg(not(enable_task_local_alloc))]
risingwave_common::enable_jemalloc_on_unix!();

const BINARY_NAME: &str = "risingwave";
const ARGS_ID: &str = "args";

/// Component to lanuch.
#[derive(Clone, Copy, EnumIter, EnumString, Display, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
enum Component {
    Compute,
    Meta,
    Frontend,
    Compactor,
    Ctl,
    Playground,
}

impl Component {
    /// Start the component from the given `args` without `argv[0]`.
    fn start(self, mut args: Vec<String>) {
        eprintln!("launching `{}` with args `{:?}`", self, args);
        args.insert(0, format!("{} {}", BINARY_NAME, self)); // mock argv[0]

        match self {
            Self::Compute => compute(args),
            Self::Meta => meta(args),
            Self::Frontend => frontend(args),
            Self::Compactor => compactor(args),
            Self::Ctl => ctl(args),
            Self::Playground => playground(args),
        }
    }

    /// Aliases that can be used to launch the component.
    fn aliases(self) -> Vec<&'static str> {
        match self {
            Component::Compute => vec!["compute-node", "compute_node"],
            Component::Meta => vec!["meta-node", "meta_node"],
            Component::Frontend => vec!["frontend-node", "frontend_node"],
            Component::Compactor => vec!["compactor-node", "compactor_node"],
            Component::Ctl => vec!["risectl"],
            Component::Playground => vec!["play"],
        }
    }

    /// `clap` commands for all components.
    fn commands() -> Vec<Command> {
        Self::iter()
            .map(|c| {
                let name: &'static str = c.into();
                let args = Arg::new(ARGS_ID)
                    // make arguments transaprent to `clap`
                    .num_args(0..)
                    .allow_hyphen_values(true)
                    .trailing_var_arg(true);
                Command::new(name).visible_aliases(c.aliases()).arg(args)
            })
            .collect()
    }
}

fn main() -> Result<()> {
    let risingwave = || command!(BINARY_NAME);
    let command = risingwave()
        // `$ ./meta <args>`
        .multicall(true)
        .subcommands(Component::commands())
        // `$ ./risingwave meta <args>`
        .subcommand(
            risingwave()
                .subcommand_value_name("COMPONENT")
                .subcommand_help_heading("Components")
                .subcommand_required(true)
                .subcommands(Component::commands()),
        )
        .disable_help_flag(true); // avoid top-level options

    let matches = command.get_matches();

    let multicall = matches.subcommand().unwrap();
    let argv_1 = multicall.1.subcommand();
    let subcommand = argv_1.unwrap_or(multicall);

    let component = Component::from_str(subcommand.0)?;
    let args = subcommand
        .1
        .get_many::<String>(ARGS_ID)
        .into_iter()
        .flatten()
        .cloned()
        .collect();

    component.start(args);

    Ok(())
}

fn compute(args: Vec<String>) {
    let opts = risingwave_compute::ComputeNodeOpts::parse_from(args);
    risingwave_rt::init_risingwave_logger(
        risingwave_rt::LoggerSettings::new().enable_tokio_console(false),
    );
    risingwave_rt::main_okk(risingwave_compute::start(opts));
}

fn meta(args: Vec<String>) {
    let opts = risingwave_meta::MetaNodeOpts::parse_from(args);
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new());
    risingwave_rt::main_okk(risingwave_meta::start(opts));
}

fn frontend(args: Vec<String>) {
    let opts = risingwave_frontend::FrontendOpts::parse_from(args);
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new());
    risingwave_rt::main_okk(risingwave_frontend::start(opts));
}

fn compactor(args: Vec<String>) {
    let opts = risingwave_compactor::CompactorOpts::parse_from(args);
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new());
    risingwave_rt::main_okk(risingwave_compactor::start(opts));
}

fn ctl(args: Vec<String>) {
    let opts = risingwave_ctl::CliOpts::parse_from(args);
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new());
    risingwave_rt::main_okk(risingwave_ctl::start(opts)).unwrap();
}

fn playground(_args: Vec<String>) {
    let settings = risingwave_rt::LoggerSettings::new()
        .enable_tokio_console(false)
        .with_target("risingwave_storage", Level::WARN);
    risingwave_rt::init_risingwave_logger(settings);
    risingwave_rt::main_okk(risingwave_cmd_all::playground()).unwrap()
}
