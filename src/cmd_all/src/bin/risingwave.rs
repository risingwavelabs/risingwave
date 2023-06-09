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
use clap::{command, ArgMatches, Args, Command, FromArgMatches};
use risingwave_cmd::{compactor, compute, ctl, frontend, meta};
use risingwave_cmd_all::PlaygroundOpts;
use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_ctl::CliOpts as CtlOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_meta::MetaNodeOpts;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter, EnumString, IntoStaticStr};
use tracing::Level;

#[cfg(enable_task_local_alloc)]
risingwave_common::enable_task_local_jemalloc_on_unix!();

#[cfg(not(enable_task_local_alloc))]
risingwave_common::enable_jemalloc_on_unix!();

const BINARY_NAME: &str = "risingwave";
/// `VERGEN_GIT_SHA` is provided by the build script. It will trigger rebuild
/// for each commit, so we only use it for the final binary (`risingwave -V`).
const VERGEN_GIT_SHA: &str = env!("VERGEN_GIT_SHA");
const VERSION: &str = const_str::concat!(env!("CARGO_PKG_VERSION"), " (", VERGEN_GIT_SHA, ")");

/// Component to launch.
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
    fn start(self, matches: &ArgMatches) {
        eprintln!("launching `{}`", self);

        fn parse_opts<T: FromArgMatches>(matches: &ArgMatches) -> T {
            T::from_arg_matches(matches).map_err(|e| e.exit()).unwrap()
        }
        let registry = prometheus::Registry::new();
        match self {
            Self::Compute => compute(parse_opts(matches), registry),
            Self::Meta => meta(parse_opts(matches), registry),
            Self::Frontend => frontend(parse_opts(matches), registry),
            Self::Compactor => compactor(parse_opts(matches), registry),
            Self::Ctl => ctl(parse_opts(matches), registry),
            Self::Playground => playground(parse_opts(matches), registry),
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

    /// Append component-specific arguments to the given `cmd`.
    fn augment_args(self, cmd: Command) -> Command {
        match self {
            Component::Compute => ComputeNodeOpts::augment_args(cmd),
            Component::Meta => MetaNodeOpts::augment_args(cmd),
            Component::Frontend => FrontendOpts::augment_args(cmd),
            Component::Compactor => CompactorOpts::augment_args(cmd),
            Component::Ctl => CtlOpts::augment_args(cmd),
            Component::Playground => PlaygroundOpts::augment_args(cmd),
        }
    }

    /// `clap` commands for all components.
    fn commands() -> Vec<Command> {
        Self::iter()
            .map(|c| {
                let name: &'static str = c.into();
                let command = Command::new(name).visible_aliases(c.aliases());
                c.augment_args(command)
            })
            .collect()
    }
}

#[cfg_attr(coverage, no_coverage)]
fn main() -> Result<()> {
    let risingwave = || {
        command!(BINARY_NAME)
            .about("All-in-one executable for components of RisingWave")
            .version(VERSION)
            .propagate_version(true)
    };
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
        );

    let matches = command.get_matches();

    let multicall = matches.subcommand().unwrap();
    let argv_1 = multicall.1.subcommand();
    let (component_name, matches) = argv_1.unwrap_or(multicall);

    let component = Component::from_str(component_name)?;
    component.start(matches);

    Ok(())
}

fn playground(opts: PlaygroundOpts, registry: prometheus::Registry) {
    let settings = risingwave_rt::LoggerSettings::new()
        .enable_tokio_console(false)
        .with_target("risingwave_storage", Level::WARN);
    risingwave_rt::init_risingwave_logger(settings, registry);
    risingwave_rt::main_okk(risingwave_cmd_all::playground(opts)).unwrap();
}

const _: () = {
    /// `GIT_SHA` is a normal environment variable. It's [`risingwave_common::GIT_SHA`]
    /// and is used in logs/version queries.
    ///
    /// Usually it's only provided by docker/binary releases (including nightly builds).
    /// We check it is the same as `VERGEN_GIT_SHA` when it's present.
    const GIT_SHA: &str = match option_env!("GIT_SHA") {
        Some(sha) => sha,
        None => VERGEN_GIT_SHA,
    };
    const ERROR_MSG: &str = const_str::concat!(
        "environment variable GIT_SHA (",
        GIT_SHA,
        ") mismatches VERGEN_GIT_SHA (",
        VERGEN_GIT_SHA,
        "). Please set the correct value for GIT_SHA or unset it."
    );
    assert!(
        const_str::starts_with!(GIT_SHA, VERGEN_GIT_SHA),
        "{}",
        ERROR_MSG
    );
};
