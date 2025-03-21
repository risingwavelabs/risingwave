// Copyright 2025 RisingWave Labs
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

#![feature(assert_matches)]
#![cfg_attr(coverage, feature(coverage_attribute))]

use std::env;
use std::ffi::OsString;
use std::str::FromStr;

use clap::error::Result as ClapResult;
use clap::{ArgMatches, Args, Command, CommandFactory, FromArgMatches, command};
use risingwave_cmd::{compactor, compute, ctl, frontend, meta};
use risingwave_cmd_all::{SingleNodeOpts, StandaloneOpts};
use risingwave_common::git_sha;
use risingwave_common::telemetry::{TELEMETRY_CLUSTER_TYPE, TELEMETRY_CLUSTER_TYPE_SINGLE_NODE};
use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_ctl::CliOpts as CtlOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter, EnumString, IntoStaticStr};
use tracing::Level;

risingwave_common::enable_jemalloc!();

const BINARY_NAME: &str = "risingwave";
const VERSION: &str = {
    const GIT_SHA: &str = {
        /// `VERGEN_GIT_SHA` is provided by the build script. It will trigger rebuild
        /// for each commit, so we only use it for the final binary (`risingwave -V`).
        const VERGEN_GIT_SHA: &str = git_sha!("VERGEN_GIT_SHA");
        /// `GIT_SHA` is a normal environment variable provided by ourselves. It's
        /// [`risingwave_common::GIT_SHA`] and is used in logs/version queries.
        ///
        /// Usually it's only provided by docker/binary releases (including nightly builds).
        /// We check it is the same as `VERGEN_GIT_SHA` when it's present.
        const GIT_SHA: &str = risingwave_common::GIT_SHA;

        match (
            const_str::equal!(VERGEN_GIT_SHA, risingwave_common::UNKNOWN_GIT_SHA),
            const_str::equal!(GIT_SHA, risingwave_common::UNKNOWN_GIT_SHA),
        ) {
            (true, true) => {
                // Both `VERGEN_GIT_SHA` and `GIT_SHA` are not available.
                risingwave_common::UNKNOWN_GIT_SHA
            }
            (true, false) => {
                // `VERGEN_GIT_SHA` is not available (no `git` installed or not in a git repo).
                // Use `GIT_SHA` instead.
                GIT_SHA
            }
            (false, true) => {
                // `GIT_SHA` env var is not set.
                VERGEN_GIT_SHA
            }
            (false, false) => {
                // Both `VERGEN_GIT_SHA` and `GIT_SHA` are set.
                // We validate they are the same.
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
                VERGEN_GIT_SHA
            }
        }
    };
    const_str::concat!(clap::crate_version!(), " (", GIT_SHA, ")")
};

/// Component to launch.
#[derive(Debug, Clone, Copy, EnumIter, EnumString, Display, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
enum Component {
    Compute,
    Meta,
    Frontend,
    Compactor,
    Ctl,
    Playground,
    /// Used by cloud to bundle different components into a single node.
    /// It exposes the low level configuration options of each node.
    Standalone,
    /// Used by users to run a single node.
    /// The low level configuration options are hidden.
    /// We only expose high-level configuration options,
    /// which map across multiple nodes.
    SingleNode,
}

impl Component {
    /// Start the component from the given `args` without `argv[0]`.
    fn start(self, matches: &ArgMatches) -> ! {
        eprintln!("launching `{}`", self);

        fn parse_opts<T: FromArgMatches>(matches: &ArgMatches) -> T {
            T::from_arg_matches(matches).map_err(|e| e.exit()).unwrap()
        }
        match self {
            Self::Compute => compute(parse_opts(matches)),
            Self::Meta => meta(parse_opts(matches)),
            Self::Frontend => frontend(parse_opts(matches)),
            Self::Compactor => compactor(parse_opts(matches)),
            Self::Ctl => ctl(parse_opts(matches)),
            Self::Playground => playground(),
            Self::Standalone => standalone(parse_opts(matches)),
            Self::SingleNode => single_node(parse_opts(matches)),
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
            Component::Standalone => vec![],
            Component::SingleNode => vec!["single-node", "single"],
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
            Component::Playground => cmd
                .about("Shortcut for `single-node --in-memory`, should not be used in production"),
            Component::Standalone => StandaloneOpts::augment_args(cmd),
            Component::SingleNode => SingleNodeOpts::augment_args(cmd),
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

/// Parse the given arguments and return the component and its matches.
fn parse_args<I, T>(args: I) -> ClapResult<(Component, ArgMatches)>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
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
                .subcommands(Component::commands())
                // Make single node the "default subcommand"
                .args_conflicts_with_subcommands(true)
                .args(SingleNodeOpts::command().get_arguments()),
        );

    let matches = command.try_get_matches_from(args)?;
    let multicall = matches.subcommand().unwrap();

    let (component_name, matches) = if multicall.0 == BINARY_NAME {
        // This is not a multicall. Match argv[1] as a component.
        (multicall.1)
            .subcommand()
            // If there's no subcommand, it must be single node ("default subcommand").
            .unwrap_or_else(|| (Component::SingleNode.into(), multicall.1))
    } else {
        multicall
    };

    let component = Component::from_str(component_name).unwrap(); // always succeeds
    let matches = matches.clone();

    Ok((component, matches))
}

#[cfg_attr(coverage, coverage(off))]
fn main() {
    let (component, matches) = parse_args(std::env::args_os())
        .map_err(|e| e.exit())
        .unwrap();

    component.start(&matches);
}

fn standalone(opts: StandaloneOpts) -> ! {
    let opts = risingwave_cmd_all::parse_standalone_opt_args(&opts);
    let settings = risingwave_rt::LoggerSettings::from_opts(&opts)
        .with_target("risingwave_storage", Level::WARN)
        .with_thread_name(true);
    risingwave_rt::init_risingwave_logger(settings);
    risingwave_rt::main_okk(|shutdown| risingwave_cmd_all::standalone(opts, shutdown));
}

/// For single node, the internals are just a config mapping from its
/// high level options to standalone mode node-level options.
/// We will start a standalone instance, with all nodes in the same process.
fn single_node(opts: SingleNodeOpts) -> ! {
    if env::var(TELEMETRY_CLUSTER_TYPE).is_err() {
        // safety: single-threaded now.
        unsafe { env::set_var(TELEMETRY_CLUSTER_TYPE, TELEMETRY_CLUSTER_TYPE_SINGLE_NODE) };
    }
    let opts = risingwave_cmd_all::map_single_node_opts_to_standalone_opts(opts);
    let settings = risingwave_rt::LoggerSettings::from_opts(&opts)
        .with_target("risingwave_storage", Level::WARN)
        .with_thread_name(true);
    risingwave_rt::init_risingwave_logger(settings);
    risingwave_rt::main_okk(|shutdown| risingwave_cmd_all::standalone(opts, shutdown));
}

fn playground() -> ! {
    single_node(SingleNodeOpts::new_for_playground());
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use clap::error::ErrorKind;

    use super::{Component, parse_args};

    #[test]
    fn test_basic() {
        let (c, _) =
            parse_args(["./risingwave", "meta", "--advertise-addr", "1.2.3.4:5678"]).unwrap();
        assert_matches!(c, Component::Meta);
    }

    #[test]
    fn test_multicall() {
        let (c, _) = parse_args(["./meta-node", "--advertise-addr", "1.2.3.4:5678"]).unwrap();
        assert_matches!(c, Component::Meta);
    }

    #[test]
    fn test_missing_sub_subcommand() {
        let e = parse_args(["./risingwave", "ctl"]).unwrap_err();
        assert_matches!(
            e.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn test_sub_expected_subcommand_but_got_unknown_arg() {
        let e = parse_args(["./risingwave", "ctl", "--foo"]).unwrap_err();
        assert_matches!(e.kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn test_issue_16065() {
        let e = parse_args([
            "./risingwave",
            "ctl",
            "meta",
            "unregister-worker",
            "my-bad-arg",
        ])
        .unwrap_err();
        assert_matches!(e.kind(), ErrorKind::UnknownArgument);
        assert!(e.to_string().contains("my-bad-arg"), "{e}")
    }

    #[test]
    fn test_default_subcommand_single_node() {
        let (c, _) = parse_args(["./risingwave"]).unwrap();
        assert_matches!(c, Component::SingleNode);
    }

    #[test]
    fn test_default_subcommand_single_node_with_args() {
        let (c, _) = parse_args(["./risingwave", "--in-memory"]).unwrap();
        assert_matches!(c, Component::SingleNode);
    }

    #[test]
    fn test_default_subcommand_single_node_with_unknown_args() {
        let e = parse_args(["./risingwave", "--foo"]).unwrap_err();
        assert_matches!(e.kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn test_default_subcommand_single_node_with_other_explicit_subcommand() {
        let e = parse_args(["./risingwave", "--in-memory", "ctl"]).unwrap_err();
        assert_matches!(e.kind(), ErrorKind::ArgumentConflict);
        assert!(e.to_string().contains("cannot be used with"), "{e}");
    }
}
