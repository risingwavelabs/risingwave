// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg_attr(coverage, feature(no_coverage))]

use global_stats_alloc::INSTRUMENTED_JEMALLOC;
use stats_alloc::StatsAlloc;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: &StatsAlloc<Jemalloc> = &INSTRUMENTED_JEMALLOC;

use std::collections::HashMap;
use std::env;

use anyhow::{bail, Result};
use clap::StructOpt;
use risingwave_cmd_all::playground;

type RwFns = HashMap<&'static str, Box<dyn Fn(Vec<String>) -> Result<()>>>;

#[cfg_attr(coverage, no_coverage)]
fn main() -> Result<()> {
    let mut fns: RwFns = HashMap::new();

    // compute node configuration
    for fn_name in ["compute", "compute-node", "compute_node"] {
        fns.insert(
            fn_name,
            Box::new(|args: Vec<String>| {
                eprintln!("launching compute node");

                let opts = risingwave_compute::ComputeNodeOpts::parse_from(args);

                risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new(
                    opts.enable_jaeger_tracing,
                    false,
                ));

                risingwave_rt::main_okk(risingwave_compute::start(opts));

                Ok(())
            }),
        );
    }

    // meta node configuration
    for fn_name in ["meta", "meta-node", "meta_node"] {
        fns.insert(
            fn_name,
            Box::new(move |args: Vec<String>| {
                eprintln!("launching meta node");

                let opts = risingwave_meta::MetaNodeOpts::parse_from(args);

                risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new_default());

                risingwave_rt::main_okk(risingwave_meta::start(opts));

                Ok(())
            }),
        );
    }

    // frontend node configuration
    for fn_name in ["frontend", "frontend-node", "frontend_node"] {
        fns.insert(
            fn_name,
            Box::new(move |args: Vec<String>| {
                eprintln!("launching frontend node");

                let opts = risingwave_frontend::FrontendOpts::parse_from(args);

                risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new_default());

                risingwave_rt::main_okk(risingwave_frontend::start(opts));

                Ok(())
            }),
        );
    }

    // frontend node configuration
    for fn_name in ["compactor", "compactor-node", "compactor_node"] {
        fns.insert(
            fn_name,
            Box::new(move |args: Vec<String>| {
                eprintln!("launching compactor node");

                let opts = risingwave_compactor::CompactorOpts::parse_from(args);

                risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new_default());

                risingwave_rt::main_okk(risingwave_compactor::start(opts));

                Ok(())
            }),
        );
    }

    // risectl
    fns.insert(
        "risectl",
        Box::new(move |args: Vec<String>| {
            eprintln!("launching risectl");

            let opts = risingwave_ctl::CliOpts::parse_from(args);
            risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new_default());

            risingwave_rt::main_okk(risingwave_ctl::start(opts))
        }),
    );

    // playground
    for fn_name in ["play", "playground"] {
        fns.insert(
            fn_name,
            Box::new(move |_: Vec<String>| {
                risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new_default());

                // Enable tokio console for `./risedev p` by replacing the above statement to:
                // risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new(false,
                // true));

                risingwave_rt::main_okk(playground())
            }),
        );
    }

    /// Get the launch target of this all-in-one binary
    fn get_target(cmds: Vec<&str>) -> (String, Vec<String>) {
        if let Some(cmd) = env::args().nth(1) && cmds.contains(&cmd.as_str()) {
            // ./risingwave meta <args>
            return (cmd, env::args().skip(1).collect());
        }

        if let Ok(target) = env::var("RW_NODE") {
            // RW_NODE=meta ./risingwave <args>
            (target, env::args().collect())
        } else {
            // ./meta-node <args>
            let x = env::args().next().expect("cannot find argv[0]");
            let x = x.rsplit('/').next().expect("cannot find binary name");
            let target = x.to_string();
            (target, env::args().collect())
        }
    }

    let (target, args) = get_target(fns.keys().copied().collect());

    match fns.remove(target.as_str()) {
        Some(func) => {
            func(args)?;
        }
        None => {
            bail!("unknown target: {}\nPlease either:\n* set `RW_NODE` env variable (`RW_NODE=<component>`)\n* create a symbol link to `risingwave` binary (ln -s risingwave <component>)\n* start with subcommand `risingwave <component>``\nwith one of the following: {:?}", target, fns.keys().collect::<Vec<_>>());
        }
    }

    Ok(())
}
