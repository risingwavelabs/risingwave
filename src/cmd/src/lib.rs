#![feature(result_option_inspect)]
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

use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_ctl::CliOpts as CtlOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;
use risingwave_rt::{init_risingwave_logger, main_okk, LoggerSettings};

/// Define the `main` function for a component.
#[macro_export]
macro_rules! main {
    ($component:ident) => {
        #[cfg(enable_task_local_alloc)]
        risingwave_common::enable_task_local_jemalloc!();

        #[cfg(not(enable_task_local_alloc))]
        risingwave_common::enable_jemalloc!();

        #[cfg_attr(coverage, coverage(off))]
        fn main() {
            let opts = clap::Parser::parse();
            $crate::$component(opts);
        }
    };
}

risingwave_expr_impl::enable!();

// Entry point functions.

pub fn compute(opts: ComputeNodeOpts) {
    init_risingwave_logger(LoggerSettings::new("compute"));
    main_okk(risingwave_compute::start(opts));
}

pub fn meta(opts: MetaNodeOpts) {
    init_risingwave_logger(LoggerSettings::new("meta"));
    main_okk(risingwave_meta_node::start(opts));
}

pub fn frontend(opts: FrontendOpts) {
    init_risingwave_logger(LoggerSettings::new("frontend"));
    main_okk(risingwave_frontend::start(opts));
}

pub fn compactor(opts: CompactorOpts) {
    init_risingwave_logger(LoggerSettings::new("compactor"));
    main_okk(risingwave_compactor::start(opts));
}

pub fn ctl(opts: CtlOpts) {
    init_risingwave_logger(LoggerSettings::new("ctl").stderr(true));

    // Note: Use a simple current thread runtime for ctl.
    // When there's a heavy workload, multiple thread runtime seems to respond slowly. May need
    // further investigation.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(risingwave_ctl::start(opts))
        .inspect_err(|e| {
            eprintln!("{:#?}", e);
        })
        .unwrap();
}
