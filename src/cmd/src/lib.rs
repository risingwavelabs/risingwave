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

use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_ctl::CliOpts as CtlOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;
use risingwave_rt::{LoggerSettings, init_risingwave_logger, main_okk};

/// Define the `main` function for a component.
#[macro_export]
macro_rules! main {
    ($component:ident) => {
        risingwave_common::enable_mimalloc!();

        fn main() {
            let opts = clap::Parser::parse();
            $crate::$component(opts);
        }
    };
}

risingwave_batch_executors::enable!();
risingwave_expr_impl::enable!();

// Entry point functions.

pub fn compute(opts: ComputeNodeOpts) -> ! {
    init_risingwave_logger(LoggerSettings::from_opts(&opts));
    main_okk(|shutdown| risingwave_compute::start(opts, shutdown));
}

pub fn meta(opts: MetaNodeOpts) -> ! {
    init_risingwave_logger(LoggerSettings::from_opts(&opts));
    main_okk(|shutdown| risingwave_meta_node::start(opts, shutdown));
}

pub fn frontend(opts: FrontendOpts) -> ! {
    init_risingwave_logger(LoggerSettings::from_opts(&opts));
    main_okk(|shutdown| risingwave_frontend::start(opts, shutdown));
}

pub fn compactor(opts: CompactorOpts) -> ! {
    init_risingwave_logger(LoggerSettings::from_opts(&opts));
    main_okk(|shutdown| risingwave_compactor::start(opts, shutdown));
}

pub fn ctl(opts: CtlOpts) -> ! {
    init_risingwave_logger(LoggerSettings::new("ctl").stderr(true));
    main_okk(|shutdown| risingwave_ctl::start(opts, shutdown));
}
