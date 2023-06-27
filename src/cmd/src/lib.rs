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
use risingwave_meta::MetaNodeOpts;

/// Define the `main` function for a component.
#[macro_export]
macro_rules! main {
    ($component:ident) => {
        #[cfg(enable_task_local_alloc)]
        risingwave_common::enable_task_local_jemalloc_on_unix!();

        #[cfg(not(enable_task_local_alloc))]
        risingwave_common::enable_jemalloc_on_unix!();

        #[cfg_attr(coverage, no_coverage)]
        fn main() {
            let opts = clap::Parser::parse();
            let registry = prometheus::Registry::new();
            $crate::$component(opts, registry);
        }
    };
}

// Entry point functions.

pub fn compute(opts: ComputeNodeOpts, registry: prometheus::Registry) {
    risingwave_rt::init_risingwave_logger(
        risingwave_rt::LoggerSettings::new().enable_tokio_console(false),
        registry.clone(),
    );
    risingwave_rt::main_okk(risingwave_compute::start(opts, registry));
}

pub fn meta(opts: MetaNodeOpts, registry: prometheus::Registry) {
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new(), registry);
    risingwave_rt::main_okk(risingwave_meta::start(opts));
}

pub fn frontend(opts: FrontendOpts, registry: prometheus::Registry) {
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new(), registry);
    risingwave_rt::main_okk(risingwave_frontend::start(opts));
}

pub fn compactor(opts: CompactorOpts, registry: prometheus::Registry) {
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new(), registry);
    risingwave_rt::main_okk(risingwave_compactor::start(opts));
}

pub fn ctl(opts: CtlOpts, registry: prometheus::Registry) {
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new(), registry);

    // Note: Use a simple current thread runtime for ctl.
    // When there's a heavy workload, multiple thread runtime seems to respond slowly. May need
    // further investigation.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(risingwave_ctl::start(opts))
        .unwrap();
}
