// Copyright 2024 RisingWave Labs
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

use std::str::FromStr;

use anyhow::Result;
use clap::Parser;
use risingwave_common::single_process_config::{
    make_single_node_sql_endpoint, make_single_node_state_store_url,
};
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;
use shell_words::split;
use tokio::signal;

use crate::common::osstrs;
use crate::ParsedStandaloneOpts;

#[derive(Eq, PartialOrd, PartialEq, Debug, Clone, Parser)]
#[command(
    version,
    about = "[default] The Single Node mode. Start all services in one process, with process-level options. This will be executed if no subcommand is specified"
)]
/// Here we define our own defaults for the single node mode.
pub struct SingleNodeOpts {
    /// The prometheus address used by the single-node cluster.
    /// If you have a prometheus instance,
    /// it will poll the metrics from this address.
    #[clap(long, env = "RW_SINGLE_NODE_PROMETHEUS_LISTENER_ADDR")]
    prometheus_listener_addr: Option<String>,

    /// The path to the cluster configuration file.
    #[clap(long, env = "RW_SINGLE_NODE_CONFIG_PATH")]
    config_path: Option<String>,

    /// The store directory used by meta store and object store.
    #[clap(long, env = "RW_SINGLE_NODE_STORE_DIRECTORY")]
    store_directory: Option<String>,

    /// The address of the meta node.
    #[clap(long, env = "RW_SINGLE_NODE_META_ADDR")]
    meta_addr: Option<String>,

    /// The address of the compute node
    #[clap(long, env = "RW_SINGLE_NODE_COMPUTE_ADDR")]
    compute_addr: Option<String>,

    /// The address of the frontend node
    #[clap(long, env = "RW_SINGLE_NODE_FRONTEND_ADDR")]
    frontend_addr: Option<String>,

    /// The address of the compactor node
    #[clap(long, env = "RW_SINGLE_NODE_COMPACTOR_ADDR")]
    compactor_addr: Option<String>,
}

pub fn map_single_node_opts_to_standalone_opts(opts: &SingleNodeOpts) -> ParsedStandaloneOpts {
    let mut meta_opts = MetaNodeOpts::new_for_single_node();
    let mut compute_opts = ComputeNodeOpts::new_for_single_node();
    let mut frontend_opts = FrontendOpts::new_for_single_node();
    let mut compactor_opts = CompactorOpts::new_for_single_node();
    if let Some(prometheus_listener_addr) = &opts.prometheus_listener_addr {
        meta_opts.prometheus_listener_addr = Some(prometheus_listener_addr.clone());
        compute_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
        frontend_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
        compactor_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
    }
    if let Some(config_path) = &opts.config_path {
        meta_opts.config_path = config_path.clone();
        compute_opts.config_path = config_path.clone();
        frontend_opts.config_path = config_path.clone();
        compactor_opts.config_path = config_path.clone();
    }
    // TODO(kwannoel): Also update state store URL
    if let Some(store_directory) = &opts.store_directory {
        let state_store_url = make_single_node_state_store_url(store_directory);
        let meta_store_endpoint = make_single_node_sql_endpoint(store_directory);
        meta_opts.state_store = Some(state_store_url);
        meta_opts.sql_endpoint = Some(meta_store_endpoint);
    }
    if let Some(meta_addr) = &opts.meta_addr {
        meta_opts.listen_addr = meta_addr.clone();
        meta_opts.advertise_addr = meta_addr.clone();

        compute_opts.meta_address = meta_addr.parse().unwrap();
        frontend_opts.meta_addr = meta_addr.parse().unwrap();
        compactor_opts.meta_address = meta_addr.parse().unwrap();
    }
    if let Some(compute_addr) = &opts.compute_addr {
        compute_opts.listen_addr = compute_addr.clone();
    }
    if let Some(frontend_addr) = &opts.frontend_addr {
        frontend_opts.listen_addr = frontend_addr.clone();
    }
    if let Some(compactor_addr) = &opts.compactor_addr {
        compactor_opts.listen_addr = compactor_addr.clone();
    }
    ParsedStandaloneOpts {
        meta_opts: Some(meta_opts),
        compute_opts: Some(compute_opts),
        frontend_opts: Some(frontend_opts),
        compactor_opts: Some(compactor_opts),
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use expect_test::{expect, Expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_parse_opt_args() {}
}
