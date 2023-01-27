// Copyright 2023 Singularity Data
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

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(option_result_contains)]
#![feature(type_alias_impl_trait)]
#![feature(drain_filter)]
#![feature(custom_test_frameworks)]
#![feature(lint_reasons)]
#![feature(map_try_insert)]
#![feature(hash_drain_filter)]
#![feature(is_some_and)]
#![feature(btree_drain_filter)]
#![feature(result_option_inspect)]
#![feature(once_cell)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![feature(assert_matches)]
#![feature(try_blocks)]
#![cfg_attr(coverage, feature(no_coverage))]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]

pub mod backup_restore;
mod barrier;
#[cfg(not(madsim))] // no need in simulation test
mod dashboard;
mod error;
pub mod hummock;
pub mod manager;
mod model;
mod rpc;
pub mod storage;
mod stream;

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub use error::{MetaError, MetaResult};
use risingwave_common::config::{load_config, MetaBackend, MetaConfig};

use crate::manager::MetaOpts;
use crate::rpc::server::{rpc_serve, AddressInfo, MetaStoreBackend};

/// Start meta node
pub fn start(opts: MetaConfig) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        let config = load_config(&opts.config_path.clone(), Some(opts));
        tracing::info!("Starting meta node with config {:?}", config);
        let listen_addr = config.meta.listen_addr.parse().unwrap();
        let dashboard_addr = config.meta.dashboard_host.map(|x| x.parse().unwrap());
        let prometheus_addr = config.meta.prometheus_host.map(|x| x.parse().unwrap());
        let (meta_endpoint, backend) = match config.meta.backend {
            MetaBackend::Etcd => (
                config
                    .meta
                    .meta_endpoint
                    .expect("meta_endpoint must be specified when using etcd"),
                MetaStoreBackend::Etcd {
                    endpoints: config
                        .meta
                        .etcd_endpoints
                        .split(',')
                        .map(|x| x.to_string())
                        .collect(),
                    credentials: match config.meta.etcd_auth {
                        true => Some((config.meta.etcd_username, config.meta.etcd_password)),
                        false => None,
                    },
                },
            ),
            MetaBackend::Mem => (
                config
                    .meta
                    .meta_endpoint
                    .unwrap_or_else(|| config.meta.listen_addr.clone()),
                MetaStoreBackend::Mem,
            ),
        };

        let max_heartbeat_interval =
            Duration::from_secs(config.meta.max_heartbeat_interval_secs as u64);
        let barrier_interval = Duration::from_millis(config.streaming.barrier_interval_ms as u64);
        let max_idle_ms = config.meta.dangerous_max_idle_secs.unwrap_or(0) * 1000;
        let in_flight_barrier_nums = config.streaming.in_flight_barrier_nums;
        let checkpoint_frequency = config.streaming.checkpoint_frequency;

        tracing::info!("Meta server listening at {}", listen_addr);
        let add_info = AddressInfo {
            meta_endpoint,
            listen_addr,
            prometheus_addr,
            dashboard_addr,
            ui_path: config.meta.dashboard_ui_path,
        };
        let (join_handle, leader_lost_handle, _shutdown_send) = rpc_serve(
            add_info,
            backend,
            max_heartbeat_interval,
            config.meta.meta_leader_lease_secs,
            MetaOpts {
                enable_recovery: !config.meta.disable_recovery,
                barrier_interval,
                in_flight_barrier_nums,
                max_idle_ms,
                checkpoint_frequency,
                compaction_deterministic_test: config.meta.enable_compaction_deterministic,
                vacuum_interval_sec: config.meta.vacuum_interval_sec,
                min_sst_retention_time_sec: config.meta.min_sst_retention_time_sec,
                collect_gc_watermark_spin_interval_sec: config
                    .meta
                    .collect_gc_watermark_spin_interval_sec,
                enable_committed_sst_sanity_check: config.meta.enable_committed_sst_sanity_check,
                periodic_compaction_interval_sec: config.meta.periodic_compaction_interval_sec,
                node_num_monitor_interval_sec: config.meta.node_num_monitor_interval_sec,
                prometheus_endpoint: config.meta.prometheus_endpoint,
                connector_rpc_endpoint: config.meta.connector_rpc_endpoint,
                backup_storage_url: config.backup.storage_url,
                backup_storage_directory: config.backup.storage_directory,
            },
        )
        .await
        .unwrap();

        if let Some(leader_lost_handle) = leader_lost_handle {
            tokio::select! {
                _ = join_handle => {},
                _ = leader_lost_handle => {},
            }
        } else {
            join_handle.await.unwrap();
        }
    })
}
