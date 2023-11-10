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

#![feature(lint_reasons)]
#![feature(let_chains)]
#![cfg_attr(coverage, feature(coverage_attribute))]

mod server;

use std::time::Duration;

use clap::Parser;
pub use error::{MetaError, MetaResult};
use redact::Secret;
use risingwave_common::config::OverrideConfig;
use risingwave_common::util::resource_util;
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_heap_profiling::HeapProfiler;
use risingwave_meta::*;
use risingwave_meta_service::*;
pub use rpc::{ElectionClient, ElectionMember, EtcdElectionClient};
use server::{rpc_serve, MetaStoreSqlBackend};

use crate::manager::MetaOpts;

#[derive(Debug, Clone, Parser, OverrideConfig)]
#[command(version, about = "The central metadata management service")]
pub struct MetaNodeOpts {
    #[clap(long, env = "RW_VPC_ID")]
    vpc_id: Option<String>,

    #[clap(long, env = "RW_VPC_SECURITY_GROUP_ID")]
    security_group_id: Option<String>,

    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:5690")]
    listen_addr: String,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// It will serve as a unique identifier in cluster
    /// membership and leader election. Must be specified for etcd backend.
    #[clap(long, env = "RW_ADVERTISE_ADDR")]
    advertise_addr: String,

    #[clap(long, env = "RW_DASHBOARD_HOST")]
    dashboard_host: Option<String>,

    #[clap(long, env = "RW_PROMETHEUS_HOST")]
    pub prometheus_host: Option<String>,

    #[clap(long, env = "RW_ETCD_ENDPOINTS", default_value_t = String::from(""))]
    etcd_endpoints: String,

    /// Enable authentication with etcd. By default disabled.
    #[clap(long, env = "RW_ETCD_AUTH")]
    etcd_auth: bool,

    /// Username of etcd, required when --etcd-auth is enabled.
    #[clap(long, env = "RW_ETCD_USERNAME", default_value = "")]
    etcd_username: String,

    /// Password of etcd, required when --etcd-auth is enabled.
    #[clap(long, env = "RW_ETCD_PASSWORD", default_value = "")]
    etcd_password: Secret<String>,

    /// Endpoint of the SQL service, make it non-option when SQL service is required.
    #[clap(long, env = "RW_SQL_ENDPOINT")]
    sql_endpoint: Option<String>,

    #[clap(long, env = "RW_DASHBOARD_UI_PATH")]
    dashboard_ui_path: Option<String>,

    /// For dashboard service to fetch cluster info.
    #[clap(long, env = "RW_PROMETHEUS_ENDPOINT")]
    prometheus_endpoint: Option<String>,

    /// Endpoint of the connector node, there will be a sidecar connector node
    /// colocated with Meta node in the cloud environment
    #[clap(long, env = "RW_CONNECTOR_RPC_ENDPOINT")]
    pub connector_rpc_endpoint: Option<String>,

    /// Default tag for the endpoint created when creating a privatelink connection.
    /// Will be appended to the tags specified in the `tags` field in with clause in `create
    /// connection`.
    #[clap(long, env = "RW_PRIVATELINK_ENDPOINT_DEFAULT_TAGS")]
    pub privatelink_endpoint_default_tags: Option<String>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    #[clap(long, env = "RW_BACKEND", value_enum)]
    #[override_opts(path = meta.backend)]
    backend: Option<MetaBackend>,

    /// The interval of periodic barrier.
    #[clap(long, env = "RW_BARRIER_INTERVAL_MS")]
    #[override_opts(path = system.barrier_interval_ms)]
    barrier_interval_ms: Option<u32>,

    /// Target size of the Sstable.
    #[clap(long, env = "RW_SSTABLE_SIZE_MB")]
    #[override_opts(path = system.sstable_size_mb)]
    sstable_size_mb: Option<u32>,

    /// Size of each block in bytes in SST.
    #[clap(long, env = "RW_BLOCK_SIZE_KB")]
    #[override_opts(path = system.block_size_kb)]
    block_size_kb: Option<u32>,

    /// False positive probability of bloom filter.
    #[clap(long, env = "RW_BLOOM_FALSE_POSITIVE")]
    #[override_opts(path = system.bloom_false_positive)]
    bloom_false_positive: Option<f64>,

    /// State store url
    #[clap(long, env = "RW_STATE_STORE")]
    #[override_opts(path = system.state_store)]
    state_store: Option<String>,

    /// Remote directory for storing data and metadata objects.
    #[clap(long, env = "RW_DATA_DIRECTORY")]
    #[override_opts(path = system.data_directory)]
    data_directory: Option<String>,

    /// Whether config object storage bucket lifecycle to purge stale data.
    #[clap(long, env = "RW_DO_NOT_CONFIG_BUCKET_LIFECYCLE")]
    #[override_opts(path = meta.do_not_config_object_storage_lifecycle)]
    do_not_config_object_storage_lifecycle: Option<bool>,

    /// Remote storage url for storing snapshots.
    #[clap(long, env = "RW_BACKUP_STORAGE_URL")]
    #[override_opts(path = system.backup_storage_url)]
    backup_storage_url: Option<String>,

    /// Remote directory for storing snapshots.
    #[clap(long, env = "RW_BACKUP_STORAGE_DIRECTORY")]
    #[override_opts(path = system.backup_storage_directory)]
    backup_storage_directory: Option<String>,

    #[clap(long, env = "RW_OBJECT_STORE_STREAMING_READ_TIMEOUT_MS", value_enum)]
    #[override_opts(path = storage.object_store_streaming_read_timeout_ms)]
    pub object_store_streaming_read_timeout_ms: Option<u64>,
    #[clap(long, env = "RW_OBJECT_STORE_STREAMING_UPLOAD_TIMEOUT_MS", value_enum)]
    #[override_opts(path = storage.object_store_streaming_upload_timeout_ms)]
    pub object_store_streaming_upload_timeout_ms: Option<u64>,
    #[clap(long, env = "RW_OBJECT_STORE_UPLOAD_TIMEOUT_MS", value_enum)]
    #[override_opts(path = storage.object_store_upload_timeout_ms)]
    pub object_store_upload_timeout_ms: Option<u64>,
    #[clap(long, env = "RW_OBJECT_STORE_READ_TIMEOUT_MS", value_enum)]
    #[override_opts(path = storage.object_store_read_timeout_ms)]
    pub object_store_read_timeout_ms: Option<u64>,

    /// Enable heap profile dump when memory usage is high.
    #[clap(long, env = "RW_HEAP_PROFILING_DIR")]
    #[override_opts(path = server.heap_profiling.dir)]
    pub heap_profiling_dir: Option<String>,
}

use std::future::Future;
use std::pin::Pin;

use risingwave_common::config::{load_config, MetaBackend, RwConfig};
use tracing::info;

/// Start meta node
pub fn start(opts: MetaNodeOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        info!("Starting meta node");
        info!("> options: {:?}", opts);
        let config = load_config(&opts.config_path, &opts);
        info!("> config: {:?}", config);
        info!("> version: {} ({})", RW_VERSION, GIT_SHA);
        let listen_addr = opts.listen_addr.parse().unwrap();
        let dashboard_addr = opts.dashboard_host.map(|x| x.parse().unwrap());
        let prometheus_addr = opts.prometheus_host.map(|x| x.parse().unwrap());
        let backend = match config.meta.backend {
            MetaBackend::Etcd => MetaStoreBackend::Etcd {
                endpoints: opts
                    .etcd_endpoints
                    .split(',')
                    .map(|x| x.to_string())
                    .collect(),
                credentials: match opts.etcd_auth {
                    true => Some((
                        opts.etcd_username,
                        opts.etcd_password.expose_secret().to_string(),
                    )),
                    false => None,
                },
            },
            MetaBackend::Mem => MetaStoreBackend::Mem,
        };
        let sql_backend = opts
            .sql_endpoint
            .map(|endpoint| MetaStoreSqlBackend { endpoint });

        validate_config(&config);

        let total_memory_bytes = resource_util::memory::system_memory_available_bytes();
        let heap_profiler =
            HeapProfiler::new(total_memory_bytes, config.server.heap_profiling.clone());
        // Run a background heap profiler
        heap_profiler.start();

        let max_heartbeat_interval =
            Duration::from_secs(config.meta.max_heartbeat_interval_secs as u64);
        let max_idle_ms = config.meta.dangerous_max_idle_secs.unwrap_or(0) * 1000;
        let in_flight_barrier_nums = config.streaming.in_flight_barrier_nums;
        let privatelink_endpoint_default_tags =
            opts.privatelink_endpoint_default_tags.map(|tags| {
                tags.split(',')
                    .map(|s| {
                        let key_val = s.split_once('=').unwrap();
                        (key_val.0.to_string(), key_val.1.to_string())
                    })
                    .collect()
            });

        let add_info = AddressInfo {
            advertise_addr: opts.advertise_addr,
            listen_addr,
            prometheus_addr,
            dashboard_addr,
            ui_path: opts.dashboard_ui_path,
        };

        let (mut join_handle, leader_lost_handle, shutdown_send) = rpc_serve(
            add_info,
            backend,
            sql_backend,
            max_heartbeat_interval,
            config.meta.meta_leader_lease_secs,
            MetaOpts {
                enable_recovery: !config.meta.disable_recovery,
                enable_scale_in_when_recovery: config.meta.enable_scale_in_when_recovery,
                in_flight_barrier_nums,
                max_idle_ms,
                compaction_deterministic_test: config.meta.enable_compaction_deterministic,
                default_parallelism: config.meta.default_parallelism,
                vacuum_interval_sec: config.meta.vacuum_interval_sec,
                vacuum_spin_interval_ms: config.meta.vacuum_spin_interval_ms,
                hummock_version_checkpoint_interval_sec: config
                    .meta
                    .hummock_version_checkpoint_interval_sec,
                min_delta_log_num_for_hummock_version_checkpoint: config
                    .meta
                    .min_delta_log_num_for_hummock_version_checkpoint,
                min_sst_retention_time_sec: config.meta.min_sst_retention_time_sec,
                full_gc_interval_sec: config.meta.full_gc_interval_sec,
                collect_gc_watermark_spin_interval_sec: config
                    .meta
                    .collect_gc_watermark_spin_interval_sec,
                enable_committed_sst_sanity_check: config.meta.enable_committed_sst_sanity_check,
                periodic_compaction_interval_sec: config.meta.periodic_compaction_interval_sec,
                node_num_monitor_interval_sec: config.meta.node_num_monitor_interval_sec,
                prometheus_endpoint: opts.prometheus_endpoint,
                vpc_id: opts.vpc_id,
                security_group_id: opts.security_group_id,
                connector_rpc_endpoint: opts.connector_rpc_endpoint,
                privatelink_endpoint_default_tags,
                periodic_space_reclaim_compaction_interval_sec: config
                    .meta
                    .periodic_space_reclaim_compaction_interval_sec,
                telemetry_enabled: config.server.telemetry_enabled,
                periodic_ttl_reclaim_compaction_interval_sec: config
                    .meta
                    .periodic_ttl_reclaim_compaction_interval_sec,
                periodic_tombstone_reclaim_compaction_interval_sec: config
                    .meta
                    .periodic_tombstone_reclaim_compaction_interval_sec,
                periodic_split_compact_group_interval_sec: config
                    .meta
                    .periodic_split_compact_group_interval_sec,
                split_group_size_limit: config.meta.split_group_size_limit,
                min_table_split_size: config.meta.move_table_size_limit,
                table_write_throughput_threshold: config.meta.table_write_throughput_threshold,
                min_table_split_write_throughput: config.meta.min_table_split_write_throughput,
                partition_vnode_count: config.meta.partition_vnode_count,
                do_not_config_object_storage_lifecycle: config
                    .meta
                    .do_not_config_object_storage_lifecycle,
                compaction_task_max_heartbeat_interval_secs: config
                    .meta
                    .compaction_task_max_heartbeat_interval_secs,
                compaction_config: Some(config.meta.compaction_config),
            },
            config.system.into_init_system_params(),
        )
        .await
        .unwrap();

        tracing::info!("Meta server listening at {}", listen_addr);

        match leader_lost_handle {
            None => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("receive ctrl+c");
                        shutdown_send.send(()).unwrap();
                        join_handle.await.unwrap()
                    }
                    res = &mut join_handle => res.unwrap(),
                };
            }
            Some(mut handle) => {
                tokio::select! {
                    _ = &mut handle => {
                        tracing::info!("receive leader lost signal");
                        // When we lose leadership, we will exit as soon as possible.
                    }
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("receive ctrl+c");
                        shutdown_send.send(()).unwrap();
                        join_handle.await.unwrap();
                        handle.abort();
                    }
                    res = &mut join_handle => {
                        res.unwrap();
                        handle.abort();
                    },
                };
            }
        };
    })
}

fn validate_config(config: &RwConfig) {
    if config.meta.meta_leader_lease_secs <= 2 {
        let error_msg = "meta leader lease secs should be larger than 2";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
}
