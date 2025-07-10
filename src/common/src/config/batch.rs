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

use risingwave_common_proc_macro::ConfigDoc;
use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;

use super::types::{RpcClientConfig, Unrecognized};

serde_with::with_prefix!(batch_prefix "batch_");

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct BatchConfig {
    /// The thread number of the batch task runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub worker_threads_num: Option<usize>,

    #[serde(default, with = "batch_prefix")]
    #[config_doc(omitted)]
    pub developer: BatchDeveloperConfig,

    /// This is the max number of queries per sql session.
    #[serde(default)]
    pub distributed_query_limit: Option<u64>,

    /// This is the max number of batch queries per frontend node.
    #[serde(default)]
    pub max_batch_queries_per_frontend_node: Option<u64>,

    #[serde(default = "default::batch::enable_barrier_read")]
    pub enable_barrier_read: bool,

    /// Timeout for a batch query in seconds.
    #[serde(default = "default::batch::statement_timeout_in_sec")]
    pub statement_timeout_in_sec: u32,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    #[serde(default)]
    /// frontend compute runtime worker threads
    pub frontend_compute_runtime_worker_threads: Option<usize>,

    /// This is the secs used to mask a worker unavailable temporarily.
    #[serde(default = "default::batch::mask_worker_temporary_secs")]
    pub mask_worker_temporary_secs: usize,

    /// Keywords on which SQL option redaction is based in the query log.
    /// A SQL option with a name containing any of these keywords will be redacted.
    #[serde(default = "default::batch::redact_sql_option_keywords")]
    pub redact_sql_option_keywords: Vec<String>,

    /// Enable the spill out to disk feature for batch queries.
    #[serde(default = "default::batch::enable_spill")]
    pub enable_spill: bool,
}

/// The subsections `[batch.developer]`.
///
/// It is put at [`BatchConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct BatchDeveloperConfig {
    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    #[serde(default = "default::developer::connector_message_buffer_size")]
    pub connector_message_buffer_size: usize,

    /// The size of the channel used for output to exchange/shuffle.
    #[serde(default = "default::developer::batch_output_channel_size")]
    pub output_channel_size: usize,

    #[serde(default = "default::developer::batch_receiver_channel_size")]
    pub receiver_channel_size: usize,

    #[serde(default = "default::developer::batch_root_stage_channel_size")]
    pub root_stage_channel_size: usize,

    /// The size of a chunk produced by `RowSeqScanExecutor`
    #[serde(default = "default::developer::batch_chunk_size")]
    pub chunk_size: usize,

    /// The number of the connections for batch remote exchange between two nodes.
    /// If not specified, the value of `server.connection_pool_size` will be used.
    #[serde(default = "default::developer::batch_exchange_connection_pool_size")]
    pub(crate) exchange_connection_pool_size: Option<u16>,

    #[serde(default)]
    pub compute_client_config: RpcClientConfig,

    #[serde(default)]
    pub frontend_client_config: RpcClientConfig,

    #[serde(default = "default::developer::batch_local_execute_buffer_size")]
    pub local_execute_buffer_size: usize,
}

mod default {
    pub mod batch {
        pub fn enable_barrier_read() -> bool { false }
        pub fn enable_spill() -> bool { false }
        pub fn statement_timeout_in_sec() -> u32 { 3600 }
        pub fn mask_worker_temporary_secs() -> usize { 30 }
        pub fn redact_sql_option_keywords() -> Vec<String> {
            vec!["credential".to_string(), "key".to_string(), "secret".to_string(), 
                 "password".to_string(), "token".to_string()]
        }
    }

    pub mod developer {
        pub fn connector_message_buffer_size() -> usize { 16 }
        pub fn batch_output_channel_size() -> usize { 64 }
        pub fn batch_receiver_channel_size() -> usize { 64 }
        pub fn batch_root_stage_channel_size() -> usize { 1000 }
        pub fn batch_chunk_size() -> usize { 1024 }
        pub fn batch_local_execute_buffer_size() -> usize { 64 }
        pub fn batch_exchange_connection_pool_size() -> Option<u16> { None }
        pub fn rpc_client_connect_timeout_secs() -> u64 { 5 }
    }
}