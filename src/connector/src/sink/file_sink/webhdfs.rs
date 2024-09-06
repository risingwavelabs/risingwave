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
use std::collections::{BTreeMap, HashMap};

use anyhow::anyhow;
use opendal::layers::LoggingLayer;
use opendal::services::Webhdfs;
use opendal::Operator;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::opendal_sink::{BatchingStrategy, FileSink, FileSinkBatchingStrategy};
use crate::sink::file_sink::opendal_sink::{
    parse_partition_granularity, OpendalSinkBackend, PartitionGranularity,
};
use crate::sink::{Result, SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::source::UnknownFields;
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct WebhdfsCommon {
    #[serde(rename = "webhdfs.endpoint")]
    pub endpoint: String,
    /// The directory where the sink file is located.
    #[serde(rename = "webhdfs.path")]
    pub path: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct WebhdfsConfig {
    #[serde(flatten)]
    pub common: WebhdfsCommon,

    #[serde(flatten)]
    pub batching_strategy: FileSinkBatchingStrategy,

    pub r#type: String, // accept "append-only"

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

pub const WEBHDFS_SINK: &str = "webhdfs";

impl<S: OpendalSinkBackend> FileSink<S> {
    pub fn new_webhdfs_sink(config: WebhdfsConfig) -> Result<Operator> {
        // Create webhdfs backend builder.
        let mut builder = Webhdfs::default();
        // Set the name node for hdfs.
        builder.endpoint(&config.common.endpoint);
        builder.root(&config.common.path);

        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        Ok(operator)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhdfsSink;

impl UnknownFields for WebhdfsConfig {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl OpendalSinkBackend for WebhdfsSink {
    type Properties = WebhdfsConfig;

    const SINK_NAME: &'static str = WEBHDFS_SINK;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties> {
        let config =
            serde_json::from_value::<WebhdfsConfig>(serde_json::to_value(btree_map).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }

    fn new_operator(properties: WebhdfsConfig) -> Result<Operator> {
        FileSink::<WebhdfsSink>::new_webhdfs_sink(properties)
    }

    fn get_path(properties: Self::Properties) -> String {
        properties.common.path
    }

    fn get_engine_type() -> super::opendal_sink::EngineType {
        super::opendal_sink::EngineType::Webhdfs
    }

    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy {
        let partition_granularity = if let Some(partition_granularity) =
            properties.batching_strategy.partition_granularity
        {
            parse_partition_granularity(&partition_granularity)
        } else {
            PartitionGranularity::None
        };
        let max_row_count: Option<usize> =
            if let Some(s) = properties.batching_strategy.max_row_count {
                s.parse().ok()
            } else {
                None
            };
        let rollover_seconds: Option<usize> =
            if let Some(s) = properties.batching_strategy.rollover_seconds {
                s.parse().ok()
            } else {
                None
            };
        BatchingStrategy {
            max_row_count,
            max_file_size: properties.batching_strategy.max_file_size,
            rollover_seconds,
            partition_granularity,
        }
    }
}
