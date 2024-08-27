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
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Hdfs;
use opendal::Operator;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::opendal_sink::FileSink;
use crate::sink::file_sink::opendal_sink::OpendalSinkBackend;
use crate::sink::{Result, SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::source::UnknownFields;
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct HdfsCommon {
    #[serde(rename = "hdfs.namenode")]
    pub namenode: String,
    /// The directory where the sink file is located.
    #[serde(rename = "hdfs.path")]
    pub path: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct HdfsConfig {
    #[serde(flatten)]
    pub common: HdfsCommon,

    pub r#type: String, // accept "append-only"

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

pub const HDFS_SINK: &str = "hdfs";

impl<S: OpendalSinkBackend> FileSink<S> {
    pub fn new_hdfs_sink(config: HdfsConfig) -> Result<Operator> {
        // Init the jvm explicitly to avoid duplicate JVM creation by hdfs client
        use risingwave_jni_core::jvm_runtime::JVM;
        let _ = JVM
            .get_or_init()
            .inspect_err(|e| tracing::error!("Failed to init JVM: {:?}", e))
            .unwrap();

        // Create hdfs backend builder.
        let mut builder = Hdfs::default();
        // Set the name node for hdfs.
        builder.name_node(&config.common.namenode);
        builder.root(&config.common.path);
        // todo: reopen the following lines after https://github.com/apache/opendal/issues/4867 is resolved.
        // if config.set_atomic_write_dir {
        //     let atomic_write_dir = format!("{}/{}", root, ATOMIC_WRITE_DIR);
        //     builder.atomic_write_dir(&atomic_write_dir);
        // }
        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        Ok(operator)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HdfsSink;

impl UnknownFields for HdfsConfig {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl OpendalSinkBackend for HdfsSink {
    type Properties = HdfsConfig;

    const SINK_NAME: &'static str = HDFS_SINK;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties> {
        let config =
            serde_json::from_value::<HdfsConfig>(serde_json::to_value(btree_map).unwrap())
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

    fn new_operator(properties: HdfsConfig) -> Result<Operator> {
        FileSink::<HdfsSink>::new_hdfs_sink(properties)
    }

    fn get_path(properties: Self::Properties) -> String {
        properties.common.path
    }

    fn get_engine_type() -> super::opendal_sink::EngineType {
        super::opendal_sink::EngineType::Hdfs
    }
}
