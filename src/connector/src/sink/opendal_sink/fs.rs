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
use std::collections::HashMap;

use anyhow::anyhow;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Fs;
use opendal::Operator;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use crate::sink::opendal_sink::OpenDalSinkWriter;
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY,
    SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct FsCommon {
    #[serde(rename = "fs.path", default)]
    pub path: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct FsConfig {
    #[serde(flatten)]
    pub common: FsCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

pub const FS_SINK: &str = "fs";

impl FsConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<FsConfig>(serde_json::to_value(properties).unwrap())
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
}

#[derive(Debug)]
pub struct FsSink {
    pub config: FsConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl FsSink {
    pub fn new(
        config: FsConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl FsSink {
    pub fn new_fs_sink(config: FsConfig) -> Result<Operator> {
        // Create fs builder.
        let mut builder = Fs::default();
        // Create fs backend builder.
        builder.root(&config.common.path);
        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(operator)
    }
}

impl Sink for FsSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<OpenDalSinkWriter>;

    const SINK_NAME: &'static str = FS_SINK;

    async fn validate(&self) -> Result<()> {
        let _op = Self::new_fs_sink(self.config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let op = Self::new_fs_sink(self.config.clone())?;
        let path = self.config.common.path.as_ref();
        println!("path = {}", path);
        Ok(OpenDalSinkWriter::new(
            op,
            path,
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )?
        .into_log_sinker(writer_param.sink_metrics))
    }
}

impl TryFrom<SinkParam> for FsSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = FsConfig::from_hashmap(param.properties)?;
        FsSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}
