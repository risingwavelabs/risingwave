// Copyright 2026 RisingWave Labs
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

use std::collections::BTreeMap;

use risingwave_common::catalog::Schema;

use super::backend::SearchBackend;
use super::client::ElasticSearchOpenSearchSinkWriter;
use super::config::ElasticSearchOpenSearchConfig;
use crate::enforce_secret::EnforceSecret;
use crate::sink::writer::{AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriterExt};
use crate::sink::{Result, Sink, SinkError, SinkParam, SinkWriterParam};

#[derive(Debug)]
pub struct SearchSink<B: SearchBackend> {
    config: B::Config,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl<B: SearchBackend> EnforceSecret for SearchSink<B> {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            ElasticSearchOpenSearchConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl<B: SearchBackend> TryFrom<SinkParam> for SearchSink<B> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = B::parse_config(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl<B: SearchBackend> Sink for SearchSink<B> {
    type LogSinker = AsyncTruncateLogSinkerOf<ElasticSearchOpenSearchSinkWriter>;

    const SINK_NAME: &'static str = B::SINK_NAME;

    fn validate_unknown_fields(&self) -> Result<()> {
        crate::sink::validate_sink_unknown_fields(B::common_config(&self.config))
    }

    fn support_schema_change() -> bool {
        B::SUPPORT_SCHEMA_CHANGE
    }

    async fn validate(&self) -> Result<()> {
        B::check_available()?;
        B::common_config(&self.config).validate_config(&self.schema)?;
        let client = B::build_client(&self.config)?;
        client.ping().await?;
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        B::parse_config(config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let client = B::build_client(&self.config)?;
        let config = B::common_config(&self.config);
        Ok(ElasticSearchOpenSearchSinkWriter::new(
            config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            client,
            self.is_append_only,
        )?
        .into_log_sinker(config.concurrent_requests))
    }
}