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

use super::avro::AvroAccessBuilder;
use super::bytes_parser::BytesAccessBuilder;
use super::simd_json_parser::{DebeziumJsonAccessBuilder, DebeziumMongoJsonAccessBuilder};
use super::unified::AccessImpl;
use super::{
    AvroParserConfig, DebeziumAvroAccessBuilder, EncodingProperties, JsonAccessBuilder,
    ProtobufAccessBuilder, ProtobufParserConfig,
};
use crate::error::ConnectorResult;
use crate::source::SourceMeta;

/// Parses raw bytes into a specific format (avro, json, protobuf, ...), and then builds an [`Access`](risingwave_connector_codec::decoder::Access) from the parsed data.
pub trait AccessBuilder {
    async fn generate_accessor(
        &mut self,
        payload: Vec<u8>,
        source_meta: &SourceMeta,
    ) -> ConnectorResult<AccessImpl<'_>>;
}

#[derive(Debug)]
pub enum AccessBuilderImpl {
    Avro(AvroAccessBuilder),
    Protobuf(ProtobufAccessBuilder),
    Json(JsonAccessBuilder),
    Bytes(BytesAccessBuilder),
    DebeziumAvro(DebeziumAvroAccessBuilder),
    DebeziumJson(DebeziumJsonAccessBuilder),
    DebeziumMongoJson(DebeziumMongoJsonAccessBuilder),
}

impl AccessBuilderImpl {
    pub async fn new_default(config: EncodingProperties) -> ConnectorResult<Self> {
        let accessor = match config {
            EncodingProperties::Avro(_) => {
                let config = AvroParserConfig::new(config).await?;
                AccessBuilderImpl::Avro(AvroAccessBuilder::new(config)?)
            }
            EncodingProperties::Protobuf(_) => {
                let config = ProtobufParserConfig::new(config).await?;
                AccessBuilderImpl::Protobuf(ProtobufAccessBuilder::new(config)?)
            }
            EncodingProperties::Bytes(_) => {
                AccessBuilderImpl::Bytes(BytesAccessBuilder::new(config)?)
            }
            EncodingProperties::Json(config) => {
                AccessBuilderImpl::Json(JsonAccessBuilder::new(config)?)
            }
            _ => unreachable!(),
        };
        Ok(accessor)
    }

    pub async fn generate_accessor(
        &mut self,
        payload: Vec<u8>,
        source_meta: &SourceMeta,
    ) -> ConnectorResult<AccessImpl<'_>> {
        let accessor = match self {
            Self::Avro(builder) => builder.generate_accessor(payload, source_meta).await?,
            Self::Protobuf(builder) => builder.generate_accessor(payload, source_meta).await?,
            Self::Json(builder) => builder.generate_accessor(payload, source_meta).await?,
            Self::Bytes(builder) => builder.generate_accessor(payload, source_meta).await?,
            Self::DebeziumAvro(builder) => builder.generate_accessor(payload, source_meta).await?,
            Self::DebeziumJson(builder) => builder.generate_accessor(payload, source_meta).await?,
            Self::DebeziumMongoJson(builder) => {
                builder.generate_accessor(payload, source_meta).await?
            }
        };
        Ok(accessor)
    }
}
