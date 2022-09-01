// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub use avro_parser::*;
pub use debezium::*;
pub use json_parser::*;
pub use protobuf_parser::*;
use risingwave_common::array::Op;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::Datum;

use crate::{SourceColumnDesc, SourceFormat};

mod avro_parser;
mod common;
mod debezium;
mod json_parser;
mod protobuf_parser;

#[derive(Debug, Default)]
pub struct Event {
    pub ops: Vec<Op>,
    pub rows: Vec<Vec<Datum>>,
}

/// `SourceParser` is the message parser, `ChunkReader` will parse the messages in `SourceReader`
/// one by one through `SourceParser` and assemble them into `DataChunk`
/// Note that the `skip_parse` parameter in `SourceColumnDesc`, when it is true, should skip the
/// parse and return `Datum` of `None`
pub trait SourceParser: Send + Sync + Debug + 'static {
    /// parse needs to be a member method because some format like Protobuf needs to be pre-compiled
    fn parse(&self, payload: &[u8], columns: &[SourceColumnDesc]) -> Result<Event>;
}

#[derive(Debug)]
pub enum SourceParserImpl {
    Json(JSONParser),
    Protobuf(ProtobufParser),
    DebeziumJson(DebeziumJsonParser),
    Avro(AvroParser),
}

impl SourceParserImpl {
    pub fn parse(&self, payload: &[u8], columns: &[SourceColumnDesc]) -> Result<Event> {
        match self {
            Self::Json(parser) => parser.parse(payload, columns),
            Self::Protobuf(parser) => parser.parse(payload, columns),
            Self::DebeziumJson(parser) => parser.parse(payload, columns),
            Self::Avro(avro_parser) => avro_parser.parse(payload, columns),
        }
    }

    pub async fn create(
        format: &SourceFormat,
        properties: &HashMap<String, String>,
        schema_location: &str,
    ) -> Result<Arc<Self>> {
        const PROTOBUF_MESSAGE_KEY: &str = "proto.message";
        let parser = match format {
            SourceFormat::Json => SourceParserImpl::Json(JSONParser {}),
            SourceFormat::Protobuf => {
                let message_name = properties.get(PROTOBUF_MESSAGE_KEY).ok_or_else(|| {
                    RwError::from(ProtocolError(format!(
                        "Must specify '{}' in WITH clause",
                        PROTOBUF_MESSAGE_KEY
                    )))
                })?;
                SourceParserImpl::Protobuf(ProtobufParser::new(schema_location, message_name)?)
            }
            SourceFormat::DebeziumJson => SourceParserImpl::DebeziumJson(DebeziumJsonParser {}),
            SourceFormat::Avro => {
                SourceParserImpl::Avro(AvroParser::new(schema_location, properties.clone()).await?)
            }
            _ => {
                return Err(RwError::from(ProtocolError(
                    "format not support".to_string(),
                )));
            }
        };
        Ok(Arc::new(parser))
    }
}
