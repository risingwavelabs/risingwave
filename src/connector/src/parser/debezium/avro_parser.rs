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

use std::fmt::Debug;
use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::{from_avro_datum, Schema};
use futures_async_stream::try_stream;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};

use super::operators::*;
use crate::impl_common_parser_logic;
use crate::parser::schema_registry::extract_schema_id;
use crate::parser::schema_resolver::ConfluentSchemaResolver;
use crate::parser::{
    from_avro_value, get_from_avro_record, AvroParserConfig, SourceStreamChunkRowWriter, WriteGuard,
};
use crate::source::SourceColumnDesc;

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";
const PAYLOAD: &str = "payload";

impl_common_parser_logic!(DebeziumAvroParser);

// TODO: avoid duplicated codes with `AvroParser`
#[derive(Debug)]
pub struct DebeziumAvroParser {
    schema: Arc<Schema>,
    schema_resolver: Arc<ConfluentSchemaResolver>,
    rw_columns: Vec<SourceColumnDesc>,
}

impl DebeziumAvroParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, config: AvroParserConfig) -> Result<Self> {
        let AvroParserConfig {
            schema,
            schema_resolver,
        } = config;
        debug_assert!(schema_resolver.is_some());
        Ok(Self {
            schema,
            schema_resolver: schema_resolver.unwrap(),
            rw_columns,
        })
    }

    pub(crate) async fn parse_inner(
        &self,
        payload: &[u8],
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let (schema_id, mut raw_payload) = extract_schema_id(payload)?;
        let writer_schema = self.schema_resolver.get(schema_id).await?;
        let avro_value =
            from_avro_datum(writer_schema.as_ref(), &mut raw_payload, Some(&self.schema))
                .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;
        let payload = get_from_avro_record(&avro_value, PAYLOAD)?;
        let op = get_from_avro_record(payload, OP)?;
        if let Value::String(op_str) = op {
            match op_str.as_str() {
                DEBEZIUM_UPDATE_OP => {
                    let before = get_from_avro_record(&avro_value, BEFORE)
                        .map_err(|_| {
                            RwError::from(ProtocolError(
                                "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                            ))
                        })?;
                    let after = get_from_avro_record(&avro_value, AFTER)?;

                    writer.update(|column| {
                        let before = from_avro_value(
                            get_from_avro_record(&before, column.name.as_str())?.clone(),
                        )?;
                        let after = from_avro_value(
                            get_from_avro_record(&after, column.name.as_str())?.clone(),
                        )?;

                        Ok((before, after))
                    })
                }
                DEBEZIUM_CREATE_OP | DEBEZIUM_READ_OP => {
                    let after = get_from_avro_record(&avro_value, AFTER)?;

                    writer.insert(|column| {
                        from_avro_value(get_from_avro_record(&after, column.name.as_str())?.clone())
                    })
                }
                DEBEZIUM_DELETE_OP => {
                    let before = get_from_avro_record(&avro_value, BEFORE)
                        .map_err(|_| {
                            RwError::from(ProtocolError(
                                "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                            ))
                        })?;

                    writer.delete(|column| {
                        from_avro_value(
                            get_from_avro_record(&before, column.name.as_str())?.clone(),
                        )
                    })
                }
                _ => Err(RwError::from(ProtocolError(format!(
                    "unknown debezium op: {}",
                    op_str
                )))),
            }
        } else {
            Err(RwError::from(ProtocolError(
                "payload op is not a string ".to_owned(),
            )))
        }
    }
}
