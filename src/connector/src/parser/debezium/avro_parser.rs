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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use apache_avro::types::Value;
use apache_avro::{from_avro_datum, Schema};
use futures_async_stream::try_stream;
use reqwest::Url;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::plan_common::ColumnDesc;

use super::operators::*;
use crate::impl_common_parser_logic;
use crate::parser::avro::util::avro_field_to_column_desc;
use crate::parser::schema_registry::{extract_schema_id, Client};
use crate::parser::schema_resolver::ConfluentSchemaResolver;
use crate::parser::util::get_kafka_topic;
use crate::parser::{from_avro_value, SourceStreamChunkRowWriter, WriteGuard};
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

#[derive(Debug, Clone)]
pub struct DebeziumAvroParserConfig {
    pub schema: Arc<Schema>,
    pub schema_resolver: Arc<ConfluentSchemaResolver>,
}

impl DebeziumAvroParserConfig {
    pub async fn new(props: &HashMap<String, String>, schema_location: &str) -> Result<Self> {
        let url = Url::parse(schema_location).map_err(|e| {
            InternalError(format!("failed to parse url ({}): {}", schema_location, e))
        })?;
        let kafka_topic = get_kafka_topic(props)?;
        let client = Client::new(url, props)?;
        let (schema, resolver) =
            ConfluentSchemaResolver::new(format!("{}-value", kafka_topic).as_str(), client).await?;
        Ok(Self {
            schema: Arc::new(schema),
            schema_resolver: Arc::new(resolver),
        })
    }

    fn extract_inner_schema(outer_schema: &Schema) -> Result<Schema> {
        match outer_schema {
            Schema::Record { fields, lookup, .. } => {
                let index = lookup.get(BEFORE).ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "debezium avro msg schema invalid, before field required".to_owned(),
                    ))
                })?;
                let before_schema = &fields
                    .get(*index)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError("debezium avro msg schema illegal".to_owned()))
                    })?
                    .schema;
                match before_schema {
                    Schema::Union(union_schema) => {
                        let inner_schema = union_schema
                            .variants()
                            .iter()
                            .find(|s| **s != Schema::Null)
                            .ok_or_else(|| {
                                RwError::from(InternalError(
                                    "before field of debezium avro msg schema invalid".to_owned(),
                                ))
                            })?
                            .clone();
                        Ok(inner_schema)
                    }
                    _ => Err(RwError::from(ProtocolError(
                        "before field of debezium avro msg schema invalid, union required"
                            .to_owned(),
                    ))),
                }
            }
            _ => Err(RwError::from(ProtocolError(
                "debezium avro msg schema invalid, record required".to_owned(),
            ))),
        }
    }

    pub fn map_to_columns(&self) -> Result<Vec<ColumnDesc>> {
        let inner_schema = Self::extract_inner_schema(self.schema.as_ref())?;
        // there must be a record at top level
        if let Schema::Record { fields, .. } = inner_schema {
            let mut index = 0;
            let fields = fields
                .iter()
                .map(|field| avro_field_to_column_desc(&field.name, &field.schema, &mut index))
                .collect::<Result<Vec<_>>>()?;
            tracing::info!("fields is {:?}", fields);
            Ok(fields)
        } else {
            Err(RwError::from(InternalError(
                "inner avro schema invalid, record required".into(),
            )))
        }
    }
}

fn get_from_avro_value<'a>(avro_value: &'a Value, field_name: &str) -> Result<&'a Value> {
    match avro_value {
        Value::Record(fields) => fields
            .iter()
            .find(|val| val.0.eq(field_name))
            .map(|entry| &entry.1)
            .ok_or_else(|| {
                RwError::from(ProtocolError(format!(
                    "field {} not found in debezium event",
                    field_name
                )))
            }),
        Value::Union(_, boxed_value) => get_from_avro_value(boxed_value.as_ref(), field_name),
        _ => Err(RwError::from(ProtocolError(format!(
            "avro parse unexpected field {}",
            field_name
        )))),
    }
}

impl DebeziumAvroParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        config: DebeziumAvroParserConfig,
    ) -> Result<Self> {
        let DebeziumAvroParserConfig {
            schema,
            schema_resolver,
        } = config;
        Ok(Self {
            schema,
            schema_resolver,
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

        let avro_value = from_avro_datum(writer_schema.as_ref(), &mut raw_payload, None)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let op = get_from_avro_value(&avro_value, OP)?;
        if let Value::String(op_str) = op {
            match op_str.as_str() {
                DEBEZIUM_UPDATE_OP => {
                    let before = get_from_avro_value(&avro_value, BEFORE)
                        .map_err(|_| {
                            RwError::from(ProtocolError(
                                "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                            ))
                        })?;
                    let after = get_from_avro_value(&avro_value, AFTER)?;

                    writer.update(|column| {
                        let before = from_avro_value(
                            get_from_avro_value(before, column.name.as_str())?.clone(),
                        )?;
                        let after = from_avro_value(
                            get_from_avro_value(after, column.name.as_str())?.clone(),
                        )?;

                        Ok((before, after))
                    })
                }
                DEBEZIUM_CREATE_OP | DEBEZIUM_READ_OP => {
                    let after = get_from_avro_value(&avro_value, AFTER)?;

                    writer.insert(|column| {
                        from_avro_value(get_from_avro_value(after, column.name.as_str())?.clone())
                    })
                }
                DEBEZIUM_DELETE_OP => {
                    let before = get_from_avro_value(&avro_value, BEFORE)
                        .map_err(|_| {
                            RwError::from(ProtocolError(
                                "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                            ))
                        })?;

                    writer.delete(|column| {
                        from_avro_value(get_from_avro_value(before, column.name.as_str())?.clone())
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
