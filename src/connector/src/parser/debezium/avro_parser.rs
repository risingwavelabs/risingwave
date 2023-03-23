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
use itertools::Itertools;
use reqwest::Url;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::plan_common::ColumnDesc;

use super::operators::*;
use crate::impl_common_parser_logic;
use crate::parser::avro::util::{
    avro_field_to_column_desc, extract_inner_field_schema, from_avro_value,
    get_field_from_avro_value,
};
use crate::parser::schema_registry::{extract_schema_id, Client};
use crate::parser::schema_resolver::ConfluentSchemaResolver;
use crate::parser::util::get_kafka_topic;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContextRef};

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";
const PAYLOAD: &str = "payload";

impl_common_parser_logic!(DebeziumAvroParser);

// TODO: avoid duplicated codes with `AvroParser`
#[derive(Debug)]
pub struct DebeziumAvroParser {
    outer_schema: Arc<Schema>,
    inner_schema: Arc<Schema>,
    schema_resolver: Arc<ConfluentSchemaResolver>,
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

#[derive(Debug, Clone)]
pub struct DebeziumAvroParserConfig {
    pub key_schema: Arc<Schema>,

    pub outer_schema: Arc<Schema>,
    pub inner_schema: Arc<Schema>,
    pub schema_resolver: Arc<ConfluentSchemaResolver>,
}

impl DebeziumAvroParserConfig {
    pub async fn new(props: &HashMap<String, String>, schema_location: &str) -> Result<Self> {
        let url = Url::parse(schema_location).map_err(|e| {
            InternalError(format!("failed to parse url ({}): {}", schema_location, e))
        })?;
        let kafka_topic = get_kafka_topic(props)?;
        let client = Client::new(url, props)?;
        let raw_schema = client
            .get_schema_by_subject(format!("{}-key", kafka_topic).as_str())
            .await?;
        let key_schema = Schema::parse_str(&raw_schema.content)
            .map_err(|e| RwError::from(ProtocolError(format!("Avro schema parse error {}", e))))?;

        let resolver = ConfluentSchemaResolver::new(client);
        let outer_schema = resolver
            .get_by_subject_name(&format!("{}-value", kafka_topic))
            .await?;
        let inner_schema = Self::extract_inner_schema(&outer_schema)?;
        Ok(Self {
            key_schema: Arc::new(key_schema),
            outer_schema,
            inner_schema: Arc::new(inner_schema),
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

    pub fn get_pk_names(&self) -> Result<Vec<String>> {
        Self::get_pk_names_inner(&self.key_schema)
    }

    pub(crate) fn get_pk_names_inner(key_schema: &Schema) -> Result<Vec<String>> {
        if let Schema::Record { fields, .. } = key_schema {
            Ok(fields.iter().map(|field| field.name.clone()).collect_vec())
        } else {
            Err(RwError::from(InternalError(
                "Get pk names from key schema: top level message must be a record".into(),
            )))
        }
    }

    pub fn map_to_columns(&self) -> Result<Vec<ColumnDesc>> {
        Self::map_to_columns_inner(&self.inner_schema)
    }

    // more convenient for testing
    pub(crate) fn map_to_columns_inner(schema: &Schema) -> Result<Vec<ColumnDesc>> {
        // there must be a record at top level
        if let Schema::Record { fields, .. } = schema {
            let mut index = 0;
            let fields = fields
                .iter()
                .map(|field| avro_field_to_column_desc(&field.name, &field.schema, &mut index))
                .collect::<Result<Vec<_>>>()?;
            Ok(fields)
        } else {
            Err(RwError::from(InternalError(
                "Map to columns from value schema failed: top level message must be a record"
                    .into(),
            )))
        }
    }
}

impl DebeziumAvroParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        config: DebeziumAvroParserConfig,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        let DebeziumAvroParserConfig {
            outer_schema,
            inner_schema,
            schema_resolver,
            ..
        } = config;
        Ok(Self {
            outer_schema,
            inner_schema,
            schema_resolver,
            rw_columns,
            source_ctx,
        })
    }

    pub(crate) async fn parse_inner(
        &self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let (schema_id, mut raw_payload) = extract_schema_id(&payload)?;
        let writer_schema = self.schema_resolver.get(schema_id).await?;

        let avro_value = from_avro_datum(writer_schema.as_ref(), &mut raw_payload, None)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let op = get_field_from_avro_value(&avro_value, OP)?;
        if let Value::String(op_str) = op {
            match op_str.as_str() {
                DEBEZIUM_UPDATE_OP => {
                    let before = get_field_from_avro_value(&avro_value, BEFORE)
                        .map_err(|_| {
                            RwError::from(ProtocolError(
                                "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                            ))
                        })?;
                    let after = get_field_from_avro_value(&avro_value, AFTER)?;

                    writer.update(|column| {
                        let field_schema =
                            extract_inner_field_schema(&self.inner_schema, Some(&column.name))?;
                        let before = from_avro_value(
                            get_field_from_avro_value(before, column.name.as_str())?.clone(),
                            field_schema,
                        )?;
                        let after = from_avro_value(
                            get_field_from_avro_value(after, column.name.as_str())?.clone(),
                            field_schema,
                        )?;

                        Ok((before, after))
                    })
                }
                DEBEZIUM_CREATE_OP | DEBEZIUM_READ_OP => {
                    let after = get_field_from_avro_value(&avro_value, AFTER)?;

                    writer.insert(|column| {
                        let field_schema =
                            extract_inner_field_schema(&self.inner_schema, Some(&column.name))?;
                        from_avro_value(
                            get_field_from_avro_value(after, column.name.as_str())?.clone(),
                            field_schema,
                        )
                    })
                }
                DEBEZIUM_DELETE_OP => {
                    let before = get_field_from_avro_value(&avro_value, BEFORE)
                        .map_err(|_| {
                            RwError::from(ProtocolError(
                                "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                            ))
                        })?;

                    writer.delete(|column| {
                        let field_schema =
                            extract_inner_field_schema(&self.inner_schema, Some(&column.name))?;
                        from_avro_value(
                            get_field_from_avro_value(before, column.name.as_str())?.clone(),
                            field_schema,
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

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::path::PathBuf;

    use apache_avro::Schema;
    use itertools::Itertools;
    use maplit::{convert_args, hashmap};
    use risingwave_common::array::Op;
    use risingwave_common::catalog::ColumnDesc as CatColumnDesc;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::*;
    use crate::parser::{DebeziumAvroParserConfig, SourceStreamChunkBuilder};

    const DEBEZIUM_AVRO_DATA: &[u8] = b"\x00\x00\x00\x00\x06\x00\x02\xd2\x0f\x0a\x53\x61\x6c\x6c\x79\x0c\x54\x68\x6f\x6d\x61\x73\x2a\x73\x61\x6c\x6c\x79\x2e\x74\x68\x6f\x6d\x61\x73\x40\x61\x63\x6d\x65\x2e\x63\x6f\x6d\x16\x32\x2e\x31\x2e\x32\x2e\x46\x69\x6e\x61\x6c\x0a\x6d\x79\x73\x71\x6c\x12\x64\x62\x73\x65\x72\x76\x65\x72\x31\xc0\xb4\xe8\xb7\xc9\x61\x00\x30\x66\x69\x72\x73\x74\x5f\x69\x6e\x5f\x64\x61\x74\x61\x5f\x63\x6f\x6c\x6c\x65\x63\x74\x69\x6f\x6e\x12\x69\x6e\x76\x65\x6e\x74\x6f\x72\x79\x00\x02\x12\x63\x75\x73\x74\x6f\x6d\x65\x72\x73\x00\x00\x20\x6d\x79\x73\x71\x6c\x2d\x62\x69\x6e\x2e\x30\x30\x30\x30\x30\x33\x8c\x06\x00\x00\x00\x02\x72\x02\x92\xc3\xe8\xb7\xc9\x61\x00";

    fn schema_dir() -> String {
        let dir = PathBuf::from("src/test_data");
        std::fs::canonicalize(dir)
            .unwrap()
            .to_string_lossy()
            .to_string()
    }

    async fn parse_one(
        parser: DebeziumAvroParser,
        columns: Vec<SourceColumnDesc>,
        payload: Vec<u8>,
    ) -> Vec<(Op, OwnedRow)> {
        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 2);
        {
            let writer = builder.row_writer();
            parser.parse_inner(payload, writer).await.unwrap();
        }
        let chunk = builder.finish();
        chunk
            .rows()
            .map(|(op, row_ref)| (op, row_ref.into_owned_row()))
            .collect::<Vec<_>>()
    }

    fn get_outer_schema() -> Schema {
        let mut outer_schema_str = String::new();
        let location = schema_dir() + "/debezium_avro_msg_schema.avsc";
        std::fs::File::open(location)
            .unwrap()
            .read_to_string(&mut outer_schema_str)
            .unwrap();
        Schema::parse_str(&outer_schema_str).unwrap()
    }

    #[test]
    fn test_extract_inner_schema() {
        let inner_shema_str = r#"{
    "type": "record",
    "name": "Value",
    "fields": [
        {
            "name": "id",
            "type": "int"
        },
        {
            "name": "first_name",
            "type": "string"
        },
        {
            "name": "last_name",
            "type": "string"
        },
        {
            "name": "email",
            "type": "string"
        }
    ]
}"#;

        let outer_schema = get_outer_schema();
        let expected_inner_schema = Schema::parse_str(inner_shema_str).unwrap();
        let extracted_inner_schema =
            DebeziumAvroParserConfig::extract_inner_schema(&outer_schema).unwrap();
        assert_eq!(expected_inner_schema, extracted_inner_schema);
    }

    #[test]
    fn test_get_pk_column_names() {
        let key_schema_str = r#"{
    "type": "record",
    "name": "Key",
    "namespace": "dbserver1.inventory.customers",
    "fields": [{
        "name": "id",
        "type": "int"
    }],
    "connect.name": "dbserver1.inventory.customers.Key"
}        
"#;
        let key_schema = Schema::parse_str(key_schema_str).unwrap();
        let names = DebeziumAvroParserConfig::get_pk_names_inner(&key_schema).unwrap();
        assert_eq!(names, vec!["id".to_owned()])
    }

    #[test]
    fn test_map_to_columns() {
        let outer_schema = get_outer_schema();
        let inner_schema = DebeziumAvroParserConfig::extract_inner_schema(&outer_schema).unwrap();
        let columns = DebeziumAvroParserConfig::map_to_columns_inner(&inner_schema)
            .unwrap()
            .into_iter()
            .map(CatColumnDesc::from)
            .collect_vec();

        assert_eq!(columns.len(), 4);
        assert_eq!(
            CatColumnDesc::new_atomic(DataType::Int32, "id", 1),
            columns[0]
        );

        assert_eq!(
            CatColumnDesc::new_atomic(DataType::Varchar, "first_name", 2),
            columns[1]
        );

        assert_eq!(
            CatColumnDesc::new_atomic(DataType::Varchar, "last_name", 3),
            columns[2]
        );

        assert_eq!(
            CatColumnDesc::new_atomic(DataType::Varchar, "email", 4),
            columns[3]
        );
    }

    #[ignore]
    #[tokio::test]
    async fn test_debezium_avro_parser() -> Result<()> {
        let props = convert_args!(hashmap!(
            "kafka.topic" => "dbserver1.inventory.customers"
        ));
        let config = DebeziumAvroParserConfig::new(&props, "http://127.0.0.1:8081").await?;
        let columns = config
            .map_to_columns()?
            .into_iter()
            .map(CatColumnDesc::from)
            .map(|c| SourceColumnDesc::from(&c))
            .collect_vec();

        let parser =
            DebeziumAvroParser::new(columns.clone(), config, Arc::new(Default::default()))?;
        let [(op, row)]: [_; 1] = parse_one(parser, columns, DEBEZIUM_AVRO_DATA.to_vec())
            .await
            .try_into()
            .unwrap();
        assert_eq!(op, Op::Insert);
        assert_eq!(row[0], Some(ScalarImpl::Int32(1001)));
        assert_eq!(row[1], Some(ScalarImpl::Utf8("Sally".into())));
        assert_eq!(row[2], Some(ScalarImpl::Utf8("Thomas".into())));
        assert_eq!(
            row[3],
            Some(ScalarImpl::Utf8("sally.thomas@acme.com".into()))
        );
        Ok(())
    }
}
