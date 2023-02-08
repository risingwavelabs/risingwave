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
use apache_avro::{from_avro_datum, Reader, Schema};
use chrono::Datelike;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{
    DataType, Datum, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, OrderedF32, OrderedF64,
    ScalarImpl,
};
use risingwave_pb::plan_common::ColumnDesc;
use url::Url;

use super::schema_resolver::*;
use crate::impl_common_parser_logic;
use crate::parser::schema_registry::{extract_schema_id, Client};
use crate::parser::util::get_kafka_topic;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::SourceColumnDesc;

fn unix_epoch_days() -> i32 {
    NaiveDateWrapper::from_ymd_uncheck(1970, 1, 1)
        .0
        .num_days_from_ce()
}

impl_common_parser_logic!(AvroParser);

#[derive(Debug)]
pub struct AvroParser {
    schema: Arc<Schema>,
    schema_resolver: Option<Arc<ConfluentSchemaResolver>>,
    rw_columns: Vec<SourceColumnDesc>,
}

#[derive(Debug, Clone)]
pub struct AvroParserConfig {
    schema: Arc<Schema>,
    schema_resolver: Option<Arc<ConfluentSchemaResolver>>,
}

impl AvroParserConfig {
    pub async fn new(
        props: &HashMap<String, String>,
        schema_location: &str,
        use_schema_registry: bool,
    ) -> Result<Self> {
        let url = Url::parse(schema_location).map_err(|e| {
            InternalError(format!("failed to parse url ({}): {}", schema_location, e))
        })?;
        let (schema, schema_resolver) = if use_schema_registry {
            let kafka_topic = get_kafka_topic(props)?;
            let client = Client::new(url, props)?;
            let (schema, resolver) =
                ConfluentSchemaResolver::new(format!("{}-value", kafka_topic).as_str(), client)
                    .await?;
            (Arc::new(schema), Some(Arc::new(resolver)))
        } else {
            let schema_content = match url.scheme() {
                "file" => read_schema_from_local(url.path()),
                "s3" => read_schema_from_s3(&url, props).await,
                "https" | "http" => read_schema_from_http(&url).await,
                scheme => Err(RwError::from(ProtocolError(format!(
                    "path scheme {} is not supported",
                    scheme
                )))),
            }?;
            let schema = Schema::parse_str(&schema_content).map_err(|e| {
                RwError::from(InternalError(format!("Avro schema parse error {}", e)))
            })?;
            (Arc::new(schema), None)
        };
        Ok(Self {
            schema,
            schema_resolver,
        })
    }

    pub fn map_to_columns(&self) -> Result<Vec<ColumnDesc>> {
        // there must be a record at top level
        if let Schema::Record { fields, .. } = self.schema.as_ref() {
            let mut index = 0;
            let fields = fields
                .iter()
                .map(|field| {
                    Self::avro_field_to_column_desc(&field.name, &field.schema, &mut index)
                })
                .collect::<Result<Vec<_>>>()?;
            tracing::info!("fields is {:?}", fields);
            Ok(fields)
        } else {
            Err(RwError::from(InternalError(
                "schema invalid, record required".into(),
            )))
        }
    }

    fn avro_field_to_column_desc(
        name: &str,
        schema: &Schema,
        index: &mut i32,
    ) -> Result<ColumnDesc> {
        let data_type = Self::avro_type_mapping(schema)?;
        match schema {
            Schema::Record {
                name: schema_name,
                fields,
                ..
            } => {
                let vec_column = fields
                    .iter()
                    .map(|f| Self::avro_field_to_column_desc(&f.name, &f.schema, index))
                    .collect::<Result<Vec<_>>>()?;
                *index += 1;
                Ok(ColumnDesc {
                    column_type: Some(data_type.to_protobuf()),
                    column_id: *index,
                    name: name.to_owned(),
                    field_descs: vec_column,
                    type_name: schema_name.to_string(),
                })
            }
            _ => {
                *index += 1;
                Ok(ColumnDesc {
                    column_type: Some(data_type.to_protobuf()),
                    column_id: *index,
                    name: name.to_owned(),
                    ..Default::default()
                })
            }
        }
    }

    fn avro_type_mapping(schema: &Schema) -> Result<DataType> {
        let data_type = match schema {
            Schema::String => DataType::Varchar,
            Schema::Int => DataType::Int32,
            Schema::Long => DataType::Int64,
            Schema::Boolean => DataType::Boolean,
            Schema::Float => DataType::Float32,
            Schema::Double => DataType::Float64,
            Schema::Date => DataType::Date,
            Schema::TimestampMillis => DataType::Timestamp,
            Schema::TimestampMicros => DataType::Timestamp,
            Schema::Duration => DataType::Interval,
            Schema::Enum { .. } => DataType::Varchar,
            Schema::Record { fields, .. } => {
                let struct_fields = fields
                    .iter()
                    .map(|f| Self::avro_type_mapping(&f.schema))
                    .collect::<Result<Vec<_>>>()?;
                let struct_names = fields.iter().map(|f| f.name.clone()).collect_vec();
                DataType::new_struct(struct_fields, struct_names)
            }
            Schema::Array(item_schema) => {
                let item_type = Self::avro_type_mapping(item_schema.as_ref())?;
                DataType::List {
                    datatype: Box::new(item_type),
                }
            }
            Schema::Union(union_schema) if union_schema.is_nullable() => {
                let nested_schema = union_schema
                    .variants()
                    .iter()
                    .find_or_first(|s| **s != Schema::Null)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "unsupported type in Avro: {:?}",
                            union_schema
                        )))
                    })?;

                Self::avro_type_mapping(nested_schema)?
            }
            _ => {
                return Err(RwError::from(InternalError(format!(
                    "unsupported type in Avro: {:?}",
                    schema
                ))));
            }
        };

        Ok(data_type)
    }
}

// confluent_wire_format, kafka only, subject-name: "${topic-name}-value"
impl AvroParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, config: AvroParserConfig) -> Result<Self> {
        let AvroParserConfig {
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
        // parse payload to avro value
        // if use confluent schema, get writer schema from confluent schema registry
        let avro_value = if let Some(resolver) = &self.schema_resolver {
            let (schema_id, mut raw_payload) = extract_schema_id(payload)?;
            let writer_schema = resolver.get(schema_id).await?;
            from_avro_datum(writer_schema.as_ref(), &mut raw_payload, Some(&self.schema))
                .map_err(|e| RwError::from(ProtocolError(e.to_string())))?
        } else {
            let mut reader = Reader::with_schema(&self.schema, payload)
                .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;
            match reader.next() {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(RwError::from(ProtocolError(e.to_string()))),
                None => {
                    return Err(RwError::from(ProtocolError(
                        "avro parse unexpected eof".to_string(),
                    )));
                }
            }
        };
        // parse the valur to rw value
        if let Value::Record(fields) = avro_value {
            writer.insert(|column| {
                let tuple = fields.iter().find(|val| column.name.eq(&val.0)).unwrap();
                from_avro_value(tuple.1.clone()).map_err(|e| {
                    tracing::error!(
                        "failed to process value ({}): {}",
                        String::from_utf8_lossy(payload),
                        e
                    );
                    e
                })
            })
        } else {
            Err(RwError::from(ProtocolError(
                "avro parse unexpected value".to_string(),
            )))
        }
    }
}

/// Convert Avro value to datum.For now, support the following [Avro type](https://avro.apache.org/docs/current/spec.html).
///  - boolean
///  - int : i32
///  - long: i64
///  - float: f32
///  - double: f64
///  - string: String
///  - Date (the number of days from the unix epoch, 1970-1-1 UTC)
///  - Timestamp (the number of milliseconds from the unix epoch,  1970-1-1 00:00:00.000 UTC)
#[inline]
fn from_avro_value(value: Value) -> Result<Datum> {
    let v = match value {
        Value::Null => {
            return Ok(None);
        }
        Value::Boolean(b) => ScalarImpl::Bool(b),
        Value::String(s) => ScalarImpl::Utf8(s.into_boxed_str()),
        Value::Int(i) => ScalarImpl::Int32(i),
        Value::Long(i) => ScalarImpl::Int64(i),
        Value::Float(f) => ScalarImpl::Float32(OrderedF32::from(f)),
        Value::Double(f) => ScalarImpl::Float64(OrderedF64::from(f)),
        Value::Date(days) => ScalarImpl::NaiveDate(
            NaiveDateWrapper::with_days(days + unix_epoch_days()).map_err(|e| {
                let err_msg = format!("avro parse error.wrong date value {}, err {:?}", days, e);
                RwError::from(InternalError(err_msg))
            })?,
        ),
        Value::TimestampMillis(millis) => ScalarImpl::NaiveDateTime(
            NaiveDateTimeWrapper::with_secs_nsecs(
                millis / 1_000,
                (millis % 1_000) as u32 * 1_000_000,
            )
                .map_err(|e| {
                    let err_msg = format!(
                        "avro parse error.wrong timestamp millis value {}, err {:?}",
                        millis, e
                    );
                    RwError::from(InternalError(err_msg))
                })?,
        ),
        Value::TimestampMicros(micros) => ScalarImpl::NaiveDateTime(
            NaiveDateTimeWrapper::with_secs_nsecs(
                micros / 1_000_000,
                (micros % 1_000_000) as u32 * 1_000,
            )
                .map_err(|e| {
                    let err_msg = format!(
                        "avro parse error.wrong timestamp micros value {}, err {:?}",
                        micros, e
                    );
                    RwError::from(InternalError(err_msg))
                })?,
        ),
        Value::Duration(duration) => {
            let months = u32::from(duration.months()) as i32;
            let days = u32::from(duration.days()) as i32;
            let millis = u32::from(duration.millis()) as i64;
            ScalarImpl::Interval(IntervalUnit::new(months, days, millis))
        }
        Value::Enum(_, symbol) => ScalarImpl::Utf8(symbol.into_boxed_str()),
        Value::Record(descs) => {
            let rw_values = descs
                .into_iter()
                .map(|(_, value)| from_avro_value(value))
                .collect::<Result<Vec<Datum>>>()?;
            ScalarImpl::Struct(StructValue::new(rw_values))
        }
        Value::Array(values) => {
            let rw_values = values
                .into_iter()
                .map(from_avro_value)
                .collect::<Result<Vec<Datum>>>()?;
            ScalarImpl::List(ListValue::new(rw_values))
        }
        Value::Union(_, value) => return from_avro_value(*value),
        _ => {
            let err_msg = format!("avro parse error.unsupported value {:?}", value);
            return Err(RwError::from(InternalError(err_msg)));
        }
    };

    Ok(Some(v))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::env;
    use std::ops::Sub;

    use apache_avro::types::{Record, Value};
    use apache_avro::{Codec, Days, Duration, Millis, Months, Reader, Schema, Writer};
    use itertools::Itertools;
    use risingwave_common::array::Op;
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::error;
    use risingwave_common::row::Row;
    use risingwave_common::types::{
        DataType, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, ScalarImpl,
    };
    use url::Url;

    use super::{
        read_schema_from_http, read_schema_from_local, read_schema_from_s3, unix_epoch_days,
        AvroParser, AvroParserConfig,
    };
    use crate::parser::SourceStreamChunkBuilder;
    use crate::source::SourceColumnDesc;

    fn test_data_path(file_name: &str) -> String {
        let curr_dir = env::current_dir().unwrap().into_os_string();
        curr_dir.into_string().unwrap() + "/src/test_data/" + file_name
    }

    #[tokio::test]
    async fn test_read_schema_from_local() {
        let schema_path = test_data_path("complex-schema.avsc");
        let content_rs = read_schema_from_local(schema_path);
        assert!(content_rs.is_ok());
    }

    #[tokio::test]
    #[ignore]
    async fn test_load_schema_from_s3() {
        let schema_location = "s3://mingchao-schemas/complex-schema.avsc".to_string();
        let mut s3_config_props = HashMap::new();
        s3_config_props.insert("region".to_string(), "ap-southeast-1".to_string());
        let url = Url::parse(&schema_location).unwrap();
        let schema_content = read_schema_from_s3(&url, &s3_config_props).await;
        assert!(schema_content.is_ok());
        let schema = Schema::parse_str(&schema_content.unwrap());
        assert!(schema.is_ok());
        println!("schema = {:?}", schema.unwrap());
    }

    #[tokio::test]
    async fn test_load_schema_from_local() {
        let schema_location = test_data_path("complex-schema.avsc");
        let schema_content = read_schema_from_local(schema_location);
        assert!(schema_content.is_ok());
        let schema = Schema::parse_str(&schema_content.unwrap());
        assert!(schema.is_ok());
        println!("schema = {:?}", schema.unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_load_schema_from_https() {
        let schema_location =
            "https://mingchao-schemas.s3.ap-southeast-1.amazonaws.com/complex-schema.avsc";
        let url = Url::parse(schema_location).unwrap();
        let schema_content = read_schema_from_http(&url).await;
        assert!(schema_content.is_ok());
        let schema = Schema::parse_str(&schema_content.unwrap());
        assert!(schema.is_ok());
        println!("schema = {:?}", schema.unwrap());
    }

    async fn new_avro_conf_from_local(file_name: &str) -> error::Result<AvroParserConfig> {
        let schema_path = "file://".to_owned() + &test_data_path(file_name);
        AvroParserConfig::new(&HashMap::new(), schema_path.as_str(), false).await
    }

    async fn new_avro_parser_from_local(file_name: &str) -> error::Result<AvroParser> {
        let conf = new_avro_conf_from_local(file_name).await?;
        AvroParser::new(Vec::default(), conf)
    }

    #[tokio::test]
    async fn test_avro_parser() {
        let avro_parser = new_avro_parser_from_local("simple-schema.avsc")
            .await
            .unwrap();
        let schema = &avro_parser.schema;
        let record = build_avro_data(schema);
        assert_eq!(record.fields.len(), 10);
        let mut writer = Writer::with_codec(schema, Vec::new(), Codec::Snappy);
        writer.append(record.clone()).unwrap();
        let flush = writer.flush().unwrap();
        assert!(flush > 0);
        let input_data = writer.into_inner().unwrap();
        let columns = build_rw_columns();
        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 1);
        {
            let writer = builder.row_writer();
            avro_parser
                .parse_inner(&input_data[..], writer)
                .await
                .unwrap();
        }
        let chunk = builder.finish();
        let (op, row) = chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        let row = row.into_owned_row();
        for (i, field) in record.fields.iter().enumerate() {
            let value = field.clone().1;
            match value {
                Value::String(str) | Value::Union(_, box Value::String(str)) => {
                    assert_eq!(row[i], Some(ScalarImpl::Utf8(str.into_boxed_str())));
                }
                Value::Boolean(bool_val) => {
                    assert_eq!(row[i], Some(ScalarImpl::Bool(bool_val)));
                }
                Value::Int(int_val) => {
                    assert_eq!(row[i], Some(ScalarImpl::Int32(int_val)));
                }
                Value::Long(i64_val) => {
                    assert_eq!(row[i], Some(ScalarImpl::Int64(i64_val)));
                }
                Value::Float(f32_val) => {
                    assert_eq!(row[i], Some(ScalarImpl::Float32(f32_val.into())));
                }
                Value::Double(f64_val) => {
                    assert_eq!(row[i], Some(ScalarImpl::Float64(f64_val.into())));
                }
                Value::Date(days) => {
                    let date = Some(ScalarImpl::NaiveDate(
                        NaiveDateWrapper::with_days(days + unix_epoch_days()).unwrap(),
                    ));
                    assert_eq!(row[i], date);
                }
                Value::TimestampMillis(millis) => {
                    let datetime = Some(ScalarImpl::NaiveDateTime(
                        NaiveDateTimeWrapper::with_secs_nsecs(
                            millis / 1000,
                            (millis % 1000) as u32 * 1_000_000,
                        )
                            .unwrap(),
                    ));
                    assert_eq!(row[i], datetime);
                }
                Value::TimestampMicros(micros) => {
                    let datetime = Some(ScalarImpl::NaiveDateTime(
                        NaiveDateTimeWrapper::with_secs_nsecs(
                            micros / 1_000_000,
                            (micros % 1_000_000) as u32 * 1_000,
                        )
                            .unwrap(),
                    ));
                    assert_eq!(row[i], datetime);
                }
                Value::Duration(duration) => {
                    let months = u32::from(duration.months()) as i32;
                    let days = u32::from(duration.days()) as i32;
                    let millis = u32::from(duration.millis()) as i64;
                    let duration = Some(ScalarImpl::Interval(IntervalUnit::new(
                        months, days, millis,
                    )));
                    assert_eq!(row[i], duration);
                }
                _ => {
                    unreachable!()
                }
            }
        }
    }

    fn build_rw_columns() -> Vec<SourceColumnDesc> {
        vec![
            SourceColumnDesc {
                name: "id".to_string(),
                data_type: DataType::Int32,
                column_id: ColumnId::from(0),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "sequence_id".to_string(),
                data_type: DataType::Int64,
                column_id: ColumnId::from(1),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "name".to_string(),
                data_type: DataType::Varchar,
                column_id: ColumnId::from(2),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "score".to_string(),
                data_type: DataType::Float32,
                column_id: ColumnId::from(3),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "avg_score".to_string(),
                data_type: DataType::Float64,
                column_id: ColumnId::from(4),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "is_lasted".to_string(),
                data_type: DataType::Boolean,
                column_id: ColumnId::from(5),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "entrance_date".to_string(),
                data_type: DataType::Date,
                column_id: ColumnId::from(6),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "birthday".to_string(),
                data_type: DataType::Timestamp,
                column_id: ColumnId::from(7),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "anniversary".to_string(),
                data_type: DataType::Timestamp,
                column_id: ColumnId::from(8),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "passed".to_string(),
                data_type: DataType::Interval,
                column_id: ColumnId::from(9),
                is_row_id: false,
                is_meta: false,
                fields: vec![],
            },
        ]
    }

    fn build_field(schema: &Schema) -> Option<Value> {
        match schema {
            Schema::String => Some(Value::String("str_value".to_string())),
            Schema::Int => Some(Value::Int(32_i32)),
            Schema::Long => Some(Value::Long(64_i64)),
            Schema::Float => Some(Value::Float(32_f32)),
            Schema::Double => Some(Value::Double(64_f64)),
            Schema::Boolean => Some(Value::Boolean(true)),

            Schema::Date => {
                let original_date =
                    NaiveDateWrapper::from_ymd_uncheck(1970, 1, 1).and_hms_uncheck(0, 0, 0);
                let naive_date =
                    NaiveDateWrapper::from_ymd_uncheck(1970, 1, 1).and_hms_uncheck(0, 0, 0);
                let num_days = naive_date.0.sub(original_date.0).num_days() as i32;
                Some(Value::Date(num_days))
            }
            Schema::TimestampMillis => {
                let datetime =
                    NaiveDateWrapper::from_ymd_uncheck(1970, 1, 1).and_hms_uncheck(0, 0, 0);
                let timestamp_mills = Value::TimestampMillis(datetime.0.timestamp() * 1_000);
                Some(timestamp_mills)
            }
            Schema::TimestampMicros => {
                let datetime =
                    NaiveDateWrapper::from_ymd_uncheck(1970, 1, 1).and_hms_uncheck(0, 0, 0);
                let timestamp_micros = Value::TimestampMicros(datetime.0.timestamp() * 1_000_000);
                Some(timestamp_micros)
            }
            Schema::Duration => {
                let months = Months::new(1);
                let days = Days::new(1);
                let millis = Millis::new(1000);
                Some(Value::Duration(Duration::new(months, days, millis)))
            }

            Schema::Union(union_schema) => {
                let inner_schema = union_schema
                    .variants()
                    .iter()
                    .find_or_first(|s| s != &&Schema::Null)
                    .unwrap();

                match build_field(inner_schema) {
                    None => {
                        let index_of_union =
                            union_schema.find_schema(&Value::Null).unwrap().0 as u32;
                        Some(Value::Union(index_of_union, Box::new(Value::Null)))
                    }
                    Some(value) => {
                        let index_of_union = union_schema.find_schema(&value).unwrap().0 as u32;
                        Some(Value::Union(index_of_union, Box::new(value)))
                    }
                }
            }
            _ => None,
        }
    }

    fn build_avro_data(schema: &Schema) -> Record<'_> {
        let mut record = Record::new(schema).unwrap();
        if let Schema::Record {
            name: _, fields, ..
        } = schema.clone()
        {
            for field in &fields {
                let value = build_field(&field.schema)
                    .unwrap_or_else(|| panic!("No value defined for field, {}", field.name));
                record.put(field.name.as_str(), value)
            }
        }
        record
    }

    #[tokio::test]
    async fn test_map_to_columns() {
        let conf = new_avro_conf_from_local("simple-schema.avsc")
            .await
            .unwrap();
        let columns = avro_parser_rs.map_to_columns().unwrap();
        assert_eq!(columns.len(), 10);
        println!("{:?}", columns);
    }

    #[tokio::test]
    async fn test_new_avro_parser() {
        let avro_parser_rs = new_avro_parser_from_local("simple-schema.avsc").await;
        assert!(avro_parser_rs.is_ok());
        let avro_parser = avro_parser_rs.unwrap();
        println!("avro_parser = {:?}", avro_parser);
    }

    #[tokio::test]
    async fn test_avro_union_type() {
        let avro_parser = new_avro_parser_from_local("union-schema.avsc")
            .await
            .unwrap();
        let schema = &avro_parser.schema;
        let mut null_record = Record::new(schema).unwrap();
        null_record.put("id", Value::Int(5));
        null_record.put("age", Value::Union(0, Box::new(Value::Null)));
        null_record.put("sequence_id", Value::Union(0, Box::new(Value::Null)));
        null_record.put("name", Value::Union(0, Box::new(Value::Null)));
        null_record.put("score", Value::Union(1, Box::new(Value::Null)));
        null_record.put("avg_score", Value::Union(0, Box::new(Value::Null)));
        null_record.put("is_lasted", Value::Union(0, Box::new(Value::Null)));
        null_record.put("entrance_date", Value::Union(0, Box::new(Value::Null)));
        null_record.put("birthday", Value::Union(0, Box::new(Value::Null)));
        null_record.put("anniversary", Value::Union(0, Box::new(Value::Null)));

        let mut writer = Writer::new(schema, Vec::new());
        writer.append(null_record).unwrap();
        writer.flush().unwrap();

        let record = build_avro_data(schema);
        writer.append(record).unwrap();
        writer.flush().unwrap();

        let records = writer.into_inner().unwrap();

        let reader: Vec<_> = Reader::with_schema(schema, &records[..]).unwrap().collect();
        assert_eq!(2, reader.len());
        let null_record_expected: Vec<(String, Value)> = vec![
            ("id".to_string(), Value::Int(5)),
            ("age".to_string(), Value::Union(0, Box::new(Value::Null))),
            (
                "sequence_id".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            ("name".to_string(), Value::Union(0, Box::new(Value::Null))),
            ("score".to_string(), Value::Union(1, Box::new(Value::Null))),
            (
                "avg_score".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "is_lasted".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "entrance_date".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "birthday".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "anniversary".to_string(),
                Value::Union(0, Box::new(Value::Null)),
            ),
        ];
        let null_record_value = reader.get(0).unwrap().as_ref().unwrap();
        match null_record_value {
            Value::Record(values) => {
                assert_eq!(values, &null_record_expected)
            }
            _ => unreachable!(),
        }
    }
}
