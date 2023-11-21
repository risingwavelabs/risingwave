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
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{KeySchemaElement, KeyType, AttributeDefinition, ScalarAttributeType, ProvisionedThroughput};
use deltalake::table::builder::s3_storage_options::{
    AWS_ACCESS_KEY_ID, AWS_ENDPOINT_URL, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ALLOW_HTTP, AWS_S3_LOCKING_PROVIDER, AWS_WEB_IDENTITY_TOKEN_FILE,
};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{DeltaTable, Schema as DeltaSchema, SchemaDataType};
use risingwave_common::array::{StreamChunk, to_record_batch_with_schema_arrow46};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use serde_derive::{Deserialize, Serialize};
use serde_with::serde_as;
use with_options::WithOptions;

use super::writer::{LogSinkerOf, SinkWriter};
use super::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use crate::aws_auth::AwsAuthProps;
use crate::sink::writer::SinkWriterExt;

pub const DELTALAKE_SINK: &str = "deltalake_rust";

#[derive(Deserialize, Serialize, Debug, Clone, WithOptions)]
pub struct DeltaLakeCommon {
    #[serde(rename = "s3.access.key")]
    pub s3_access_key: Option<String>,
    #[serde(rename = "s3.secret.key")]
    pub s3_secret_key: Option<String>,
    #[serde(rename = "s3.endpoint")]
    pub s3_endpoint: Option<String>,
    #[serde(rename = "local.path")]
    pub local_path: Option<String>,
    #[serde(rename = "s3.path")]
    pub s3_path: Option<String>,
    #[serde(rename = "s3.region")]
    pub s3_region: Option<String>,
}
impl DeltaLakeCommon {
    pub async fn create_deltalake_client(&self) -> Result<DeltaTable> {
        let table = if let Some(local_path) = &self.local_path {
            deltalake::open_table(local_path).await?
        } else if let Some(s3_path) = &self.s3_path {
            let mut storage_options = HashMap::new();
            // storage_options.insert(
            //     AWS_ENDPOINT_URL.to_string(),
            //     self.s3_endpoint.clone().ok_or_else(|| {
            //         SinkError::Config(anyhow!("s3.endpoint is required with aws s3"))
            //     })?,
            // );
            storage_options.insert(
                AWS_ACCESS_KEY_ID.to_string(),
                self.s3_access_key.clone().ok_or_else(|| {
                    SinkError::Config(anyhow!("s3.access.key is required with aws s3"))
                })?,
            );
            storage_options.insert(
                AWS_SECRET_ACCESS_KEY.to_string(),
                self.s3_secret_key.clone().ok_or_else(|| {
                    SinkError::Config(anyhow!("s3.secret.key is required with aws s3"))
                })?,
            );
            storage_options.insert(
                AWS_REGION.to_string(),
                self.s3_region.clone().ok_or_else(|| {
                    SinkError::Config(anyhow!("s3.region is required with aws s3"))
                })?
            );
            storage_options.insert(
                AWS_ALLOW_HTTP.to_string(),
                "true".to_string(),
            ); 
            storage_options.insert(
                AWS_S3_LOCKING_PROVIDER.to_string(),
                "dynamodb".to_string(),
            ); 
            self.create_dynamo_lock().await?;            
            deltalake::open_table_with_storage_options(s3_path.clone(), storage_options).await?
        } else {
            return Err(SinkError::DeltaLake(
                "`local.path` or `s3.path` set at least one, configure as needed.".to_string(),
            ));
        };
        Ok(table)
    }
    async fn create_dynamo_lock(&self) -> Result<()>{
        let mut pairs = HashMap::new();
        pairs.insert("access_key", self.s3_access_key.as_ref().unwrap());
        // pairs.insert("endpoint", self.s3_endpoint.as_ref().unwrap());
        pairs.insert("secret_access", self.s3_secret_key.as_ref().unwrap());
        pairs.insert("region", self.s3_region.as_ref().unwrap());
        let config = AwsAuthProps::from_pairs(
            pairs
                .iter()
                .map(|(k, v)| (*k, v.as_str())),
        ).build_config().await?;
        let client = Client::new(&config);

        let key_schema = KeySchemaElement::builder()
        .attribute_name("key")
        .key_type(KeyType::Hash)
        .build()
        .map_err(|e| SinkError::DeltaLake(e.to_string()))?;

        let attribute_definition = AttributeDefinition::builder()
        .attribute_name("key")
        .attribute_type(ScalarAttributeType::S)
        .build()
        .map_err(|e| SinkError::DeltaLake(e.to_string()))?;

        let pt = ProvisionedThroughput::builder()
        .read_capacity_units(10)
        .write_capacity_units(10)
        .build()
        .map_err(|e| SinkError::DeltaLake(e.to_string()))?;

        client.create_table()
        .table_name("delta_rs_lock_table")
        .key_schema(key_schema).attribute_definitions(attribute_definition).provisioned_throughput(pt).send()
        .await
        .map_err(|e| SinkError::DeltaLake(format!("cannot create dynamic table for lock, err is {}" , e)))?;

        Ok(())
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct DeltaLakeConfig {
    #[serde(flatten)]
    pub common: DeltaLakeCommon,

    pub r#type: String,
}

impl DeltaLakeConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<DeltaLakeConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY
            && config.r#type != SINK_USER_FORCE_APPEND_ONLY_OPTION
        {
            return Err(SinkError::Config(anyhow!(
                "only append-only delta lake sink is supported",
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct DeltaLakeSink {
    pub config: DeltaLakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl DeltaLakeSink {
    pub fn new(
        config: DeltaLakeConfig,
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
impl DeltaLakeSink {
    fn check_field_type(
        &self,
        rw_data_type: &DataType,
        dl_data_type: &SchemaDataType,
    ) -> Result<bool> {
        let mut result = false;
        match rw_data_type {
            DataType::Boolean => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("boolean");
                }
            }
            DataType::Int16 => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("short");
                }
            }
            DataType::Int32 => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("integer");
                }
            }
            DataType::Int64 => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("long");
                }
            }
            DataType::Float32 => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("float");
                }
            }
            DataType::Float64 => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("double");
                }
            }
            DataType::Decimal => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("decimal");
                }
            }
            DataType::Date => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("date");
                }
            }
            DataType::Varchar => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("string");
                }
            }
            DataType::Time => {
                return Err(SinkError::DeltaLake(
                    "deltalake cannot support type Time".to_string(),
                ))
            }
            DataType::Timestamp => {
                if let SchemaDataType::primitive(s) = dl_data_type {
                    result = s.eq("timestamp");
                }
            }
            DataType::Timestamptz => {
                return Err(SinkError::DeltaLake(
                    "deltalake cannot support type Timestamptz".to_string(),
                ))
            }
            DataType::Interval => {
                return Err(SinkError::DeltaLake(
                    "deltalake cannot support type Interval".to_string(),
                ))
            }
            DataType::Struct(rw_s) => {
                if let SchemaDataType::r#struct(dl_s) = dl_data_type {
                    for ((rw_n, rw_t), dl_f) in rw_s
                        .names()
                        .zip_eq_fast(rw_s.types())
                        .zip_eq_fast(dl_s.get_fields())
                    {
                        result = self.check_field_type(rw_t, dl_f.get_type())?;
                        if !(result && rw_n.eq(dl_f.get_name())) {
                            return Ok(result);
                        }
                    }
                    result = true;
                }
            }
            DataType::List(rw_l) => {
                if let SchemaDataType::array(dl_l) = dl_data_type {
                    result = self.check_field_type(rw_l.as_list(), dl_l.get_element_type())?;
                }
            }
            DataType::Bytea => {
                return Err(SinkError::DeltaLake(
                    "deltalake cannot support type Bytea".to_string(),
                ))
            }
            DataType::Jsonb => {
                return Err(SinkError::DeltaLake(
                    "deltalake cannot support type Jsonb".to_string(),
                ))
            }
            DataType::Serial => {
                return Err(SinkError::DeltaLake(
                    "deltalake cannot support type Serial".to_string(),
                ))
            }
            DataType::Int256 => {
                return Err(SinkError::DeltaLake(
                    "deltalake cannot support type Int256".to_string(),
                ))
            }
        };
        Ok(result)
    }
}

impl Sink for DeltaLakeSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<DeltaLakeSinkWriter>;

    const SINK_NAME: &'static str = DELTALAKE_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(DeltaLakeSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
        let table = self.config.common.create_deltalake_client().await?;
        let deltalake_fields: HashMap<String, &SchemaDataType> = table
            .get_schema()?
            .get_fields()
            .iter()
            .map(|f| (f.get_name().to_string(), f.get_type()))
            .collect();
        if deltalake_fields.len() != self.schema.fields().len() {
            return Err(SinkError::DeltaLake(format!(
                "column count not match, rw is {}, deltalake is {}",
                self.schema.fields().len(),
                deltalake_fields.len()
            )));
        }
        for field in self.schema.fields() {
            if !deltalake_fields.contains_key(&field.name) {
                return Err(SinkError::DeltaLake(format!(
                    "column {} not found in deltalake table",
                    field.name
                )));
            }
            let deltalake_field_type: &&SchemaDataType = deltalake_fields
                .get(&field.name)
                .ok_or_else(|| SinkError::DeltaLake("cannot find field type".to_string()))?;
            if !self.check_field_type(&field.data_type, deltalake_field_type)? {
                return Err(SinkError::DeltaLake(format!(
                    "column {} type is not match, deltalake is {:?}, rw is{:?}",
                    field.name, deltalake_field_type, field.data_type
                )));
            }
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for DeltaLakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = DeltaLakeConfig::from_hashmap(param.properties)?;
        DeltaLakeSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

pub struct DeltaLakeSinkWriter {
    pub config: DeltaLakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    writer: RecordBatchWriter,
    is_append_only: bool,
    dl_schema: Arc<deltalake::arrow::datatypes::Schema>,
    dl_table: DeltaTable,
}

impl DeltaLakeSinkWriter {
    pub async fn new(
        config: DeltaLakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let dl_table = config.common.create_deltalake_client().await?;
        let writer = RecordBatchWriter::for_table(&dl_table)?;
        let dl_schema: Arc<deltalake::arrow::datatypes::Schema> = Arc::new(covert_schema(dl_table.get_schema()?)?);

        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
            writer,
            dl_schema,
            dl_table,
        })
    }

    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let a = to_record_batch_with_schema_arrow46(self.dl_schema.clone(), &chunk)
            .map_err(|err| SinkError::DeltaLake(format!("convert record batch error: {}", err)))?;
        self.writer.write(a).await?;
        Ok(())
    }
}

fn covert_schema(schema: &DeltaSchema) -> Result<deltalake::arrow::datatypes::Schema> {
    let mut builder = deltalake::arrow::datatypes::SchemaBuilder::new();
    for field in schema.get_fields() {
        let dl_field = deltalake::arrow::datatypes::Field::new(
            field.get_name(),
            deltalake::arrow::datatypes::DataType::try_from(field.get_type())
                .map_err(|err| SinkError::DeltaLake(format!("convert schema error: {}", err)))?,
            field.is_nullable(),
        );
        builder.push(dl_field);
    }
    Ok(builder.finish())
}

#[async_trait]
impl SinkWriter for DeltaLakeSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.write(chunk).await
        } else {
            Err(SinkError::DeltaLake(
                "only append-only delta lake sink is supported".to_string(),
            ))
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        self.writer.flush_and_commit(&mut self.dl_table).await?;
        // self.writer.flush().await?;
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

// #[cfg(test)]
// mod tests{
//     use std::collections::HashMap;

//     use deltalake::table::builder::s3_storage_options::{AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_ALLOW_UNSAFE_RENAME, AWS_REGION, AWS_ALLOW_HTTP};


//     #[tokio::test]
//     async fn test(){
//         let mut storage_options = HashMap::new();
//             storage_options.insert(
//                 "endpoint_url".to_string(),
//                 "http://127.0.0.1:9301".to_string(),
//             );
//             storage_options.insert(
//                 "aws_access_key_id".to_string(),
//                 "hummockadmin".to_string(),
//             );
//             storage_options.insert(
//                 "aws_secret_access_key".to_string(),
//                 "hummockadmin".to_string(),
//             );
//             storage_options.insert(
//                 "aws_region".to_string(),
//                 "us-east-1".to_string(),
//             );
//             storage_options.insert(
//                 AWS_ALLOW_HTTP.to_string(),
//                 "true".to_string(),
//             );
//             deltalake::open_table_with_storage_options("s3a://yiming-test/deltalake-time".to_string(), storage_options).await.unwrap();
//     }
// }