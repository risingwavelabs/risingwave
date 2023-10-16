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

use core::mem;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_data_insert_all_request_rows::TableDataInsertAllRequestRows;
use gcp_bigquery_client::Client;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde_derive::Deserialize;
use serde_json::Value;
use serde_with::serde_as;

use super::encoder::{JsonEncoder, RowEncoder, TimestampHandlingMode};
use super::writer::LogSinkerOf;
use super::{SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::common::BigQueryCommon;
use crate::sink::writer::SinkWriterExt;
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkParam, SinkWriter, SinkWriterParam,
};

pub const BIGQUERY_SINK: &str = "bigquery";
const BIGQUERY_INSERT_MAX_NUMS: usize = 500;
#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct BigQueryConfig {
    #[serde(flatten)]
    pub common: BigQueryCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}
impl BigQueryConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<BigQueryConfig>(serde_json::to_value(properties).unwrap())
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
pub struct BigQuerySink {
    pub config: BigQueryConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl BigQuerySink {
    pub fn new(
        config: BigQueryConfig,
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

impl BigQuerySink {
    fn check_column_name_and_type(
        &self,
        big_query_columns_desc: HashMap<String, String>,
    ) -> Result<()> {
        let rw_fields_name = self.schema.fields();
        if big_query_columns_desc.is_empty() {
            return Err(SinkError::BigQuery(
                "Cannot find table in bigquery".to_string(),
            ));
        }
        if rw_fields_name.len().ne(&big_query_columns_desc.len()) {
            return Err(SinkError::BigQuery(format!("The length of the RisingWave column {} must be equal to the length of the bigquery column {}",rw_fields_name.len(),big_query_columns_desc.len())));
        }

        for i in rw_fields_name {
            let value = big_query_columns_desc.get(&i.name).ok_or_else(|| {
                SinkError::BigQuery(format!(
                    "Column name don't find in bigquery, risingwave is {:?} ",
                    i.name
                ))
            })?;
            let data_type_string = Self::get_string_and_check_support_from_datatype(&i.data_type)?;
            if data_type_string.ne(value) {
                return Err(SinkError::BigQuery(format!(
                    "Column type don't match, column name is {:?}. bigquery type is {:?} risingwave type is {:?} ",i.name,value,data_type_string
                )));
            };
        }
        Ok(())
    }

    fn get_string_and_check_support_from_datatype(rw_data_type: &DataType) -> Result<String> {
        match rw_data_type {
            DataType::Boolean => Ok("BOOL".to_owned()),
            DataType::Int16 => Ok("INT64".to_owned()),
            DataType::Int32 => Ok("INT64".to_owned()),
            DataType::Int64 => Ok("INT64".to_owned()),
            DataType::Float32 => Err(SinkError::BigQuery(
                "Bigquery cannot support real".to_string(),
            )),
            DataType::Float64 => Ok("FLOAT64".to_owned()),
            DataType::Decimal => Ok("NUMERIC".to_owned()),
            DataType::Date => Ok("DATE".to_owned()),
            DataType::Varchar => Ok("STRING".to_owned()),
            DataType::Time => Err(SinkError::BigQuery(
                "Bigquery cannot support Time".to_string(),
            )),
            DataType::Timestamp => Ok("DATETIME".to_owned()),
            DataType::Timestamptz => Ok("TIMESTAMP".to_owned()),
            DataType::Interval => Ok("INTERVAL".to_owned()),
            DataType::Struct(structs) => {
                let mut elements_vec = vec![];
                for (name, datatype) in structs.iter() {
                    let element_string =
                        Self::get_string_and_check_support_from_datatype(datatype)?;
                    elements_vec.push(format!("{} {}", name, element_string));
                }
                Ok(format!("STRUCT<{}>", elements_vec.join(", ")))
            }
            DataType::List(l) => {
                let element_string = Self::get_string_and_check_support_from_datatype(l.as_ref())?;
                Ok(format!("ARRAY<{}>", element_string))
            }
            DataType::Bytea => Ok("BYTES".to_owned()),
            DataType::Jsonb => Ok("JSON".to_owned()),
            DataType::Serial => Ok("INT64".to_owned()),
            DataType::Int256 => Err(SinkError::BigQuery(
                "Bigquery cannot support Int256".to_string(),
            )),
        }
    }
}

impl Sink for BigQuerySink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<BigQuerySinkWriter>;

    const SINK_NAME: &'static str = BIGQUERY_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(BigQuerySinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Config(anyhow!(
                "BigQuery sink don't support upsert"
            )));
        }

        let client = Client::from_service_account_key_file(&self.config.common.file_path)
            .await
            .map_err(|e| SinkError::BigQuery(e.to_string()))?;
        let mut rs = client
        .job()
        .query(
            &self.config.common.project,
            QueryRequest::new(format!(
                "SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{}'"
                ,self.config.common.project,self.config.common.dataset,self.config.common.table,
            )),
        )
        .await.map_err(|e| SinkError::BigQuery(e.to_string()))?;
        let mut big_query_schema = HashMap::default();
        while rs.next_row() {
            big_query_schema.insert(
                rs.get_string_by_name("column_name")
                    .map_err(|e| SinkError::BigQuery(e.to_string()))?
                    .ok_or_else(|| SinkError::BigQuery("Cannot find column_name".to_owned()))?,
                rs.get_string_by_name("data_type")
                    .map_err(|e| SinkError::BigQuery(e.to_string()))?
                    .ok_or_else(|| SinkError::BigQuery("Cannot find column_name".to_owned()))?,
            );
        }

        self.check_column_name_and_type(big_query_schema)?;
        Ok(())
    }

    fn default_sink_decouple(_desc: &super::catalog::desc::SinkDesc) -> bool {
        false
    }

    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        Err(SinkError::Coordinator(anyhow!("no coordinator")))
    }
}

pub struct BigQuerySinkWriter {
    pub config: BigQueryConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    client: Client,
    is_append_only: bool,
    insert_request: TableDataInsertAllRequest,
    row_encoder: JsonEncoder,
}

impl TryFrom<SinkParam> for BigQuerySink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = BigQueryConfig::from_hashmap(param.properties)?;
        BigQuerySink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl BigQuerySinkWriter {
    pub async fn new(
        config: BigQueryConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = Client::from_service_account_key_file(&config.common.file_path)
            .await
            .map_err(|e| SinkError::BigQuery(e.to_string()))
            .unwrap();
        Ok(Self {
            config,
            schema: schema.clone(),
            pk_indices,
            client,
            is_append_only,
            insert_request: TableDataInsertAllRequest::new(),
            row_encoder: JsonEncoder::new_with_big_query(
                schema,
                None,
                TimestampHandlingMode::String,
            ),
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut insert_vec = vec![];
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            insert_vec.push(TableDataInsertAllRequestRows {
                insert_id: None,
                json: Value::Object(self.row_encoder.encode(row)?),
            })
        }
        self.insert_request
            .add_rows(insert_vec)
            .map_err(|e| SinkError::BigQuery(e.to_string()))?;
        if self.insert_request.len().ge(&BIGQUERY_INSERT_MAX_NUMS) {
            self.insert_data().await?;
        }
        Ok(())
    }

    async fn insert_data(&mut self) -> Result<()> {
        if !self.insert_request.is_empty() {
            let insert_request =
                mem::replace(&mut self.insert_request, TableDataInsertAllRequest::new());
            self.client
                .tabledata()
                .insert_all(
                    &self.config.common.project,
                    &self.config.common.dataset,
                    &self.config.common.table,
                    insert_request,
                )
                .await
                .map_err(|e| SinkError::BigQuery(e.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for BigQuerySinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            Err(SinkError::BigQuery(
                "BigQuery sink don't support upsert".to_string(),
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
        self.insert_data().await
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::types::{DataType, StructType};

    use crate::sink::big_query::BigQuerySink;

    #[tokio::test]
    async fn test_type_check() {
        let big_query_type_string = "ARRAY<STRUCT<v1 ARRAY<INT64>, v2 STRUCT<v1 INT64, v2 INT64>>>";
        let rw_datatype = DataType::List(Box::new(DataType::Struct(StructType::new(vec![
            ("v1".to_owned(), DataType::List(Box::new(DataType::Int64))),
            (
                "v2".to_owned(),
                DataType::Struct(StructType::new(vec![
                    ("v1".to_owned(), DataType::Int64),
                    ("v2".to_owned(), DataType::Int64),
                ])),
            ),
        ]))));
        assert_eq!(
            BigQuerySink::get_string_and_check_support_from_datatype(&rw_datatype).unwrap(),
            big_query_type_string
        );
    }
}
