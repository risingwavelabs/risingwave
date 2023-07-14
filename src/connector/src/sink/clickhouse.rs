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

use crate::sink::SinkWriterV1Adapter;
use anyhow::anyhow;
use clickhouse::update::Field;
use clickhouse::Client;
use itertools::Itertools;
use risingwave_common::row::Row;
use risingwave_common::types::{ScalarRefImpl, DatumRef, ToText};
use risingwave_rpc_client::ConnectorClient;
use crate::common::ClickHouseCommon;
use crate::source::DataType;
use serde_with::serde_as;
use serde_derive::{Deserialize};
use crate::sink::{
    Result, SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM, SINK_TYPE_OPTION,
    SINK_TYPE_UPSERT,
};
use crate::sink::Sink;
use risingwave_common::array::{StreamChunk, RowRef, Op};
use risingwave_common::catalog::Schema;
use core::fmt::Debug;
use clickhouse::Row as ClickHouseRow;
use bytes::{BytesMut, BufMut};
use crate::sink::SinkWriterV1;

use super::{SinkWriterParam, DummySinkCommitCoordinator};

pub const CLICKHOUSE_SINK: &str = "clickhouse";


#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct ClickHouseConfig {
    #[serde(flatten)]
    pub common: ClickHouseCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

#[derive(Clone)]
pub struct ClickHouseSink {
    pub config: ClickHouseConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl ClickHouseConfig{
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<ClickHouseConfig>(serde_json::to_value(properties).unwrap()).map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY
            && config.r#type != SINK_TYPE_DEBEZIUM
            && config.r#type != SINK_TYPE_UPSERT
        {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_DEBEZIUM,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

impl ClickHouseSink {
    pub fn new(
            config: ClickHouseConfig,
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

    ///Check that the column names and types of risingwave and clickhouse are identical
    fn check_column_name_and_type(&mut self,clickhouse_column: Vec<SystemColumn>) -> Result<()>{
        let fields = self.schema.fields().clone();
        let vec:Result<()>  = fields.iter().zip(clickhouse_column).map(|(key,value)|{
            assert_eq!(key.name,value.name,"name is not match. name is {:?} and {:?}",key.name,value.name);
            self.check_and_correct_column_type(&key.data_type,&value)
        }).collect();
        vec
    }

    ///Check that the column types of risingwave and clickhouse are identical
    fn check_and_correct_column_type(&self,fields_type: &DataType, ck_column: &SystemColumn) -> Result<()>{
        let is_match = match fields_type {
            risingwave_common::types::DataType::Boolean => Ok(ck_column.r#type.contains("Bool")),
            risingwave_common::types::DataType::Int16 => Ok(ck_column.r#type.contains("UInt16") | ck_column.r#type.contains("Int16")),
            risingwave_common::types::DataType::Int32 => Ok(ck_column.r#type.contains("UInt32") | ck_column.r#type.contains("Int32")),
            risingwave_common::types::DataType::Int64 => Ok(ck_column.r#type.contains("UInt64") | ck_column.r#type.contains("Int64")),
            risingwave_common::types::DataType::Float32 => Ok(ck_column.r#type.contains("Float32")),
            risingwave_common::types::DataType::Float64 => Ok(ck_column.r#type.contains("Float64")),
            risingwave_common::types::DataType::Decimal => todo!(),
            risingwave_common::types::DataType::Date => Ok(ck_column.r#type.contains("Date32")),
            risingwave_common::types::DataType::Varchar => Ok(ck_column.r#type.contains("String")),
            risingwave_common::types::DataType::Time => Err(SinkError::ClickHouse("clickhouse can not support Time".to_string())),
            risingwave_common::types::DataType::Timestamp => Ok(ck_column.r#type.contains("DateTime64")),
            risingwave_common::types::DataType::Timestamptz => Err(SinkError::ClickHouse("clickhouse can not support Timestamptz".to_string())),
            risingwave_common::types::DataType::Interval => Err(SinkError::ClickHouse("clickhouse can not support Interval".to_string())),
            risingwave_common::types::DataType::Struct(_) => todo!(),
            risingwave_common::types::DataType::List(list) => {
                self.check_and_correct_column_type(list.as_ref(), ck_column)?;
                Ok(ck_column.r#type.contains("Array"))
            },
            risingwave_common::types::DataType::Bytea => Err(SinkError::ClickHouse("clickhouse can not support Bytea".to_string())),
            risingwave_common::types::DataType::Jsonb => todo!(),
            risingwave_common::types::DataType::Serial => Ok(ck_column.r#type.contains("UInt64") | ck_column.r#type.contains("Int64")),
            risingwave_common::types::DataType::Int256 => Err(SinkError::ClickHouse("clickhouse can not support Interval".to_string())),
        };
        if !is_match? {
            return Err(SinkError::ClickHouse(format!("type can not match name is {:?}, type is{:?} and {:?}",ck_column.name,fields_type,ck_column.r#type)));
        }
        
        Ok(())
    }


}
#[async_trait::async_trait]
impl Sink for ClickHouseSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = SinkWriterV1Adapter<ClickHouseSinkWriter>;
    async fn validate(&mut self, _client: Option<ConnectorClient>) -> Result<()> {
        // For upsert clickhouse sink, the primary key must be defined.
            if !self.is_append_only && self.pk_indices.is_empty() {
                return Err(SinkError::Config(anyhow!(
                    "primary key not defined for {} kafka sink (please define in `primary_key` field)",
                    self.config.r#type
                )));
            }

            // check reachability
            let client = self.config.common.build_client().await?;
            let query_column = format!("select distinct ?fields from system.columns where database = ? and table = ? order by ?");
            let clickhouse_column = client.query(&query_column).bind(self.config.common.database.clone()).bind(self.config.common.table.clone()).bind("position").fetch_all::<SystemColumn>().await.map_err(|e|SinkError::ClickHouse(e.to_string()))?;
            assert!(!clickhouse_column.is_empty(),"table {:?}.{:?} is not find in clickhouse",self.config.common.database,self.config.common.table);
            self.check_column_name_and_type(clickhouse_column)?;
            Ok(())
        }

    async fn new_writer(&self, _writer_env: SinkWriterParam) -> Result<Self::Writer> {
        Ok(SinkWriterV1Adapter::new(
            ClickHouseSinkWriter::new(
                self.config.clone(),
                self.schema.clone(),
                self.pk_indices.clone(),
                self.is_append_only,
            )
            .await?,
        ))
        
    }
}
pub struct ClickHouseSinkWriter{
    pub config: ClickHouseConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    client: Client,
    // Save the data to be inserted
    buffer: BytesMut,
    is_append_only: bool,
    // Save some features of the clickhouse column type
    column_correct_vec: Vec<(bool,u8)>
}

impl ClickHouseSinkWriter{
    pub async fn new(
        config: ClickHouseConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self>{
        let client = config
                .common
                .build_client()
                .await
                .map_err(|e|SinkError::ClickHouse(e.to_string()))?;
        let buffer_size = client.get_buffer_size();
        let client = config.common.build_client().await?;
        let query_column = format!("select distinct ?fields from system.columns where database = ? and table = ? order by position");
        let clickhouse_column = client.query(&query_column).bind(config.common.database.clone()).bind(config.common.table.clone()).fetch_all::<SystemColumn>().await.map_err(|e|SinkError::ClickHouse(e.to_string()))?;
        let column_correct_vec:Result<Vec<(bool,u8)>> = clickhouse_column.iter().map(Self::build_column_correct_vec).collect();
        Ok(Self{
            config,
            schema,
            pk_indices,
            client,
            buffer: BytesMut::with_capacity(buffer_size),
            is_append_only,
            column_correct_vec: column_correct_vec?,
        })
    }

    /// Check if clickhouse's column is 'Nullable', valid bits of 'DateTime64'. And save it in 'column_correct_vec'
    fn build_column_correct_vec(ck_column: &SystemColumn) -> Result<(bool,u8)>{
        let can_null = ck_column.r#type.contains("Nullable");
        let accuracy_time = if ck_column.r#type.contains("DateTime64("){
            ck_column.r#type.split("DateTime64(").last().ok_or(SinkError::ClickHouse("must have last".to_string()))?.split(")").next().ok_or(SinkError::ClickHouse("must have next".to_string()))?.parse::<u8>().map_err(|e| SinkError::ClickHouse(e.to_string()))?
        }else{
            0 as u8
        };
        Ok((can_null,accuracy_time))
    }
    
    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let file_name = self.schema.fields().iter().map(|f|{f.name.clone()}).collect_vec();
        let mut inter = self.client.insert_with_filed::<Rows>(&self.config.common.table,Some(file_name)).map_err(|e|SinkError::ClickHouse(e.to_string()))?;
        for (op,row) in chunk.rows(){
            if op != Op::Insert {
                continue;
            }
            self.build_row_binary(row)?;
            let buffer = mem::replace(&mut self.buffer,BytesMut::with_capacity(self.client.get_buffer_size()));
            inter.write_row_binary(buffer).await.map_err(|e|SinkError::ClickHouse(e.to_string()))?;
        }
        inter.end().await.map_err(|e|SinkError::ClickHouse(e.to_string()))?;
        Ok(())
    }

    /// Write row data in 'buffer'
    fn build_row_binary(&mut self, row : RowRef<'_>) -> Result<()>{
        for (index , row) in row.iter().enumerate(){
            self.build_data_binary(index,row)?
        }
        Ok(())
    }


    fn build_data_binary(&mut self,index: usize,data: Option<ScalarRefImpl<'_>>) -> Result<()>{
        let &(can_null,accuracy_time) = self.column_correct_vec.get(index).unwrap();
        match data{
            Some(ScalarRefImpl::Int16(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                self.buffer.put_i16_le(v)
            },
            Some(ScalarRefImpl::Int32(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                self.buffer.put_i32_le(v)},
            Some(ScalarRefImpl::Int64(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                self.buffer.put_i64_le(v)
            },
            Some(ScalarRefImpl::Int256(_v)) => return Err(SinkError::ClickHouse("clickhouse can not support Int256".to_string())),
            Some(ScalarRefImpl::Serial(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                self.buffer.put_i64_le(v.into_inner())
            },
            Some(ScalarRefImpl::Float32(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                self.buffer.put_f32_le(v.into_inner())},
            Some(ScalarRefImpl::Float64(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                self.buffer.put_f64_le(v.into_inner())},
            Some(ScalarRefImpl::Utf8(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                Self::put_unsigned_leb128(&mut self.buffer, v.len() as u64);
                self.buffer.put_slice(v.as_bytes());
            },
            Some(ScalarRefImpl::Bool(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                self.buffer.put_u8(v as _)},
            Some(ScalarRefImpl::Decimal(_v)) => todo!(),
            Some(ScalarRefImpl::Interval(_v)) => return Err(SinkError::ClickHouse("clickhouse can not support Interval".to_string())),
            Some(ScalarRefImpl::Date(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                
                let days = v.get_nums_days_unix_epoch();
                println!("{:?}",days);
                self.buffer.put_i32_le(days);
            },
            Some(ScalarRefImpl::Time(_v)) => return Err(SinkError::ClickHouse("clickhouse can not support Time".to_string())),
            Some(ScalarRefImpl::Timestamp(v)) => {
                if can_null{
                    self.buffer.put_u8(0);
                }
                let time = v.get_timestamp_nsecs() / 10_i32.pow((9 - accuracy_time).into()) as i64;
                self.buffer.put_i64_le(time);
            },
            Some(ScalarRefImpl::Timestamptz(_v)) => return Err(SinkError::ClickHouse("clickhouse can not support Timestamptz".to_string())),
            Some(ScalarRefImpl::Jsonb(_v)) => todo!(),
            Some(ScalarRefImpl::Struct(_v)) => todo!(),
            Some(ScalarRefImpl::List(v)) => {
                Self::put_unsigned_leb128(&mut self.buffer, v.len() as u64);
                for data in v.iter(){
                    self.build_data_binary(index,data)?;
                }
            },
            Some(ScalarRefImpl::Bytea(_v)) => return Err(SinkError::ClickHouse("clickhouse can not support Bytea".to_string())),
            None => {
                if can_null{
                    self.buffer.put_u8(1);
                }else{
                    return Err(SinkError::ClickHouse(format!("clickhouse column can not insert null")))
                }
            },
        };
        Ok(())
    }
    fn put_unsigned_leb128(mut buffer: impl BufMut, mut value: u64) {
        while {
            let mut byte = value as u8 & 0x7f;
            value >>= 7;

            if value != 0 {
                byte |= 0x80;
            }

            buffer.put_u8(byte);

            value != 0
        } {}
    }

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        // Get field names from schema.
        let field_names = self.schema.fields().iter().map(|f|{f.name.clone()}).collect_vec();
        let pk_index = *self.pk_indices.get(0).unwrap();
        let pk_name_0 = field_names.get(pk_index).unwrap();

        // Get the names of the columns excluding pk, and use them to update.
        let field_names_update = field_names.iter().enumerate().filter(|(index,_)|{!self.pk_indices.contains(index)}).map(|(_,value)|{value.clone()}).collect_vec();
        let update: clickhouse::update::Update = self.client.update(&self.config.common.table, pk_name_0, field_names_update);
        for (index , (op,row)) in chunk.rows().enumerate(){
            let &(_,accuracy_time) = self.column_correct_vec.get(index).unwrap();
            match op{
                Op::Insert => {
                    let mut inter = self.client.insert_with_filed::<Rows>(&self.config.common.table,Some(field_names.clone())).map_err(|e|SinkError::ClickHouse(e.to_string()))?;
                    self.build_row_binary(row)?;
                    let buffer = mem::replace(&mut self.buffer,BytesMut::with_capacity(self.client.get_buffer_size()));
                    inter.write_row_binary(buffer).await.map_err(|e|SinkError::ClickHouse(e.to_string()))?;
                    inter.end().await.map_err(|e|SinkError::ClickHouse(e.to_string()))?;
                },
                Op::Delete => {
                    match Self::build_ck_fields(row.datum_at(index), accuracy_time)? {
                        Some(f) => {
                            self.client.delete(&self.config.common.table, pk_name_0, vec![f]).delete().await.map_err(|e|SinkError::ClickHouse(e.to_string()))?;
                        }
                        None => return Err(SinkError::ClickHouse(format!("pk can not null")))
                    }
                },
                Op::UpdateDelete => continue,
                Op::UpdateInsert => {
                    let pk = Self::build_ck_fields(row.datum_at(pk_index),accuracy_time)?.ok_or(SinkError::ClickHouse(format!("pk can not none")))?;
                    let fields_vec = self.build_update_fields(row, accuracy_time)?;
                    update.clone().update_fields(fields_vec,pk).await.map_err(|e|SinkError::ClickHouse(e.to_string()))?;
                },
            }
        }
        Ok(())
    }

    /// Get the data excluding pk.
    fn build_update_fields(&self,row : RowRef<'_>,accuracy_time: u8) -> Result<Vec<Field>> {
        let mut vec = vec![];
        for (index,data) in row.iter().enumerate(){
            if self.pk_indices.contains(&index){
                continue;
            }
            match Self::build_ck_fields(data, accuracy_time)? { 
                Some(f) => {
                    vec.push(f);
                },
                None => todo!(),
            }
        }
        Ok(vec)
    }

    /// Convert data to the type provided by the clickhouse interface. It is not possible to convert risingwave's list to Vec<Field>, 
    /// so to use Customize, we need to serialize it ourselves
    fn build_ck_fields(data: DatumRef<'_>,accuracy_time: u8) -> Result<Option<Field>>{
        let date = match data {
            Some(ScalarRefImpl::Int16(v)) => Some(Field::I16(v)),
            Some(ScalarRefImpl::Int32(v)) => Some(Field::I32(v)),
            Some(ScalarRefImpl::Int64(v)) => Some(Field::I64(v)),
            Some(ScalarRefImpl::Int256(_)) => return Err(SinkError::ClickHouse(format!("clickhouse can not support Int256"))),
            Some(ScalarRefImpl::Serial(v)) => Some(Field::I64(v.into_inner())),
            Some(ScalarRefImpl::Float32(v)) => Some(Field::F32(v.into_inner())),
            Some(ScalarRefImpl::Float64(v)) => Some(Field::F64(v.into_inner())),
            Some(ScalarRefImpl::Utf8(v)) => Some(Field::String(v.to_string())),
            Some(ScalarRefImpl::Bool(v)) => Some(Field::Bool(v)),
            Some(ScalarRefImpl::Decimal(_)) => todo!(),
            Some(ScalarRefImpl::Interval(_)) => return Err(SinkError::ClickHouse(format!("clickhouse can not support Interval"))),
            Some(ScalarRefImpl::Date(v)) => {
                let days = v.get_nums_days_unix_epoch();
                Some(Field::Date32(days))},
            Some(ScalarRefImpl::Time(_)) => return Err(SinkError::ClickHouse(format!("clickhouse can not support Time"))),
            Some(ScalarRefImpl::Timestamp(v)) => {
                let time = v.get_timestamp_nsecs() / 10_i32.pow((9 - accuracy_time).into()) as i64;
                Some(Field::DateTime64(time))},
            Some(ScalarRefImpl::Timestamptz(_)) => return Err(SinkError::ClickHouse(format!("clickhouse can not support Timestamptz"))),
            Some(ScalarRefImpl::Jsonb(_)) => todo!(),
            Some(ScalarRefImpl::Struct(_)) => todo!(),
            Some(ScalarRefImpl::List(v)) => Some(Field::Customize(v.to_text().replace("{", "[").replace("}", "]"))),
            Some(ScalarRefImpl::Bytea(_)) => return Err(SinkError::ClickHouse(format!("clickhouse can not support Bytea"))),
            None => None,
        };
        Ok(date)
    }
    
}
#[async_trait::async_trait]
impl SinkWriterV1 for ClickHouseSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.append_only(chunk).await
        } else if self.config.r#type == SINK_TYPE_DEBEZIUM {
            unreachable!()
        } else if self.config.r#type == SINK_TYPE_UPSERT {
            self.upsert(chunk).await
        } else {
            unreachable!()
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        // clickhouse no transactional guarantees, so we do nothing here.
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
    async fn commit(&mut self) -> Result<()>{
        Ok(())
    }

}
#[derive(ClickHouseRow)]
struct Rows{
}

#[derive(ClickHouseRow,Deserialize)]
struct SystemColumn{
    name: String,
    r#type: String,
}
