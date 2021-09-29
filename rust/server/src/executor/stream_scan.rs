use crate::array2::column::Column;
use crate::array2::{ArrayBuilder, ArrayBuilderImpl, DataChunk};
use crate::catalog::TableId;
use crate::error::ErrorCode;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::source::{SourceColumnDesc, SourceMessage, SourceReader};
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::StreamScanNode;
use rust_decimal::Decimal;
use serde_json::Value;
use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::sync::Arc;

const K_STREAM_SCAN_CHUNK_SIZE: usize = 1024;

pub(super) struct StreamScanExecutor {
    reader: Box<dyn SourceReader>,
    columns: Vec<SourceColumnDesc>,
    column_idxes: Vec<usize>,
    done: bool,
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for StreamScanExecutor {
    type Error = RwError;

    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        let stream_scan_node = unpack_from_any!(source.plan_node().get_body(), StreamScanNode);

        let table_id = TableId::from_protobuf(stream_scan_node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let source_column = source
            .global_task_env()
            .source_manager()
            .get_source(&table_id)?;

        let column_idxes = stream_scan_node
            .get_column_ids()
            .iter()
            .map(|id| {
                source_column
                    .columns
                    .iter()
                    .position(|c| c.column_id == *id)
                    .ok_or_else(|| {
                        InternalError(format!(
                            "column id {:?} not found in table {:?}",
                            id, table_id
                        ))
                        .into()
                    })
            })
            .collect::<Result<Vec<usize>>>()?;

        Ok(Self {
            reader: source_column.source.reader()?,
            columns: source_column.columns,
            column_idxes,
            done: false,
        })
    }
}

impl Executor for StreamScanExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        if self.done {
            return Ok(ExecutorResult::Done);
        }

        let columns = self
            .column_idxes
            .iter()
            .map(|idx| self.columns[*idx].clone())
            .collect::<Vec<SourceColumnDesc>>();

        let mut builders = columns
            .iter()
            .map(|k| {
                k.data_type
                    .clone()
                    .create_array_builder(K_STREAM_SCAN_CHUNK_SIZE)
            })
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

        let mut values: Vec<Value> = Vec::with_capacity(K_STREAM_SCAN_CHUNK_SIZE);

        for i in 0..K_STREAM_SCAN_CHUNK_SIZE {
            let next_message = async_std::task::block_on(self.reader.next())?;

            match next_message {
                Some(source_message) => match source_message {
                    SourceMessage::Kafka(kafka_message) => {
                        if let Some(payload) = kafka_message.payload {
                            if let Ok(value) = serde_json::from_slice(payload.as_ref()) {
                                values.push(value)
                            }
                        }
                    }
                    SourceMessage::File(file_message) => {
                        if let Ok(value) = serde_json::from_slice(file_message.data.as_ref()) {
                            values.push(value)
                        }
                    }
                },
                None => {
                    if i == 0 {
                        return Ok(ExecutorResult::Done);
                    } else {
                        self.done = true;
                        break;
                    }
                }
            };
        }

        for value in values.into_iter() {
            for (i, k) in columns.iter().enumerate() {
                let column_name = k.name.clone();
                let value = value.get(column_name);

                match builders[i].borrow_mut() {
                    ArrayBuilderImpl::Int16(b) => {
                        b.append(value.map(|v| v.as_i64()).flatten().map(|v| v as i16))?;
                    }
                    ArrayBuilderImpl::Int32(b) => {
                        b.append(value.map(|v| v.as_i64()).flatten().map(|v| v as i32))?;
                    }
                    ArrayBuilderImpl::Int64(b) => {
                        b.append(value.map(|v| v.as_i64()).flatten())?;
                    }
                    ArrayBuilderImpl::Float32(b) => {
                        b.append(value.map(|v| v.as_f64()).flatten().map(|v| v as f32))?;
                    }
                    ArrayBuilderImpl::Float64(b) => {
                        b.append(value.map(|v| v.as_f64()).flatten())?;
                    }
                    ArrayBuilderImpl::Bool(b) => {
                        b.append(value.map(|v| v.as_bool()).flatten())?;
                    }
                    ArrayBuilderImpl::Decimal(b) => {
                        b.append(value.map(|v| v.as_u64()).flatten().map(Decimal::from))?;
                    }
                    ArrayBuilderImpl::Interval(_) => {
                        unimplemented!()
                    }
                    ArrayBuilderImpl::UTF8(b) => {
                        b.append(value.map(|v| v.as_str()).flatten())?;
                    }
                }
            }
        }

        let columns = builders
            .into_iter()
            .zip(columns.iter().map(|c| c.data_type.clone()))
            .map(|(builder, data_type)| {
                builder
                    .finish()
                    .map(|arr| Column::new(Arc::new(arr), data_type.clone()))
            })
            .collect::<Result<Vec<Column>>>()?;

        let ret = DataChunk::builder().columns(columns).build();

        Ok(ExecutorResult::Batch(Arc::new(ret)))
    }
    fn clean(&mut self) -> Result<()> {
        async_std::task::block_on(self.reader.cancel())
    }
}
