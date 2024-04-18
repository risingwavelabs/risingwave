use std::str::FromStr;

use async_trait::async_trait;
use futures_async_stream::try_stream;
use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::{ArrayImpl, DataChunk, StreamChunk};
use risingwave_common::catalog::{is_row_id_column_name, RW_RESERVED_COLUMN_NAME_PREFIX};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, Serial};
use risingwave_expr::table_function::{TableFunction, UserDefinedTableFunction};
use risingwave_expr::ExprError;

use super::split::UdfSplit;
use super::{OperationName, UdfProperties};
use crate::connector_common::nested_fields::{
    get_string_field_index_path_from_columns, get_string_from_index_path,
};
use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::{BoxChunkSourceStream, Column, SourceContextRef, SplitId, SplitReader};

pub struct UdfSplitReader {
    table_function: UserDefinedTableFunction,
    chunk_size: usize,
    split_id: SplitId,
    operation_field_path: Option<Vec<usize>>,
    offset_field_path: Option<Vec<usize>>,
    rowid_idx: Option<usize>,
    offset_idx: Option<usize>,
    split_idx: Option<usize>,
    additional_columns_types: Vec<DataType>,
}

impl UdfSplitReader {
    #[try_stream(boxed, ok = StreamChunk,  error = crate::error::ConnectorError)]
    async fn do_execute(self) {
        let dummy_chunk = DataChunk::new_dummy(1);

        let types: Vec<DataType> = match self.table_function.return_type() {
            DataType::Struct(st) => st
                .types()
                .cloned()
                .chain(self.additional_columns_types.into_iter())
                .collect(),
            t => std::iter::once(t)
                .chain(self.additional_columns_types.into_iter())
                .collect(),
        };

        let mut global_offset: i64 = 0;
        let mut wait_for_next = false;
        let mut row = vec![None as DatumRef<'_>; types.len()];
        let mut builder = StreamChunkBuilder::new(self.chunk_size, types);

        #[for_await]
        for chunk in self.table_function.eval(&dummy_chunk).await {
            let chunk = chunk?;

            let chunk = match chunk.column_at(1).as_ref() {
                ArrayImpl::Struct(struct_array) => struct_array.into(),
                _ => chunk.split_column_at(1).1,
            };

            let split_id: &str = self.split_id.as_ref();

            for i in 0..chunk.capacity() {
                let (orig_row, _) = chunk.row_at(i);
                let row: &mut [DatumRef<'_>] = unsafe { std::mem::transmute(row.as_mut_slice()) };

                let operation = if let Some(path) = &self.operation_field_path {
                    get_string_from_index_path(path, None, &orig_row)
                        .and_then(|s| OperationName::from_str(s).ok())
                        .unwrap_or(OperationName::Insert)
                } else {
                    OperationName::Insert
                };

                let offset = if let Some(path) = &self.offset_field_path {
                    get_string_from_index_path(path, None, &orig_row)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| global_offset.to_string())
                } else {
                    global_offset.to_string()
                };

                let mut valid = false;
                orig_row.iter().enumerate().for_each(|(idx, datum)| {
                    if datum.is_some() {
                        valid = true;
                    }
                    row[idx] = datum;
                });

                if valid {
                    if wait_for_next && !matches!(operation, OperationName::UpdateInsert) {
                        return Err(ExprError::InvalidState(
                            "An U+ operation is expected after an U- operation".to_string(),
                        )
                        .into());
                    }

                    wait_for_next = matches!(operation, OperationName::UpdateDelete);
                    // Sets the row id
                    if let Some(rowid_idx) = self.rowid_idx {
                        row[rowid_idx] = Some(Serial::from(i as i64).into());
                    }

                    if let Some(split_idx) = self.split_idx {
                        row[split_idx] = Some(split_id.into());
                    }

                    if let Some(offset_idx) = self.offset_idx {
                        row[offset_idx] = Some(offset.as_str().into());
                    }

                    // Sets the offset id

                    if let Some(chunk) = builder.append_row(operation.into(), &*row) {
                        yield chunk;
                    }
                    global_offset += 1;
                }
            }

            if !wait_for_next {
                if let Some(chunk) = builder.take() {
                    yield chunk;
                }
            }
        }
    }
}

#[async_trait]
impl SplitReader for UdfSplitReader {
    type Properties = UdfProperties;
    type Split = UdfSplit;

    async fn new(
        properties: UdfProperties,
        splits: Vec<UdfSplit>,
        _parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        assert!(splits.len() == 1);
        let mut operation_field_path = None;
        let mut offset_field_path = None;
        let mut rowid_idx = None;
        let mut offset_idx = None;
        let mut split_idx = None;
        let mut additional_columns_types = Vec::new();
        if let Some(columns) = columns {
            for (i, column) in columns.iter().enumerate() {
                if is_row_id_column_name(&column.name) {
                    rowid_idx = Some(i);
                    additional_columns_types.push(DataType::Serial);
                    continue;
                }

                if column.name.starts_with(RW_RESERVED_COLUMN_NAME_PREFIX) {
                    if column.name.ends_with("offset") {
                        offset_idx = Some(i);
                    } else if column.name.ends_with("partition") {
                        split_idx = Some(i);
                    }

                    additional_columns_types.push(column.data_type.clone());
                }
            }

            if let Some(field) = properties.operation_field.as_deref() {
                operation_field_path =
                    Some(get_string_field_index_path_from_columns(&columns, field)?);
            }

            if let Some(field) = properties.offset_field.as_deref() {
                offset_field_path =
                    Some(get_string_field_index_path_from_columns(&columns, field)?);
            }
        }

        let split = splits.first().unwrap();

        Ok(Self {
            table_function: UserDefinedTableFunction::build_from_prost(
                split.expr.clone(),
                source_ctx.source_ctrl_opts.chunk_size,
            )?,
            chunk_size: source_ctx.source_ctrl_opts.chunk_size,
            split_id: split.split_id.clone(),
            offset_field_path,
            operation_field_path,
            rowid_idx,
            offset_idx,
            split_idx,
            additional_columns_types,
        })
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        self.do_execute()
    }
}
