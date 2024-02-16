// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use arrow_array::{ArrayRef, StructArray};
use arrow_schema::DataType;
use itertools::Itertools;

use crate::array::{ArrayError, DataChunk};
use crate::util::iter_util::ZipEqFast;

/// Converts RisingWave array to Arrow array with the schema.
/// The behavior is specified for iceberg:
/// For different struct type, try to use fields in schema to cast.
pub fn to_iceberg_record_batch_with_schema(
    schema: arrow_schema::SchemaRef,
    chunk: &DataChunk,
) -> Result<arrow_array::RecordBatch, ArrayError> {
    if !chunk.is_compacted() {
        let c = chunk.clone();
        return to_iceberg_record_batch_with_schema(schema, &c.compact());
    }
    let columns: Vec<_> = chunk
        .columns()
        .iter()
        .zip_eq_fast(schema.fields().iter())
        .map(|(column, field)| {
            let column: arrow_array::ArrayRef = column.as_ref().try_into()?;
            if column.data_type() == field.data_type() {
                Ok(column)
            } else if let DataType::Struct(actual) = column.data_type()
                && let DataType::Struct(expect) = field.data_type()
            {
                // Special case for iceberg
                if actual.len() != expect.len() {
                    return Err(ArrayError::to_arrow(format!(
                        "Struct field count mismatch, expect {}, actual {}",
                        expect.len(),
                        actual.len()
                    )));
                }
                let column = column
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .unwrap()
                    .clone();
                let (_, struct_columns, nullable) = column.into_parts();
                Ok(Arc::new(
                    StructArray::try_new(expect.clone(), struct_columns, nullable)
                        .map_err(ArrayError::from_arrow)?,
                ) as ArrayRef)
            } else {
                arrow_cast::cast(&column, field.data_type()).map_err(ArrayError::from_arrow)
            }
        })
        .try_collect::<_, _, ArrayError>()?;

    let opts = arrow_array::RecordBatchOptions::default().with_row_count(Some(chunk.capacity()));
    arrow_array::RecordBatch::try_new_with_options(schema, columns, &opts)
        .map_err(ArrayError::to_arrow)
}
