// Copyright 2025 RisingWave Labs
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

use datafusion::arrow::datatypes::Field;
use datafusion_common::DFSchema;
use datafusion_common::arrow::datatypes::DataType as DFDataType;
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::Schema as RwSchema;
use risingwave_common::types::DataType as RwDataType;
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_expr::expr::{BoxedExpression, build_from_prost};

use crate::error::{ErrorCode, Result as RwResult};
use crate::expr::{Expr, ExprImpl, InputRef};

#[derive(Debug)]
pub struct CastExecutor {
    executors: Vec<Option<BoxedExpression>>,
}

impl CastExecutor {
    pub fn new(df_schema: &DFSchema, rw_schema: &RwSchema) -> RwResult<Self> {
        let mut executors = Vec::with_capacity(df_schema.fields().len());
        for (i, (df_field, rw_field)) in df_schema
            .fields()
            .iter()
            .zip_eq_fast(rw_schema.fields().iter())
            .enumerate()
        {
            let target_type = rw_field.data_type();
            let source_type = IcebergArrowConvert.type_from_field(df_field)?;

            if source_type == target_type {
                executors.push(None);
            } else {
                let cast_executor = build_single_cast_executor(i, source_type, target_type)?;
                executors.push(Some(cast_executor));
            }
        }
        Ok(CastExecutor { executors })
    }

    pub fn from_iter(
        source_types: impl Iterator<Item = DFDataType>,
        target_types: impl Iterator<Item = RwDataType>,
    ) -> RwResult<Self> {
        let mut executors = Vec::new();
        for (i, (source_type, target_type)) in source_types.zip_eq_debug(target_types).enumerate() {
            let source_type =
                IcebergArrowConvert.type_from_field(&Field::new("", source_type.clone(), true))?;

            if source_type == target_type {
                executors.push(None);
            } else {
                let cast_executor = build_single_cast_executor(i, source_type, target_type)?;
                executors.push(Some(cast_executor));
            }
        }
        Ok(CastExecutor { executors })
    }

    pub async fn execute(&self, chunk: DataChunk) -> RwResult<DataChunk> {
        let mut arrays = Vec::with_capacity(chunk.columns().len());
        for (exe, col) in self.executors.iter().zip_eq_fast(chunk.columns()) {
            if let Some(exe) = exe {
                arrays.push(exe.eval(&chunk).await?);
            } else {
                arrays.push(col.clone());
            }
        }
        Ok(DataChunk::new(arrays, chunk.into_parts_v2().1))
    }
}

fn build_single_cast_executor(
    idx: usize,
    source_type: RwDataType,
    target_type: RwDataType,
) -> RwResult<BoxedExpression> {
    let expr: ExprImpl = InputRef::new(idx, source_type).into();
    let expr = expr.cast_explicit(&target_type)?;
    let res = build_from_prost(
        &expr
            .try_to_expr_proto()
            .map_err(ErrorCode::InvalidInputSyntax)?,
    )?;
    Ok(res)
}
