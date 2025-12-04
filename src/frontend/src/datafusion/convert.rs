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

use datafusion::prelude::Expr as DFExpr;
use datafusion_common::{Column, DFSchema, JoinType as DFJoinType, ScalarValue};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::{DataType as RwDataType, ScalarImpl};
use risingwave_pb::plan_common::JoinType as RwJoinType;

use crate::datafusion::convert_function_call;
use crate::error::Result as RwResult;
use crate::expr::{Expr, ExprImpl};

pub fn convert_expr(expr: &ExprImpl, input_schema: &impl ColumnTrait) -> RwResult<DFExpr> {
    match expr {
        ExprImpl::Literal(lit) => {
            let scalar = match lit.get_data() {
                None => ScalarValue::Null,
                Some(sv) => convert_scalar_value(sv, lit.return_type())?,
            };
            Ok(DFExpr::Literal(scalar))
        }
        ExprImpl::InputRef(input_ref) => Ok(DFExpr::Column(input_schema.column(input_ref.index()))),
        ExprImpl::FunctionCall(func_call) => convert_function_call(func_call, input_schema),
        _ => bail_not_implemented!("DataFusionPlanConverter: unsupported expression {:?}", expr),
    }
}

pub fn convert_scalar_value(sv: &ScalarImpl, data_type: RwDataType) -> RwResult<ScalarValue> {
    match (sv, &data_type) {
        (ScalarImpl::Bool(v), RwDataType::Boolean) => Ok(ScalarValue::Boolean(Some(*v))),
        (ScalarImpl::Int16(v), RwDataType::Int16) => Ok(ScalarValue::Int16(Some(*v))),
        (ScalarImpl::Int32(v), RwDataType::Int32) => Ok(ScalarValue::Int32(Some(*v))),
        (ScalarImpl::Int64(v), RwDataType::Int64) => Ok(ScalarValue::Int64(Some(*v))),
        (ScalarImpl::Float32(v), RwDataType::Float32) => {
            Ok(ScalarValue::Float32(Some(v.into_inner())))
        }
        (ScalarImpl::Float64(v), RwDataType::Float64) => {
            Ok(ScalarValue::Float64(Some(v.into_inner())))
        }
        (ScalarImpl::Utf8(v), RwDataType::Varchar) => Ok(ScalarValue::Utf8(Some(v.to_string()))),
        (ScalarImpl::Bytea(v), RwDataType::Bytea) => Ok(ScalarValue::Binary(Some(v.to_vec()))),
        // For other types, use fallback conversion via IcebergArrowConvert to ensure consistency
        _ => convert_scalar_value_fallback(sv, data_type),
    }
}

fn convert_scalar_value_fallback(sv: &ScalarImpl, data_type: RwDataType) -> RwResult<ScalarValue> {
    let mut array_builder = data_type.create_array_builder(1);
    array_builder.append(Some(sv));
    let array = array_builder.finish();
    let arrow_field = IcebergArrowConvert.to_arrow_field("", &data_type)?;
    let array = IcebergArrowConvert.to_arrow_array(arrow_field.data_type(), &array)?;
    let scalar_value = ScalarValue::try_from_array(&array, 0)?;
    Ok(scalar_value)
}

pub fn convert_join_type(join_type: RwJoinType) -> RwResult<DFJoinType> {
    match join_type {
        RwJoinType::Inner => Ok(DFJoinType::Inner),
        RwJoinType::LeftOuter => Ok(DFJoinType::Left),
        RwJoinType::RightOuter => Ok(DFJoinType::Right),
        RwJoinType::FullOuter => Ok(DFJoinType::Full),
        RwJoinType::LeftSemi => Ok(DFJoinType::LeftSemi),
        RwJoinType::LeftAnti => Ok(DFJoinType::LeftAnti),
        RwJoinType::RightSemi => Ok(DFJoinType::RightSemi),
        RwJoinType::RightAnti => Ok(DFJoinType::RightAnti),
        _ => bail_not_implemented!(
            "DataFusionPlanConverter: unsupported join type {:?}",
            join_type
        ),
    }
}

#[allow(clippy::len_without_is_empty)]
pub trait ColumnTrait {
    fn len(&self) -> usize;
    fn column(&self, index: usize) -> Column;
}

impl ColumnTrait for DFSchema {
    fn len(&self) -> usize {
        self.fields().len()
    }

    fn column(&self, index: usize) -> Column {
        Column::from(self.qualified_field(index))
    }
}

impl ColumnTrait for Vec<Column> {
    fn len(&self) -> usize {
        self.len()
    }

    fn column(&self, index: usize) -> Column {
        self[index].clone()
    }
}
