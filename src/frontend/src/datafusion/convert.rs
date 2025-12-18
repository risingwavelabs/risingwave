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

use datafusion::arrow::datatypes::DataType as DFDataType;
use datafusion::prelude::Expr as DFExpr;
use datafusion_common::{Column, DFSchema, JoinType as DFJoinType, ScalarValue};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::Schema as RwSchema;
use risingwave_common::types::{DataType as RwDataType, ScalarImpl};
use risingwave_pb::plan_common::JoinType as RwJoinType;

use crate::datafusion::convert_function_call;
use crate::error::Result as RwResult;
use crate::expr::{Expr, ExprImpl};

pub fn convert_expr(expr: &ExprImpl, input_columns: &impl ColumnTrait) -> RwResult<DFExpr> {
    match expr {
        ExprImpl::Literal(lit) => {
            let scalar = match lit.get_data() {
                None => ScalarValue::Null,
                Some(sv) => convert_scalar_value(sv, lit.return_type())?,
            };
            Ok(DFExpr::Literal(scalar, None))
        }
        ExprImpl::InputRef(input_ref) => {
            Ok(DFExpr::Column(input_columns.column(input_ref.index())))
        }
        ExprImpl::FunctionCall(func_call) => convert_function_call(func_call, input_columns),
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

pub trait ColumnTrait {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn len(&self) -> usize;
    fn column(&self, index: usize) -> Column;
    fn rw_data_type(&self, index: usize) -> RwDataType;
    fn df_data_type(&self, index: usize) -> DFDataType;
}

pub struct InputColumns<'a> {
    df_schema: &'a DFSchema,
    rw_schema: &'a RwSchema,
}

impl<'a> InputColumns<'a> {
    pub fn new(df_schema: &'a DFSchema, rw_schema: &'a RwSchema) -> Self {
        Self {
            df_schema,
            rw_schema,
        }
    }
}

impl<'a> ColumnTrait for InputColumns<'a> {
    fn len(&self) -> usize {
        self.df_schema.fields().len()
    }

    fn column(&self, index: usize) -> Column {
        Column::from(self.df_schema.qualified_field(index))
    }

    fn rw_data_type(&self, index: usize) -> RwDataType {
        let field = &self.rw_schema.fields[index];
        field.data_type()
    }

    fn df_data_type(&self, index: usize) -> DFDataType {
        self.df_schema.field(index).data_type().clone()
    }
}

pub struct ConcatColumns<'a> {
    df_left: &'a DFSchema,
    rw_left: &'a RwSchema,
    df_right: &'a DFSchema,
    rw_right: &'a RwSchema,
    left_len: usize,
}

impl<'a> ConcatColumns<'a> {
    pub fn new(
        df_left: &'a DFSchema,
        rw_left: &'a RwSchema,
        df_right: &'a DFSchema,
        rw_right: &'a RwSchema,
    ) -> Self {
        Self {
            df_left,
            rw_left,
            df_right,
            rw_right,
            left_len: df_left.fields().len(),
        }
    }
}

impl ColumnTrait for ConcatColumns<'_> {
    fn len(&self) -> usize {
        self.df_left.fields().len() + self.df_right.fields().len()
    }

    fn column(&self, index: usize) -> Column {
        if index < self.left_len {
            Column::from(self.df_left.qualified_field(index))
        } else {
            Column::from(self.df_right.qualified_field(index - self.left_len))
        }
    }

    fn rw_data_type(&self, index: usize) -> RwDataType {
        if index < self.left_len {
            let field = &self.rw_left.fields[index];
            field.data_type()
        } else {
            let field = &self.rw_right.fields[index - self.left_len];
            field.data_type()
        }
    }

    fn df_data_type(&self, index: usize) -> DFDataType {
        if index < self.left_len {
            self.df_left.field(index).data_type().clone()
        } else {
            self.df_right
                .field(index - self.left_len)
                .data_type()
                .clone()
        }
    }
}
