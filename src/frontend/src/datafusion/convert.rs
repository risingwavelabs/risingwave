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

use std::sync::Arc;

use datafusion::arrow::array::ArrayRef as DFArrayRef;
use datafusion::arrow::datatypes::DataType as DFDataType;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::Expr as DFExpr;
use datafusion_common::{Column, DFSchema, JoinType as DFJoinType, ScalarValue};
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bail_not_implemented;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema as RwSchema;
use risingwave_common::types::{DataType as RwDataType, ScalarImpl};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::plan_common::JoinType as RwJoinType;

use crate::datafusion::convert_function_call;
use crate::error::Result as RwResult;
use crate::expr::{Expr, ExprImpl};

pub fn convert_expr(expr: &ExprImpl, input_columns: &impl ColumnTrait) -> RwResult<DFExpr> {
    match expr {
        ExprImpl::Literal(lit) => {
            let scalar = convert_scalar_value(lit.get_data(), lit.return_type())?;
            Ok(DFExpr::Literal(scalar, None))
        }
        ExprImpl::InputRef(input_ref) => {
            Ok(DFExpr::Column(input_columns.column(input_ref.index())))
        }
        ExprImpl::FunctionCall(func_call) => convert_function_call(func_call, input_columns),
        _ => bail_not_implemented!("DataFusionPlanConverter: unsupported expression {:?}", expr),
    }
}

pub fn convert_scalar_value(
    sv: &Option<ScalarImpl>,
    data_type: RwDataType,
) -> RwResult<ScalarValue> {
    match (sv, &data_type) {
        (Some(ScalarImpl::Bool(v)), RwDataType::Boolean) => Ok(ScalarValue::Boolean(Some(*v))),
        (None, RwDataType::Boolean) => Ok(ScalarValue::Boolean(None)),
        (Some(ScalarImpl::Int16(v)), RwDataType::Int16) => Ok(ScalarValue::Int16(Some(*v))),
        (None, RwDataType::Int16) => Ok(ScalarValue::Int16(None)),
        (Some(ScalarImpl::Int32(v)), RwDataType::Int32) => Ok(ScalarValue::Int32(Some(*v))),
        (None, RwDataType::Int32) => Ok(ScalarValue::Int32(None)),
        (Some(ScalarImpl::Int64(v)), RwDataType::Int64) => Ok(ScalarValue::Int64(Some(*v))),
        (None, RwDataType::Int64) => Ok(ScalarValue::Int64(None)),
        (Some(ScalarImpl::Float32(v)), RwDataType::Float32) => {
            Ok(ScalarValue::Float32(Some(v.into_inner())))
        }
        (None, RwDataType::Float32) => Ok(ScalarValue::Float32(None)),
        (Some(ScalarImpl::Float64(v)), RwDataType::Float64) => {
            Ok(ScalarValue::Float64(Some(v.into_inner())))
        }
        (None, RwDataType::Float64) => Ok(ScalarValue::Float64(None)),
        (Some(ScalarImpl::Utf8(v)), RwDataType::Varchar) => {
            Ok(ScalarValue::Utf8(Some(v.to_string())))
        }
        (None, RwDataType::Varchar) => Ok(ScalarValue::Utf8(None)),
        (Some(ScalarImpl::Bytea(v)), RwDataType::Bytea) => {
            Ok(ScalarValue::Binary(Some(v.to_vec())))
        }
        (None, RwDataType::Bytea) => Ok(ScalarValue::Binary(None)),
        // For other types, use fallback conversion via IcebergArrowConvert to ensure consistency
        _ => convert_scalar_value_fallback(sv, data_type),
    }
}

fn convert_scalar_value_fallback(
    sv: &Option<ScalarImpl>,
    data_type: RwDataType,
) -> RwResult<ScalarValue> {
    let mut array_builder = data_type.create_array_builder(1);
    array_builder.append(sv.as_ref());
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

pub fn convert_column_order(
    column_order: &ColumnOrder,
    input_columns: &impl ColumnTrait,
) -> SortExpr {
    let expr = DFExpr::Column(input_columns.column(column_order.column_index));
    SortExpr::new(
        expr,
        column_order.order_type.is_ascending(),
        column_order.order_type.nulls_are_first(),
    )
}

pub fn create_data_chunk(
    arrays: impl ExactSizeIterator<Item = DFArrayRef>,
    size: usize,
) -> RwResult<DataChunk> {
    let mut columns = Vec::with_capacity(arrays.len());
    for array in arrays {
        let column = Arc::new(IcebergArrowConvert.array_from_arrow_array_raw(&array)?);
        columns.push(column);
    }
    Ok(DataChunk::new(columns, Bitmap::ones(size)))
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field as DFField, Schema as ArrowSchema};
    use risingwave_common::catalog::Field as RwField;
    use risingwave_common::types::ScalarImpl;

    use super::*;
    use crate::expr::{ExprImpl, InputRef, Literal};

    fn create_input_columns(fields: Vec<(String, RwDataType, DFDataType)>) -> (RwSchema, DFSchema) {
        let rw_fields = fields
            .iter()
            .map(|(name, rw_type, _)| RwField::new(name.clone(), rw_type.clone()))
            .collect();
        let rw_schema = RwSchema::new(rw_fields);

        let arrow_fields = fields
            .iter()
            .map(|(name, _, df_type)| DFField::new(name.clone(), df_type.clone(), true))
            .collect::<Vec<_>>();
        let arrow_schema = ArrowSchema::new(arrow_fields);
        let df_schema = DFSchema::try_from(Arc::new(arrow_schema)).unwrap();

        (rw_schema, df_schema)
    }

    #[test]
    fn test_convert_scalar_value_int32() {
        assert_eq!(
            convert_scalar_value(&Some(ScalarImpl::Int32(1000)), RwDataType::Int32).unwrap(),
            ScalarValue::Int32(Some(1000))
        );
        assert_eq!(
            convert_scalar_value(&None, RwDataType::Int32).unwrap(),
            ScalarValue::Int32(None)
        );
    }

    #[test]
    fn test_convert_scalar_value_varchar() {
        let s: Box<str> = "hello".into();
        assert_eq!(
            convert_scalar_value(&Some(ScalarImpl::Utf8(s)), RwDataType::Varchar).unwrap(),
            ScalarValue::Utf8(Some("hello".to_owned()))
        );
        assert_eq!(
            convert_scalar_value(&None, RwDataType::Varchar).unwrap(),
            ScalarValue::Utf8(None)
        );
    }

    #[test]
    fn test_convert_scalar_value_bytea() {
        let bytes: Box<[u8]> = vec![1, 2, 3].into_boxed_slice();
        assert_eq!(
            convert_scalar_value(&Some(ScalarImpl::Bytea(bytes)), RwDataType::Bytea).unwrap(),
            ScalarValue::Binary(Some(vec![1, 2, 3]))
        );
        assert_eq!(
            convert_scalar_value(&None, RwDataType::Bytea).unwrap(),
            ScalarValue::Binary(None)
        );
    }

    #[test]
    fn test_convert_expr_literal_int32() {
        let expr = ExprImpl::from(Literal::new(Some(ScalarImpl::Int32(42)), RwDataType::Int32));
        let (rw_schema, df_schema) = create_input_columns(vec![]);
        let columns = InputColumns::new(&df_schema, &rw_schema);

        let result = convert_expr(&expr, &columns).unwrap();
        match result {
            DFExpr::Literal(scalar, _) => {
                assert_eq!(scalar, ScalarValue::Int32(Some(42)));
            }
            _ => panic!("Expected literal expression"),
        }
    }

    #[test]
    fn test_convert_expr_literal_null() {
        let expr = ExprImpl::from(Literal::new(None, RwDataType::Int32));
        let (rw_schema, df_schema) = create_input_columns(vec![]);
        let columns = InputColumns::new(&df_schema, &rw_schema);

        let result = convert_expr(&expr, &columns).unwrap();
        match result {
            DFExpr::Literal(scalar, _) => {
                assert_eq!(scalar, ScalarValue::Int32(None));
            }
            _ => panic!("Expected literal expression"),
        }
    }

    #[test]
    fn test_convert_expr_input_ref() {
        let (rw_schema, df_schema) = create_input_columns(vec![(
            "col1".to_owned(),
            RwDataType::Int32,
            DFDataType::Int32,
        )]);
        let columns = InputColumns::new(&df_schema, &rw_schema);

        let expr = ExprImpl::from(InputRef::new(0, RwDataType::Int32));
        let result = convert_expr(&expr, &columns).unwrap();

        match result {
            DFExpr::Column(col) => {
                assert_eq!(col.name, "col1");
            }
            _ => panic!("Expected column expression"),
        }
    }

    #[test]
    fn test_convert_expr_input_ref_multiple_columns() {
        let (rw_schema, df_schema) = create_input_columns(vec![
            ("col0".to_owned(), RwDataType::Int32, DFDataType::Int32),
            ("col1".to_owned(), RwDataType::Varchar, DFDataType::Utf8),
            ("col2".to_owned(), RwDataType::Float64, DFDataType::Float64),
        ]);
        let columns = InputColumns::new(&df_schema, &rw_schema);

        // Test accessing different columns
        for i in 0..3 {
            let expr = ExprImpl::from(InputRef::new(i, rw_schema.fields[i].data_type()));
            let result = convert_expr(&expr, &columns).unwrap();

            match result {
                DFExpr::Column(col) => {
                    assert_eq!(col.name, format!("col{}", i));
                }
                _ => panic!("Expected column expression at index {}", i),
            }
        }
    }
}
