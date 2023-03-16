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

use std::fmt::Write;
use std::sync::Arc;

use risingwave_common::array::*;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::to_text::ToText;
use risingwave_common::types::{DataType, Datum};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost, Expression};
use crate::{bail, ensure, ExprError, Result};

/// Converts each array element to its text representation, and concatenates those
/// separated by the delimiter string. If `null_string` is given and is not NULL,
/// then NULL array entries are represented by that string; otherwise, they are omitted.
///
/// ```sql
/// array_to_string ( array anyarray, delimiter text [, null_string text ] ) → text
/// ```
///
/// Examples:
///
/// ```slt
/// query T
/// select array_to_string(array[1, 2, 3, NULL, 5], ',')
/// ----
/// 1,2,3,5
///
/// query T
/// select array_to_string(array[1, 2, 3, NULL, 5], ',', '*')
/// ----
/// 1,2,3,*,5
///
/// query T
/// select array_to_string(array[null,'foo',null], ',', '*');
/// ----
/// *,foo,*
///
/// query T
/// select array_to_string(array['2023-02-20 17:35:25'::timestamp, null,'2023-02-19 13:01:30'::timestamp], ',', '*');
/// ----
/// 2023-02-20 17:35:25,*,2023-02-19 13:01:30
///
/// query T
/// with t as (
///   select array[1,null,2,3] as arr, ',' as d union all
///   select array[4,5,6,null,7] as arr, '|')
/// select array_to_string(arr, d) from t;
/// ----
/// 1,2,3
/// 4|5|6|7
///
/// # `array` or `delimiter` are required. Otherwise, returns null.
/// query T
/// select array_to_string(array[1,2], NULL);
/// ----
/// NULL
///
/// query error polymorphic type
/// select array_to_string(null, ',');
/// ```
#[derive(Debug)]
pub struct ArrayToStringExpression {
    array: Box<dyn Expression>,
    element_data_type: DataType,
    delimiter: Box<dyn Expression>,
    null_string: Option<Box<dyn Expression>>,
}

impl<'a> TryFrom<&'a ExprNode> for ArrayToStringExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::ArrayToString);
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        let mut children = func_call_node.children.iter();
        let Some(array_node) = children.next() else {
            bail!("Expected argument 'array'");
        };
        let array = build_from_prost(array_node)?;

        let element_data_type = match array.return_type() {
            DataType::List { datatype } => *datatype,
            _ => bail!("Expected argument 'array' to be of type List"),
        };

        let Some(delim_node) = children.next() else {
            bail!("Expected argument 'delimiter'");
        };
        let delimiter = build_from_prost(delim_node)?;

        let null_string = if let Some(null_string_node) = children.next() {
            Some(build_from_prost(null_string_node)?)
        } else {
            None
        };

        Ok(Self {
            array,
            element_data_type,
            delimiter,
            null_string,
        })
    }
}

#[async_trait::async_trait]
impl Expression for ArrayToStringExpression {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let list_array = self.array.eval_checked(input).await?;
        let list_array = list_array.as_list();

        let delim_array = self.delimiter.eval_checked(input).await?;
        let delim_array = delim_array.as_utf8();

        let null_string_array = if let Some(expr) = &self.null_string {
            let null_string_array = expr.eval_checked(input).await?;
            Some(null_string_array)
        } else {
            None
        };
        let null_string_array = null_string_array.as_ref().map(|a| a.as_utf8());

        let mut output = Utf8ArrayBuilder::with_meta(input.capacity(), ArrayMeta::Simple);

        for (i, vis) in input.vis().iter().enumerate() {
            if !vis {
                output.append_null();
                continue;
            }
            let array = list_array.value_at(i);
            let delim = delim_array.value_at(i);
            let null_string = if let Some(a) = null_string_array {
                a.value_at(i)
            } else {
                None
            };

            if let Some(array) = array && let Some(delim) = delim {
                let mut writer = output.writer().begin();
                if let Some(null_string) = null_string {
                    self.evaluate_with_nulls(array, delim, null_string, &mut writer);
                } else {
                    self.evaluate(array, delim, &mut writer);
                }
                writer.finish();
            } else {
                output.append_null();
            }
        }
        Ok(Arc::new(output.finish().into()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let array = self.array.eval_row(input).await?;
        let delimiter = self.delimiter.eval_row(input).await?;

        let result = if let Some(array) = array && let Some(delimiter) = delimiter {
            let null_string = if let Some(e) = &self.null_string {
                e.eval_row(input).await?
            } else {
                None
            };
            let mut writer = String::new();
            if let Some(null_string) = null_string {
                self.evaluate_with_nulls(array.as_scalar_ref_impl().into_list(), delimiter.as_utf8(), null_string.as_utf8(), &mut writer);
            } else {
                self.evaluate(array.as_scalar_ref_impl().into_list(), delimiter.as_utf8(), &mut writer);
            }
            Some(writer)
        } else {
            None
        };
        Ok(result.map(|r| r.into()))
    }
}

impl ArrayToStringExpression {
    fn evaluate(&self, array: ListRef<'_>, delimiter: &str, mut writer: &mut dyn Write) {
        let mut first = true;
        for element in array.values_ref().iter().flat_map(|f| f.iter()) {
            if !first {
                write!(writer, "{}", delimiter).unwrap();
            } else {
                first = false;
            }
            element
                .write_with_type(&self.element_data_type, &mut writer)
                .unwrap();
        }
    }

    fn evaluate_with_nulls(
        &self,
        array: ListRef<'_>,
        delimiter: &str,
        null_string: &str,
        mut writer: &mut dyn Write,
    ) {
        let mut first = true;
        for element in array.values_ref() {
            if !first {
                write!(writer, "{}", delimiter).unwrap();
            } else {
                first = false;
            }
            match element {
                Some(s) => s
                    .write_with_type(&self.element_data_type, &mut writer)
                    .unwrap(),
                None => write!(writer, "{}", null_string).unwrap(),
            }
        }
    }
}
