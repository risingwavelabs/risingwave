use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::{
    list_array, Array, ArrayBuilder, ArrayMeta, ArrayRef, DataChunk, ListArray, ListRef, ListValue,
    Utf8Array, Utf8ArrayBuilder,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost, Expression};
use crate::{bail, ensure, ExprError, Result};

/// Converts each array element to its text representation, and concatenates those
/// separated by the delimiter string. If null_string is given and is not NULL,
/// then NULL array entries are represented by that string; otherwise, they are omitted.
///
/// ```sql
/// array_to_string ( array anyarray, delimiter text [, null_string text ] ) → text
/// ```
///
/// Example:
///
/// ```sql
/// array_to_string(ARRAY[1, 2, 3, NULL, 5], ',') → 1,2,3,5
/// array_to_string(ARRAY[1, 2, 3, NULL, 5], ',', '*') → 1,2,3,*,5
/// ```
#[derive(Debug)]
pub struct ArrayToStringExpression {
    pub array: Box<dyn Expression>,
    pub delimiter: Box<dyn Expression>,
    pub null_string: Option<Box<dyn Expression>>,
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
            delimiter,
            null_string,
        })
    }
}

impl Expression for ArrayToStringExpression {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let list_array = self.array.eval_checked(input)?;
        let list_array = list_array.as_list();
        let delim_array = self.delimiter.eval_checked(input)?;
        let delim_array = delim_array.as_utf8();

        let mut output = Utf8ArrayBuilder::with_meta(input.capacity(), ArrayMeta::Simple);

        for (i, vis) in input.vis().iter().enumerate() {
            if !vis {
                output.append_null();
            } else {
                let array = list_array.value_at(i);
                let delim = delim_array.value_at(i);
                if let Some(array) = array && let Some(delim) = delim {
                    output.append(Some(&Self::evaluate(array, delim)));
                } else {
                    output.append_null();
                }
            }
        }
        Ok(Arc::new(output.finish().into()))
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        todo!()
    }
}

impl ArrayToStringExpression {
    fn evaluate(array: ListRef<'_>, delimiter: &str) -> String {
        array
            .values_ref()
            .iter()
            .flat_map(|f| f.iter())
            .map(|f| f.into_utf8())
            .join(delimiter)
    }

    fn evaluate_with_nulls(array: ListRef<'_>, delimiter: &str, null_string: &str) -> String {
        array
            .values_ref()
            .iter()
            .map(|f| match f {
                Some(s) => s.into_utf8(),
                None => null_string,
            })
            .join(delimiter)
    }
}
