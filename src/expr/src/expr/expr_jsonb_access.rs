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

use either::Either;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I32Array, JsonbArray, JsonbArrayBuilder,
    JsonbRef, Utf8Array, Utf8ArrayBuilder,
};
use risingwave_common::ensure;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar, ScalarRef};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::expr::ExprNode;

use super::{BoxedExpression, Expression};
use crate::expr::build_expr_from_prost::get_children_and_return_type;
use crate::expr::build_from_prost;
use crate::{ExprError, Result};

/// This is forked from [`BinaryExpression`] for the following reasons:
/// * Optimize for the case when rhs path is const. (not implemented yet)
/// * It can return null when neither input is null.
/// * We could `append(RefItem)` directly rather than getting a `OwnedItem` first.
pub struct JsonbAccessExpression<A: Array, O, F> {
    input: BoxedExpression,
    path: Either<BoxedExpression, A::OwnedItem>,
    func: F,
    _phantom: std::marker::PhantomData<O>,
}

impl<A: Array, O, F> std::fmt::Debug for JsonbAccessExpression<A, O, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonbAccessExpression")
            .field("input", &self.input)
            .field("path", &self.path)
            .finish()
    }
}

impl<A: Array, O, F> JsonbAccessExpression<A, O, F>
where
    F: Send + Sync + for<'a> Fn(JsonbRef<'a>, A::RefItem<'_>) -> Option<JsonbRef<'a>>,
{
    #[expect(dead_code)]
    pub fn new_const(input: BoxedExpression, path: A::OwnedItem, func: F) -> Self {
        Self {
            input,
            path: Either::Right(path),
            func,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn new_expr(input: BoxedExpression, path: BoxedExpression, func: F) -> Self {
        Self {
            input,
            path: Either::Left(path),
            func,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn eval_strict<'a>(
        &self,
        v: Option<JsonbRef<'a>>,
        p: Option<A::RefItem<'_>>,
    ) -> Option<JsonbRef<'a>> {
        match (v, p) {
            (Some(v), Some(p)) => (self.func)(v, p),
            _ => None,
        }
    }
}

impl<A, O, F> Expression for JsonbAccessExpression<A, O, F>
where
    A: Array,
    for<'a> &'a A: From<&'a ArrayImpl>,
    O: AccessOutput,
    F: Send + Sync + for<'a> Fn(JsonbRef<'a>, A::RefItem<'_>) -> Option<JsonbRef<'a>>,
{
    fn return_type(&self) -> DataType {
        O::return_type()
    }

    fn eval(&self, input: &DataChunk) -> crate::Result<ArrayRef> {
        let Either::Left(path_expr) = &self.path else {
            unreachable!("optimization for const path not implemented yet");
        };
        let path_array = path_expr.eval_checked(input)?;
        let path_array: &A = path_array.as_ref().into();

        let input_array = self.input.eval_checked(input)?;
        let input_array: &JsonbArray = input_array.as_ref().into();

        let mut builder = O::new(input.capacity());
        match input.visibility() {
            // We could ignore visibility and always evaluate access path for all values, because it
            // never returns runtime error. But using visibility could save us some clone cost,
            // unless we adjust [`JsonbArray`] to make sure all clones are on [`Arc`].
            Some(visibility) => {
                for ((v, p), visible) in input_array
                    .iter()
                    .zip_eq_fast(path_array.iter())
                    .zip_eq_fast(visibility.iter())
                {
                    let r = visible.then(|| self.eval_strict(v, p)).flatten();
                    builder.output_nullable(r)?;
                }
            }
            None => {
                for (v, p) in input_array.iter().zip_eq_fast(path_array.iter()) {
                    builder.output_nullable(self.eval_strict(v, p))?;
                }
            }
        };
        Ok(std::sync::Arc::new(builder.finish().into()))
    }

    fn eval_row(&self, input: &OwnedRow) -> crate::Result<Datum> {
        let Either::Left(path_expr) = &self.path else {
            unreachable!("optimization for const path not implemented yet");
        };
        let p = path_expr.eval_row(input)?;
        let p = p
            .as_ref()
            .map(|p| p.as_scalar_ref_impl().try_into().unwrap());

        let v = self.input.eval_row(input)?;
        let v = v
            .as_ref()
            .map(|v| v.as_scalar_ref_impl().try_into().unwrap());

        let r = self.eval_strict(v, p);
        Ok(r.and_then(O::to_datum))
    }
}

// TODO: this needs template support for returning Option.
// #[function("jsonb_access_inner(jsonb, varchar) -> jsonb")]
pub fn jsonb_object_field<'a>(v: JsonbRef<'a>, p: &str) -> Option<JsonbRef<'a>> {
    v.access_object_field(p)
}

// #[function("jsonb_access_inner(jsonb, int32) -> jsonb")]
pub fn jsonb_array_element(v: JsonbRef<'_>, p: i32) -> Option<JsonbRef<'_>> {
    let idx = if p < 0 {
        let Ok(len) = v.array_len() else {
            return None;
        };
        if ((-p) as usize) > len {
            return None;
        } else {
            len - ((-p) as usize)
        }
    } else {
        p as usize
    };
    v.access_array_element(idx)
}

trait AccessOutput: ArrayBuilder {
    fn return_type() -> DataType;
    fn output(&mut self, v: JsonbRef<'_>) -> crate::Result<()>;
    fn to_datum(v: JsonbRef<'_>) -> Datum;
    fn output_nullable(&mut self, v: Option<JsonbRef<'_>>) -> crate::Result<()> {
        match v {
            Some(v) => self.output(v)?,
            None => self.append_null(),
        };
        Ok(())
    }
}

impl AccessOutput for JsonbArrayBuilder {
    fn return_type() -> DataType {
        DataType::Jsonb
    }

    fn output(&mut self, v: JsonbRef<'_>) -> crate::Result<()> {
        self.append(Some(v));
        Ok(())
    }

    fn to_datum(v: JsonbRef<'_>) -> Datum {
        Some(v.to_owned_scalar().to_scalar_value())
    }
}

impl AccessOutput for Utf8ArrayBuilder {
    fn return_type() -> DataType {
        DataType::Varchar
    }

    fn output(&mut self, v: JsonbRef<'_>) -> crate::Result<()> {
        match v.is_jsonb_null() {
            true => self.append_null(),
            false => {
                let mut writer = self.writer().begin();
                v.force_str(&mut writer)
                    .map_err(|e| crate::ExprError::Internal(e.into()))?;
                writer.finish();
            }
        };
        Ok(())
    }

    fn to_datum(v: JsonbRef<'_>) -> Datum {
        match v.is_jsonb_null() {
            true => None,
            false => {
                let mut s = String::new();
                v.force_str(&mut s).unwrap();
                let s: Box<str> = s.into();
                Some(s.to_scalar_value())
            }
        }
    }
}

/// Create a new jsonb expression.
pub fn build_jsonb_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, _) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 2);
    let l = build_from_prost(&children[0])?;
    let r = build_from_prost(&children[1])?;

    let expr = match prost.get_expr_type().unwrap() {
        Type::JsonbAccessInner => match r.return_type() {
            DataType::Varchar => {
                JsonbAccessExpression::<Utf8Array, JsonbArrayBuilder, _>::new_expr(
                    l,
                    r,
                    jsonb_object_field,
                )
                .boxed()
            }
            DataType::Int32 => JsonbAccessExpression::<I32Array, JsonbArrayBuilder, _>::new_expr(
                l,
                r,
                jsonb_array_element,
            )
            .boxed(),
            t => return Err(ExprError::UnsupportedFunction(format!("jsonb -> {t}"))),
        },
        Type::JsonbAccessStr => match r.return_type() {
            DataType::Varchar => JsonbAccessExpression::<Utf8Array, Utf8ArrayBuilder, _>::new_expr(
                l,
                r,
                jsonb_object_field,
            )
            .boxed(),
            DataType::Int32 => JsonbAccessExpression::<I32Array, Utf8ArrayBuilder, _>::new_expr(
                l,
                r,
                jsonb_array_element,
            )
            .boxed(),
            t => return Err(ExprError::UnsupportedFunction(format!("jsonb ->> {t}"))),
        },

        tp => {
            return Err(ExprError::UnsupportedFunction(format!(
                "{:?}({:?}, {:?})",
                tp,
                l.return_type(),
                r.return_type(),
            )));
        }
    };
    Ok(expr)
}

#[cfg(test)]
mod tests {
    use std::vec;

    use risingwave_common::array::{ArrayImpl, DataChunk, Utf8Array};
    use risingwave_common::types::Scalar;
    use risingwave_common::util::value_encoding::serialize_datum;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::{DataType as ProstDataType, Datum as ProstDatum};
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use super::*;

    #[test]
    fn test_array_access_expr() {
        let values = FunctionCall {
            children: vec![
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Varchar as i32,
                        ..Default::default()
                    }),
                    rex_node: Some(RexNode::Constant(ProstDatum {
                        body: serialize_datum(Some("foo".into()).as_ref()),
                    })),
                },
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Varchar as i32,
                        ..Default::default()
                    }),
                    rex_node: Some(RexNode::Constant(ProstDatum {
                        body: serialize_datum(Some("bar".into()).as_ref()),
                    })),
                },
            ],
        };
        let array_index = FunctionCall {
            children: vec![
                ExprNode {
                    expr_type: Type::Array as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::List as i32,
                        field_type: vec![ProstDataType {
                            type_name: TypeName::Varchar as i32,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    rex_node: Some(RexNode::FuncCall(values)),
                },
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    rex_node: Some(RexNode::Constant(ProstDatum {
                        body: serialize_datum(Some(1_i32.to_scalar_value()).as_ref()),
                    })),
                },
            ],
        };
        let access = ExprNode {
            expr_type: Type::ArrayAccess as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Varchar as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(array_index)),
        };
        let expr = build_from_prost(&access);
        assert!(expr.is_ok());

        let res = expr.unwrap().eval(&DataChunk::new_dummy(1)).unwrap();
        assert_eq!(*res, ArrayImpl::Utf8(Utf8Array::from_iter(["foo"])));
    }
}
