// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use itertools::Itertools;
use regex::Regex;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayMeta, ArrayRef, DataChunk, ListArrayBuilder, ListRef, ListValue, Row,
    Utf8Array,
};
use risingwave_common::types::{DataType, Datum, Scalar, ScalarImpl};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::{build_from_prost as expr_build_from_prost, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct RegexpContext(Regex);

impl RegexpContext {
    pub fn new(pattern: &str) -> Result<Self> {
        Ok(Self(Regex::new(pattern)?))
    }
}

#[derive(Debug)]
pub struct RegexpMatchExpression {
    pub child: Box<dyn Expression>,
    pub ctx: RegexpContext,
}

impl<'a> TryFrom<&'a ExprNode> for RegexpMatchExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::RegexpMatch);
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        let mut children = func_call_node.children.iter();
        let Some(text_node) = children.next() else {
            bail!("Expected argument text");
        };
        let text_expr = expr_build_from_prost(text_node)?;
        let Some(pattern_node) = children.next() else {
            bail!("Expected argument pattern");
        };
        let RexNode::Constant(pattern_value) = pattern_node.get_rex_node().unwrap() else {
            return Err(ExprError::UnsupportedFunction("non-constant pattern in regexp_match".to_string()))
        };
        let pattern_scalar = ScalarImpl::from_proto_bytes(
            pattern_value.get_body(),
            pattern_node.get_return_type().unwrap(),
        )?;
        let ScalarImpl::Utf8(pattern) = pattern_scalar else {
            bail!("Expected pattern to be an String");
        };

        let ctx = RegexpContext::new(&pattern)?;
        Ok(Self {
            child: text_expr,
            ctx,
        })
    }
}

impl RegexpMatchExpression {
    /// Match one row and return the result.
    // TODO: The optimization can be allocated.
    fn match_one(&self, text: Option<&str>) -> Option<ListValue> {
        // If there are multiple captures, then the first one is the whole match, and should be
        // ignored in PostgreSQL's behavior.
        let mut skip_flag = self.ctx.0.captures_len() > 1;

        if let Some(text) = text {
            if let Some(capture) = self.ctx.0.captures(text) {
                let list = capture
                    .iter()
                    .skip_while(|_| {
                        if skip_flag {
                            skip_flag = false;
                            true
                        } else {
                            false
                        }
                    })
                    .flatten()
                    .map(|mat| Some(mat.as_str().to_string().to_scalar_value()))
                    .collect_vec();
                let list = ListValue::new(list);
                Some(list)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl Expression for RegexpMatchExpression {
    fn return_type(&self) -> DataType {
        DataType::List {
            datatype: Box::new(DataType::Varchar),
        }
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let text_arr = self.child.eval_checked(input)?;
        let text_arr: &Utf8Array = text_arr.as_ref().into();
        let mut output = ListArrayBuilder::with_meta(
            input.capacity(),
            ArrayMeta::List {
                datatype: Box::new(DataType::Varchar),
            },
        );

        for (text, vis) in text_arr.iter().zip_eq(input.vis().iter()) {
            if !vis {
                output.append_null()?;
            } else if let Some(list) = self.match_one(text) {
                let list_ref = ListRef::ValueRef { val: &list };
                output.append(Some(list_ref))?;
            } else {
                output.append_null()?;
            }
        }

        Ok(Arc::new(output.finish().into()))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let text = self.child.eval_row(input)?;
        Ok(if let Some(ScalarImpl::Utf8(text)) = text {
            self.match_one(Some(&text)).map(Into::into)
        } else {
            None
        })
    }
}
