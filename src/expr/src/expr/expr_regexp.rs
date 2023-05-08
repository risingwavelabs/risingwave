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

use std::str::FromStr;
use std::sync::Arc;

use itertools::Itertools;
use regex::{Regex, RegexBuilder};
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayRef, DataChunk, ListArrayBuilder, ListRef, ListValue, Utf8Array,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::{build_from_prost as expr_build_from_prost, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct RegexpContext(pub Regex);

impl RegexpContext {
    pub fn new(pattern: &str, flags: &str) -> Result<Self> {
        let options = RegexpOptions::from_str(flags)?;
        Ok(Self(
            RegexBuilder::new(pattern)
                .case_insensitive(options.case_insensitive)
                .build()?,
        ))
    }
}

/// <https://www.postgresql.org/docs/current/functions-matching.html#POSIX-EMBEDDED-OPTIONS-TABLE>
struct RegexpOptions {
    /// `c` and `i`
    case_insensitive: bool,
}

#[expect(clippy::derivable_impls)]
impl Default for RegexpOptions {
    fn default() -> Self {
        Self {
            case_insensitive: false,
        }
    }
}

impl FromStr for RegexpOptions {
    type Err = ExprError;

    fn from_str(s: &str) -> Result<Self> {
        let mut opts = Self::default();
        for c in s.chars() {
            match c {
                'c' => opts.case_insensitive = false,
                'i' => opts.case_insensitive = true,
                'g' => {}
                _ => {
                    bail!("invalid regular expression option: \"{c}\"");
                }
            }
        }
        Ok(opts)
    }
}

#[derive(Debug)]
pub struct RegexpMatchExpression {
    pub child: Box<dyn Expression>,
    pub ctx: RegexpContext,
}

/// The pattern that matches nothing.
pub const NULL_PATTERN: &str = "a^";

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
        let mut pattern = match &pattern_node.get_rex_node()? {
            RexNode::Constant(pattern_value) => {
                let pattern_datum = deserialize_datum(
                    pattern_value.get_body().as_slice(),
                    &DataType::from(pattern_node.get_return_type().unwrap()),
                )
                .map_err(|e| ExprError::Internal(e.into()))?;

                match pattern_datum {
                    Some(ScalarImpl::Utf8(pattern)) => pattern.to_string(),
                    // NULL pattern
                    None => NULL_PATTERN.to_string(),
                    _ => bail!("Expected pattern to be an String"),
                }
            }
            _ => {
                return Err(ExprError::UnsupportedFunction(
                    "non-constant pattern in regexp_match".to_string(),
                ))
            }
        };

        let flags = if let Some(flags_node) = children.next() {
            match &flags_node.get_rex_node()? {
                RexNode::Constant(flags_value) => {
                    let flags_datum = deserialize_datum(
                        flags_value.get_body().as_slice(),
                        &DataType::from(flags_node.get_return_type().unwrap()),
                    )
                    .map_err(|e| ExprError::Internal(e.into()))?;

                    match flags_datum {
                        Some(ScalarImpl::Utf8(flags)) => flags.to_string(),
                        // NULL flag
                        None => {
                            pattern = NULL_PATTERN.to_string();
                            "".to_string()
                        }
                        _ => bail!("Expected flags to be an String"),
                    }
                }
                _ => {
                    return Err(ExprError::UnsupportedFunction(
                        "non-constant flags in regexp_match".to_string(),
                    ))
                }
            }
        } else {
            "".to_string()
        };

        let ctx = RegexpContext::new(&pattern, &flags)?;
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
        let skip_flag = self.ctx.0.captures_len() > 1;

        if let Some(text) = text {
            if let Some(capture) = self.ctx.0.captures(text) {
                let list = capture
                    .iter()
                    .skip(if skip_flag { 1 } else { 0 })
                    .map(|mat| mat.map(|m| m.as_str().into()))
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

#[async_trait::async_trait]
impl Expression for RegexpMatchExpression {
    fn return_type(&self) -> DataType {
        DataType::List(Box::new(DataType::Varchar))
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let text_arr = self.child.eval_checked(input).await?;
        let text_arr: &Utf8Array = text_arr.as_ref().into();
        let mut output = ListArrayBuilder::with_type(
            input.capacity(),
            DataType::List(Box::new(DataType::Varchar)),
        );

        for (text, vis) in text_arr.iter().zip_eq_fast(input.vis().iter()) {
            if !vis {
                output.append_null();
            } else if let Some(list) = self.match_one(text) {
                let list_ref = ListRef::ValueRef { val: &list };
                output.append(Some(list_ref));
            } else {
                output.append_null();
            }
        }

        Ok(Arc::new(output.finish().into()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let text = self.child.eval_row(input).await?;
        Ok(if let Some(ScalarImpl::Utf8(text)) = text {
            self.match_one(Some(&text)).map(Into::into)
        } else {
            None
        })
    }
}
