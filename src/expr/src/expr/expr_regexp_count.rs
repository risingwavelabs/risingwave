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

use std::sync::Arc;

use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I32ArrayBuilder,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_common::{bail, ensure};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::expr_regexp::RegexpContext;
use super::{build_from_prost as expr_build_from_prost, Expression};
use crate::{ExprError, Result};

#[derive(Debug)]
pub struct RegexpCountExpression {
    /// The source text
    pub source: Box<dyn Expression>,
    /// Relevant regex context, contains `flags` option
    pub ctx: RegexpContext,
    /// The start position to begin the counting process
    pub start: Option<u32>,
}

pub const NULL_PATTERN: &str = "a^";

/// This trait provides the transformation from `ExprNode` to `RegexpCountExpression`
impl<'a> TryFrom<&'a ExprNode> for RegexpCountExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        // Sanity check first
        ensure!(prost.get_function_type().unwrap() == Type::RegexpCount);

        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };

        let mut children = func_call_node.children.iter();

        let Some(source_node) = children.next() else {
            bail!("Expected source text");
        };
        let source = expr_build_from_prost(source_node)?;

        let Some(pattern_node) = children.next() else {
            bail!("Expected pattern text");
        };
        let pattern = match &pattern_node.get_rex_node()? {
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
                    _ => bail!("Expected pattern to be a String"),
                }
            }
            _ => {
                return Err(ExprError::UnsupportedFunction(
                    "non-constant pattern in `regexp_count`".to_string(),
                ))
            }
        };

        // Parsing for [ , start [, flags ]]
        let mut flags: Option<String> = None;
        let mut start: Option<u32> = None;

        // See if `start` is specified
        if let Some(start_node) = children.next() {
            start = match &start_node.get_rex_node()? {
                RexNode::Constant(start_value) => {
                    let start_datum = deserialize_datum(
                        start_value.get_body().as_slice(),
                        &DataType::from(start_node.get_return_type().unwrap()),
                    )
                    .map_err(|e| ExprError::Internal(e.into()))?;

                    match start_datum {
                        Some(ScalarImpl::Int32(start)) => {
                            if start <= 0 {
                                bail!("start must greater than zero");
                            }
                            Some(start as u32)
                        }
                        _ => bail!("Expected start to be a Unsigned Int32"),
                    }
                }
                _ => {
                    return Err(ExprError::UnsupportedFunction(
                        "non-constant start in `regexp_count`".to_string(),
                    ))
                }
            };

            // See if `flags` is specified
            if let Some(flags_node) = children.next() {
                flags = match &flags_node.get_rex_node()? {
                    RexNode::Constant(flags_value) => {
                        let flags_datum = deserialize_datum(
                            flags_value.get_body().as_slice(),
                            &DataType::from(flags_node.get_return_type().unwrap()),
                        )
                        .map_err(|e| ExprError::Internal(e.into()))?;

                        match flags_datum {
                            Some(ScalarImpl::Utf8(flags)) => Some(flags.to_string()),
                            _ => bail!("Expected flags to be a String"),
                        }
                    }
                    _ => {
                        return Err(ExprError::UnsupportedFunction(
                            "non-constant flags in `regexp_count`".to_string(),
                        ))
                    }
                }
            }
        };

        // Sanity check
        if children.next().is_some() {
            bail!("syntax error in `regexp_count`");
        }

        let flags = flags.unwrap_or_default();

        if flags.contains('g') {
            bail!("`regexp_count` does not support global flag option");
        }

        let ctx = RegexpContext::new(&pattern, &flags)?;

        Ok(Self { source, ctx, start })
    }
}

impl RegexpCountExpression {
    fn match_row(&self, text: Option<&str>) -> Option<i32> {
        if let Some(text) = text {
            // First get the start position to count for
            let start = if let Some(s) = self.start { s - 1 } else { 0 };

            // For unicode purpose
            let mut start = match text.char_indices().nth(start as usize) {
                Some((idx, _)) => idx,
                // The `start` is out of bound
                None => return Some(0),
            };

            let mut count = 0;

            while let Some(captures) = self.ctx.regex.captures(&text[start..]) {
                count += 1;
                start += captures.get(0).unwrap().end();
            }

            Some(count)
        } else {
            // Input string is None, the return value should be NULL
            None
        }
    }
}

#[async_trait::async_trait]
impl Expression for RegexpCountExpression {
    fn return_type(&self) -> DataType {
        DataType::Int32
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let source_column = self.source.eval_checked(input).await?;
        let source_column = source_column.as_utf8();

        let row_len = input.capacity();
        let vis = input.vis();
        let mut builder: I32ArrayBuilder = ArrayBuilder::new(row_len);

        for row_idx in 0..row_len {
            if !vis.is_set(row_idx) {
                builder.append(None);
                continue;
            }

            let source = source_column.value_at(row_idx);
            builder.append(self.match_row(source));
        }

        Ok(Arc::new(ArrayImpl::from(builder.finish())))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let source = self.source.eval_row(input).await?;
        // Will panic if the input text is not a String
        let source = match source {
            Some(ScalarImpl::Utf8(s)) => s,
            None => return Ok(None),
            // Other than the above cases
            // The input is invalid and we should panic here
            _ => bail!("source should be a String"),
        };

        Ok(self
            .match_row(Some(&source))
            .map(|replaced| replaced.into()))
    }
}
