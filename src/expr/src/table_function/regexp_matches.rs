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

use regex::Regex;
use risingwave_common::array::{Array, ArrayRef, DataChunk, ListValue, Utf8Array};
use risingwave_common::types::{Scalar, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_common::{bail, ensure};
use risingwave_pb::expr::expr_node::RexNode;

use super::*;
use crate::expr::Expression;
use crate::ExprError;

#[derive(Debug)]
pub struct RegexpContext(Regex);

impl RegexpContext {
    pub fn new(pattern: &str) -> Result<Self> {
        Ok(Self(Regex::new(pattern)?))
    }
}

/// It is almost the same as `regexp_match` function (see `expr_regexp.rs`), except
/// that all the captures are returned.
#[derive(Debug)]
pub struct RegexpMatches {
    text: Box<dyn Expression>,
    ctx: RegexpContext,
}

impl RegexpMatches {
    /// Match one row and return the result.
    // TODO: The optimization can be allocated.
    fn eval_row(&self, text: &str) -> Result<ArrayRef> {
        let mut builder = self
            .return_type()
            .create_array_builder(DEFAULT_CHUNK_BUFFER_SIZE);

        for capture in self.ctx.0.captures_iter(text) {
            // If there are multiple captures, then the first one is the whole match, and should be
            // ignored in PostgreSQL's behavior.
            let mut skip_flag = self.ctx.0.captures_len() > 1;
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
            builder.append_datum(&Some(list.into()))?;
        }

        Ok(Arc::new(builder.finish()))
    }
}

impl TableFunction for RegexpMatches {
    fn return_type(&self) -> DataType {
        DataType::List {
            datatype: Box::new(DataType::Varchar),
        }
    }

    fn eval(&self, input: &DataChunk) -> Result<Vec<ArrayRef>> {
        let text_arr = self.text.eval_checked(input)?;
        let text_arr: &Utf8Array = text_arr.as_ref().into();

        let bitmap = input.get_visibility_ref();
        let mut output_arrays: Vec<ArrayRef> = vec![];

        match bitmap {
            Some(bitmap) => {
                for (text, visible) in text_arr.iter().zip_eq(bitmap.iter()) {
                    let array = if !visible {
                        empty_array(self.return_type())
                    } else if let Some(text) = text {
                        self.eval_row(text)?
                    } else {
                        empty_array(self.return_type())
                    };
                    output_arrays.push(array);
                }
            }
            None => {
                for text in text_arr.iter() {
                    let array = if let Some(text) = text {
                        self.eval_row(text)?
                    } else {
                        empty_array(self.return_type())
                    };
                    output_arrays.push(array);
                }
            }
        }

        Ok(output_arrays)
    }
}

pub fn new_regexp_matches(prost: &TableFunctionProst) -> Result<BoxedTableFunction> {
    ensure!(
        prost.return_type
            == Some(
                DataType::List {
                    datatype: Box::new(DataType::Varchar),
                }
                .to_protobuf()
            )
    );
    let mut args = prost.args.iter();
    let Some(text_node) = args.next() else {
        bail!("Expected argument text");
    };
    let text_expr = expr_build_from_prost(text_node)?;
    let Some(pattern_node) = args.next() else {
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
    Ok(RegexpMatches {
        text: text_expr,
        ctx,
    }
    .boxed())
}
