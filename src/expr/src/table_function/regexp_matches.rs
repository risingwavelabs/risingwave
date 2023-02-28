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

use risingwave_common::array::{Array, ArrayRef, DataChunk, ListValue, Utf8Array};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_common::{bail, ensure};
use risingwave_pb::expr::expr_node::RexNode;

use super::*;
use crate::expr::expr_regexp::{RegexpContext, NULL_PATTERN};
use crate::expr::Expression;
use crate::ExprError;

/// It is almost the same as `regexp_match` function (see `expr_regexp.rs`), except
/// that all the captures are returned.
#[derive(Debug)]
pub struct RegexpMatches {
    text: Box<dyn Expression>,
    ctx: RegexpContext,
    chunk_size: usize,
}

impl RegexpMatches {
    /// Match one row and return the result.
    // TODO: The optimization can be allocated.
    fn eval_row(&self, text: &str) -> Result<ArrayRef> {
        let mut builder = self.return_type().create_array_builder(self.chunk_size);

        for capture in self.ctx.0.captures_iter(text) {
            // If there are multiple captures, then the first one is the whole match, and should be
            // ignored in PostgreSQL's behavior.
            let skip_flag = self.ctx.0.captures_len() > 1;
            let list = capture
                .iter()
                .skip(if skip_flag { 1 } else { 0 })
                .map(|mat| mat.map(|m| m.as_str().into()))
                .collect_vec();
            let list = ListValue::new(list);
            builder.append_datum(&Some(list.into()));
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

        let bitmap = input.visibility();
        let mut output_arrays: Vec<ArrayRef> = vec![];

        match bitmap {
            Some(bitmap) => {
                for (text, visible) in text_arr.iter().zip_eq_fast(bitmap.iter()) {
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

pub fn new_regexp_matches(
    prost: &TableFunctionProst,
    chunk_size: usize,
) -> Result<BoxedTableFunction> {
    ensure!(
        prost.return_type
            == vec![DataType::List {
                datatype: Box::new(DataType::Varchar),
            }
            .to_protobuf()]
    );
    let mut args = prost.args.iter();
    let Some(text_node) = args.next() else {
        bail!("Expected argument text");
    };
    let text_expr = expr_build_from_prost(text_node)?;

    let Some(pattern_node) = args.next() else {
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

    let flags = if let Some(flags_node) = args.next() {
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
    Ok(RegexpMatches {
        text: text_expr,
        ctx,
        chunk_size,
    }
    .boxed())
}
