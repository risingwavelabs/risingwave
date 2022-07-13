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

use std::collections::BinaryHeap;

use risingwave_common::array::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{HeapElem, OrderPair};

use crate::vector_op::agg::aggregator::Aggregator;

pub struct StringAgg {
    agg_col_idx: usize,
    curr: String,
    order_pairs: Vec<OrderPair>,
    min_heap: BinaryHeap<HeapElem>,
}

impl StringAgg {
    pub fn new(agg_col_idx: usize, order_pairs: Vec<OrderPair>) -> Self {
        StringAgg {
            agg_col_idx,
            curr: String::new(),
            order_pairs,
            min_heap: BinaryHeap::new(),
        }
    }
}

impl Aggregator for StringAgg {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        log::warn!("[rc] update_single, input: {:?} row_id: {}", input, row_id);
        if let ArrayImpl::Utf8(input) = input.column_at(self.agg_col_idx).array_ref() {
            if let Some(s) = input.value_at(row_id) {
                self.curr.push_str(s);
            }
            Ok(())
        } else {
            Err(
                ErrorCode::InternalError(format!("Input fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }

    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        log::warn!(
            "[rc] update_multi, input: {:?}, start_row_id: {}, end_row_id: {}",
            input,
            start_row_id,
            end_row_id
        );
        if let ArrayImpl::Utf8(input) = input.column_at(self.agg_col_idx).array_ref() {
            for input_row in input
                .iter()
                .skip(start_row_id)
                .take(end_row_id - start_row_id)
            {
                if let Some(s) = input_row {
                    self.curr.push_str(s);
                }
            }
            Ok(())
        } else {
            Err(
                ErrorCode::InternalError(format!("Input fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }

    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        log::warn!("[rc] output, builder: {:?}", builder);
        if let ArrayBuilderImpl::Utf8(builder) = builder {
            builder.append(Some(&self.curr)).map_err(Into::into)
        } else {
            Err(
                ErrorCode::InternalError(format!("Builder fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }

    fn output_and_reset(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        let res = self.output(builder);
        self.curr.clear();
        res
    }
}
