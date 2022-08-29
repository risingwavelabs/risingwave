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

use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::DataType;

use super::aggregator::Aggregator;
use super::BoxedAggState;
use crate::expr::ExpressionRef;
use crate::Result;

#[derive(Clone)]
pub struct Filter {
    filter: ExpressionRef,
    child: BoxedAggState,
}

impl Filter {
    pub fn new(filter: ExpressionRef, child: BoxedAggState) -> Self {
        assert_eq!(filter.return_type(), DataType::Boolean);
        Self { filter, child }
    }
}

impl Aggregator for Filter {
    fn return_type(&self) -> DataType {
        self.child.return_type()
    }

    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        let (row_ref, vis) = input.row_at(row_id)?;
        assert!(vis);
        if self
            .filter
            .eval_row(&row_ref.to_owned_row())?
            .expect("filter must have non-null result")
            .into_bool()
        {
            self.child.update_single(input, row_id)?;
        }
        Ok(())
    }

    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        let bitmap = if start_row_id == 0 && end_row_id == input.cardinality() {
            // if the input if the whole chunk, use `eval` to speed up
            self.filter.eval(input)?.as_bool().to_bitmap()
        } else {
            // otherwise, run `eval_row` on each row
            (start_row_id..end_row_id)
                .map(|row_id| -> Result<bool> {
                    let (row_ref, vis) = input.row_at(row_id)?;
                    assert!(vis);
                    Ok(self
                        .filter
                        .eval_row(&row_ref.to_owned_row())?
                        .expect("filter must have non-null result")
                        .into_bool())
                })
                .try_collect::<Bitmap>()?
        };
        if bitmap.is_all_set() {
            // if the bitmap is all set, meaning all rows satisfy the filter,
            // call `update_multi` for potential optimization
            self.child.update_multi(input, start_row_id, end_row_id)
        } else {
            for (_, row_id) in (start_row_id..end_row_id)
                .enumerate()
                .filter(|(i, _)| bitmap.is_set(*i).unwrap())
            {
                self.child.update_single(input, row_id)?;
            }
            Ok(())
        }
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        self.child.output(builder)
    }
}
