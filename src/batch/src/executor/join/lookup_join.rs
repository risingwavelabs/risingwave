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

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_expr::expr::BoxedExpression;
use crate::executor::{BoxedDataChunkStream, BoxedExecutor, Executor};
use crate::executor::join::{concatenate, JoinType};
use crate::executor::join::row_level_iter::RowLevelIter;

pub struct LookupJoinExecutor {
    join_type: JoinType,
    join_expr: BoxedExpression,
    probe_side_source: RowLevelIter,
    probe_side_schema: Vec<DataType>,
    build_side: RowLevelIter,
    probe_side_idxs: Vec<usize>,
    build_side_idxs: Vec<usize>,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    identity: String,
}

impl Executor for LookupJoinExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl LookupJoinExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        loop {
            if let Some(cur_probe_row) = self.probe_side_source.get_current_row_ref() {
                // data chunk from index scanning the current probe row on the build table
                let build_side_chunk = DataChunk::new_dummy(1);

                let const_row_chunk = self.convert_row_to_chunk(
                    &probe_row,
                    build_side_chunk.capacity(),
                    &self.probe_side_schema,
                )?;

                let new_chunk = concatenate(&const_row_chunk, &build_side_chunk)?;
                let visibility = self.join_expr.eval(&new_chunk)?;
                let return_chunk = new_chunk.with_visibility(visibility.as_bool().try_into()?);

                if return_chunk.capacity() > 0 {
                    let (mut left_data_chunk, return_data_chunk) = self
                        .chunk_builder
                        .append_chunk(SlicedDataChunk::new_checked(ret_chunk)?)?;
                    // Have checked last chunk is None in before. Now swap to buffer it.
                    std::mem::swap(&mut self.last_chunk, &mut left_data_chunk);
                    if let Some(inner_chunk) = return_data_chunk {
                        yield inner_chunk.reorder_columns(&self.output_indices);
                    }
                }

                self.probe_side_source.advance_row();
            } else {
                break;
            }
        }
    }
}