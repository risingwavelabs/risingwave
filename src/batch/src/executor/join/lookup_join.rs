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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::join::row_level_iter::RowLevelIter;
use crate::executor::join::{
    concatenate, convert_datum_refs_to_chunk, convert_row_to_chunk, JoinType,
};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

pub struct LookupJoinExecutor {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    probe_side_source: RowLevelIter,
    probe_side_schema: Vec<DataType>,
    probe_side_idxs: Vec<usize>,
    build_side: RowLevelIter,
    build_side_schema: Vec<DataType>,
    build_side_idxs: Vec<usize>,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    last_chunk: Option<SlicedDataChunk>,
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
            let cur_row = self.probe_side_source.get_current_row_ref();
            if cur_row.is_some() {
                // TODO: lookup rows on build side table using index scan, with cur_probe_row as
                // input
                let join_result = match self.join_type {
                    JoinType::Inner => self.do_inner_join(),
                    JoinType::LeftOuter => self.do_left_outer_join(),
                    _ => Err(ErrorCode::NotImplemented(
                        format!(
                            "Lookup Join does not support join type {:?}",
                            self.join_type
                        ),
                        None.into(),
                    )
                    .into()),
                }?;

                if let Some(return_chunk) = join_result {
                    if return_chunk.capacity() > 0 {
                        if let Some(inner_chunk) =
                            self.append_chunk(SlicedDataChunk::new_checked(return_chunk)?)?
                        {
                            yield inner_chunk.reorder_columns(&self.output_indices);
                        }
                    }
                }

                while self.last_chunk.is_some() {
                    let mut temp_chunk: Option<SlicedDataChunk> = None;
                    std::mem::swap(&mut temp_chunk, &mut self.last_chunk);
                    if let Some(inner_chunk) = self.append_chunk(temp_chunk.unwrap())? {
                        yield inner_chunk.reorder_columns(&self.output_indices);
                    }
                }

                self.probe_side_source.advance_row();
            } else {
                break;
            }
        }
    }

    fn do_inner_join(&mut self) -> Result<Option<DataChunk>> {
        let build_chunk = Some(DataChunk::new_dummy(1));
        if let Some(build_side_chunk) = build_chunk {
            let probe_row = self.probe_side_source.get_current_row_ref().unwrap();

            let const_row_chunk = convert_row_to_chunk(
                &probe_row,
                build_side_chunk.capacity(),
                &self.probe_side_schema,
            )?;

            let new_chunk = concatenate(&const_row_chunk, &build_side_chunk)?;

            if let Some(cond) = self.condition.as_ref() {
                let visibility = cond.eval(&new_chunk)?;
                Ok(Some(
                    new_chunk.with_visibility(visibility.as_bool().try_into()?),
                ))
            } else {
                Ok(Some(new_chunk))
            }
        } else {
            Ok(None)
        }
    }

    fn do_left_outer_join(&mut self) -> Result<Option<DataChunk>> {
        let ret = self.do_inner_join()?;
        if let Some(inner) = ret.as_ref() {
            if inner.cardinality() > 0 {
                self.probe_side_source.set_cur_row_matched(true);
            }
        }

        if !self.probe_side_source.get_cur_row_matched() {
            assert!(ret.is_none());
            let mut probe_datum_refs = self
                .probe_side_source
                .get_current_row_ref()
                .unwrap()
                .values()
                .collect_vec();

            for _ in 0..self.build_side_schema.len() {
                probe_datum_refs.push(None);
            }

            let one_row_chunk =
                convert_datum_refs_to_chunk(&probe_datum_refs, 1, &self.schema.data_types())?;

            return Ok(Some(one_row_chunk));
        }

        Ok(ret)
    }

    fn append_chunk(&mut self, input_chunk: SlicedDataChunk) -> Result<Option<DataChunk>> {
        let (mut left_data_chunk, return_chunk) = self.chunk_builder.append_chunk(input_chunk)?;
        std::mem::swap(&mut self.last_chunk, &mut left_data_chunk);
        Ok(return_chunk)
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for LookupJoinExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 2,
            "LookupJoinExeuctor should have 2 children!"
        );

        let lookup_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::LookupJoin
        )?;

        let join_type = JoinType::from_prost(lookup_join_node.get_join_type()?);
        let condition = match lookup_join_node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };

        let left_child = inputs.remove(0);
        let probe_side_schema = left_child.schema().data_types();

        let right_child = inputs.remove(0);
        let build_side_schema = right_child.schema().data_types();

        let output_indices: Vec<usize> = lookup_join_node
            .output_indices
            .iter()
            .map(|&x| x as usize)
            .collect();

        let fields = [
            left_child.schema().fields.clone(),
            right_child.schema().fields.clone(),
        ]
        .concat();
        let original_schema = Schema { fields };
        let actual_schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();

        let mut probe_side_idxs = vec![];
        for left_key in lookup_join_node.get_left_key() {
            probe_side_idxs.push(*left_key as usize)
        }

        let mut build_side_idxs = vec![];
        for right_key in lookup_join_node.get_right_key() {
            build_side_idxs.push(*right_key as usize)
        }

        Ok(Box::new(Self {
            join_type,
            condition,
            probe_side_source: RowLevelIter::new(left_child),
            probe_side_schema,
            probe_side_idxs,
            build_side: RowLevelIter::new(right_child),
            build_side_schema,
            build_side_idxs,
            chunk_builder: DataChunkBuilder::with_default_size(original_schema.data_types()),
            schema: actual_schema,
            output_indices,
            last_chunk: None,
            identity: "LookupJoinExecutor".to_string(),
        }))
    }
}
