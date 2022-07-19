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

use anyhow::Ok;
use async_trait::async_trait;
use futures::future::ok;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::util::sort_util::{OrderPair, OrderType};

use super::error::StreamExecutorResult;
use super::top_n_executor::{generate_output, TopNExecutorBase, TopNExecutorWrapper};
use super::{BoxedMessageStream, Executor, ExecutorInfo, PkIndices, PkIndicesRef};

pub type TopNGlobalExecutor = TopNExecutorWrapper<InnerTopNGlobalExecutor>;

impl TopNGlobalExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        total_count: usize,
        executor_id: u64,
    ) -> StreamExecutorResult<Self> {
        let info = input.info();
        let schema = input.schema().clone();

        Ok(TopNExecutorWrapper {
            input,
            inner: InnerTopNGlobalExecutor::new(
                info,
                schema,
                order_pairs,
                offset_and_limit,
                pk_indices,
                total_count,
                executor_id,
            )?,
        })
    }
}

pub struct InnerTopNGlobalExecutor{
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// `LIMIT XXX`. None means no limit.
    limit: Option<usize>,

    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The primary key indices of the `TopNExecutor`
    pk_indices: PkIndices,

    /// The internal key indices of the `TopNExecutor`
    internal_key_indices: PkIndices,

    /// The order of internal keys of the `TopNExecutor`
    internal_key_order_types: Vec<OrderType>,

    /// The output row num of the `TopNExecutor`
    total_count: usize,
}

pub fn generate_internal_key(
    order_pairs: &[OrderPair],
    pk_indices: PkIndicesRef,
) -> (PkIndices,  Vec<OrderType>) {
    let mut internal_key_indices = vec![];
    let mut internal_order_types = vec![];

    for order_pair in order_pairs {
        internal_key_indices.push(order_pair.column_idx);
        internal_order_types.push(order_pair.order_type);
    }
    for pk_index in pk_indices {
        if !internal_key_indices.contains(pk_index) {
            internal_key_indices.push(*pk_index);
            internal_order_types.push(OrderType::Ascending);
        }
    }

    (
        internal_key_indices,
        internal_order_types,
    )
}

impl InnerTopNGlobalExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        schema: Schema,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        total_count: usize,
        executor_id: u64,
    ) -> StreamExecutorResult<Self> {
        let (internal_key_indices, internal_key_order_types) =
            generate_internal_key(&order_pairs, &pk_indices);

        Ok(Self{
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("TopNGlobalExecutor {:X}", executor_id),
            },
            schema,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            pk_indices,
            internal_key_indices,
            internal_key_order_types,
            total_count,
        })
    }
    /// Sort `new_row` and keep only `self.limits` entries
    fn sort_and_filter(&self, new_rows: &Vec<Row>, schema: &Schema) {
        let limit = self.limit.unwrap_or(0);
        let offset = self.offset;

    }

}

impl Executor for InnerTopNGlobalExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        panic!("Should execute by wrapper");
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

#[async_trait]
impl TopNExecutorBase for InnerTopNGlobalExecutor {
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.limit.unwrap_or_default());
        let mut res_rows = Vec::with_capacity(self.limit.unwrap_or_default());

        for (op, row_ref) in chunk.rows() {
            let row = row_ref.to_owned_row();
            if let Op::Insert = op {
                res_ops.push(op);
                res_rows.push(row);
            } else {
                panic!("global topn only support insert");
            }
        }
        self.sort_and_filter(&mut res_rows, &self.schema);
        generate_output(res_rows, res_ops, &self.schema)
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        // no need to flush data in global.
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}
