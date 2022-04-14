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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_pb::stream_plan::BatchParallelInfo;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::StateStore;

use super::error::TracedStreamExecutorError;
use super::{Executor, ExecutorInfo, Message};
use crate::executor_v2::BoxedMessageStream;

pub struct BatchQueryExecutor<S: StateStore> {
    /// The [`CellBasedTable`] that needs to be queried
    table: CellBasedTable<S>,

    /// The number of tuples in one [`StreamChunk`]
    batch_size: usize,

    info: ExecutorInfo,

    #[allow(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,

    /// This is a workaround for parallelized chain.
    /// TODO(zbw): Remove this when we support range scan.
    parallel_info: BatchParallelInfo,
}

impl<S> BatchQueryExecutor<S>
where
    S: StateStore,
{
    pub const DEFAULT_BATCH_SIZE: usize = 100;

    pub fn new(
        table: CellBasedTable<S>,
        batch_size: usize,
        info: ExecutorInfo,
        key_indices: Vec<usize>,
        parallel_info: BatchParallelInfo,
    ) -> Self {
        Self {
            table,
            batch_size,
            info,
            key_indices,
            parallel_info,
        }
    }

    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self, epoch: u64) {
        let mut iter = self.table.iter(epoch).await?;

        while let Some(data_chunk) = iter
            .collect_data_chunk(&self.table, Some(self.batch_size))
            .await?
        {
            let ops = vec![Op::Insert; data_chunk.cardinality()];
            // Filter out rows
            let filtered_data_chunk = match self.filter_chunk(data_chunk) {
                Some(chunk) => chunk,
                None => {
                    continue;
                }
            };

            let stream_chunk = StreamChunk::from_parts(ops, filtered_data_chunk);
            yield Message::Chunk(stream_chunk);
        }
    }

    /// Now we use hash as a workaround for supporting parallelized chain.
    fn filter_chunk(&self, data_chunk: DataChunk) -> Option<DataChunk> {
        let hash_values = data_chunk
            .get_hash_values(self.info.pk_indices.as_ref(), CRC32FastBuilder)
            .unwrap();
        let n = data_chunk.cardinality();
        let (columns, _visibility) = data_chunk.into_parts();

        let mut new_visibility = BitmapBuilder::with_capacity(n);
        for hv in &hash_values {
            new_visibility
                .append((hv % self.parallel_info.degree as u64) == self.parallel_info.index as u64);
        }
        let new_visibility = new_visibility.finish();
        if new_visibility.num_high_bits() > 0 {
            Some(DataChunk::new(columns, Some(new_visibility)))
        } else {
            None
        }
    }
}

impl<S> Executor for BatchQueryExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        unreachable!("should call `execute_with_epoch`")
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn execute_with_epoch(self: Box<Self>, epoch: u64) -> BoxedMessageStream {
        self.execute_inner(epoch).boxed()
    }
}

#[cfg(test)]
mod test {

    use std::vec;

    use futures_async_stream::for_await;

    use super::*;
    use crate::executor_v2::mview::test_utils::gen_basic_table;

    #[tokio::test]
    async fn test_basic() {
        let test_batch_size = 50;
        let test_batch_count = 5;
        let table = gen_basic_table(test_batch_count * test_batch_size).await;

        let info = ExecutorInfo {
            schema: table.schema().clone(),
            pk_indices: vec![0, 1],
            identity: "BatchQuery".to_owned(),
        };
        let executor = Box::new(BatchQueryExecutor::new(
            table,
            test_batch_size,
            info,
            vec![],
            BatchParallelInfo {
                degree: 1,
                index: 0,
            },
        ));

        let stream = executor.execute_with_epoch(u64::MAX);
        let mut batch_cnt = 0;

        #[for_await]
        for msg in stream {
            let msg: Message = msg.unwrap();
            let chunk = msg.as_chunk().unwrap();
            let data = *chunk
                .column_at(0)
                .array_ref()
                .datum_at(0)
                .unwrap()
                .as_int32();
            assert_eq!(data, (batch_cnt * test_batch_size) as i32);
            batch_cnt += 1;
        }

        assert_eq!(batch_cnt, test_batch_count)
    }
}
