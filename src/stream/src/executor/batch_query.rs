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

use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::{OrderedColumnDesc, Schema};
use risingwave_common::hash::VIRTUAL_NODE_COUNT;
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{Executor, ExecutorInfo, Message};
use crate::executor::BoxedMessageStream;

pub struct BatchQueryExecutor<S: StateStore> {
    /// The [`CellBasedTable`] that needs to be queried
    table: CellBasedTable<S>,

    /// The number of tuples in one [`StreamChunk`]
    batch_size: usize,

    info: ExecutorInfo,

    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,

    /// vnode bitmap used to filter data belong to this parallel unit.
    hash_filter: Bitmap,

    /// public key field descriptors. Used to decode pk into datums
    /// for dedup pk encoding.
    pk_descs: Vec<OrderedColumnDesc>,
}

impl<S> BatchQueryExecutor<S>
where
    S: StateStore,
{
    const DEFAULT_BATCH_SIZE: usize = 100;

    pub fn new(
        table: CellBasedTable<S>,
        batch_size: Option<usize>,
        info: ExecutorInfo,
        key_indices: Vec<usize>,
        hash_filter: Bitmap,
        pk_descs: Vec<OrderedColumnDesc>,
    ) -> Self {
        Self {
            table,
            batch_size: batch_size.unwrap_or(Self::DEFAULT_BATCH_SIZE),
            info,
            key_indices,
            hash_filter,
            pk_descs,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self, epoch: u64) {
        let iter = self
            .table
            .batch_dedup_pk_iter(epoch, &self.pk_descs)
            .await?;
        pin_mut!(iter);

        while let Some(data_chunk) = iter
            .collect_data_chunk(self.schema(), Some(self.batch_size))
            .await?
        {
            // Filter out rows
            let filtered_data_chunk = match self.filter_chunk(data_chunk) {
                Some(chunk) => chunk,
                None => {
                    continue;
                }
            };
            let compacted_chunk = filtered_data_chunk.compact()?;
            let ops = vec![Op::Insert; compacted_chunk.cardinality()];
            let stream_chunk = StreamChunk::from_parts(ops, compacted_chunk);
            yield Message::Chunk(stream_chunk);
        }
    }

    /// Now we use hash as a workaround for supporting parallelized chain.
    fn filter_chunk(&self, data_chunk: DataChunk) -> Option<DataChunk> {
        let hash_values = data_chunk
            .get_hash_values(self.key_indices.as_ref(), CRC32FastBuilder)
            .unwrap();
        let n = data_chunk.cardinality();
        let (columns, _visibility) = data_chunk.into_parts();

        let mut new_visibility = BitmapBuilder::with_capacity(n);
        for hv in &hash_values {
            new_visibility.append(
                self.hash_filter
                    .is_set((hv.0 % VIRTUAL_NODE_COUNT as u64) as usize)
                    .unwrap_or(false),
            );
        }
        let new_visibility = new_visibility.finish();
        if new_visibility.num_high_bits() > 0 {
            Some(DataChunk::new(columns, new_visibility))
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
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::mview::test_utils::gen_basic_table;

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
        let hash_filter = {
            let mut builder = BitmapBuilder::with_capacity(VIRTUAL_NODE_COUNT);
            for _ in 0..VIRTUAL_NODE_COUNT {
                builder.append(true);
            }
            builder.finish()
        };
        let pk_descs = vec![
            OrderedColumnDesc {
                column_desc: ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
                order: OrderType::Ascending,
            },
            OrderedColumnDesc {
                column_desc: ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
                order: OrderType::Descending,
            },
        ];

        let executor = Box::new(BatchQueryExecutor::new(
            table,
            Some(test_batch_size),
            info,
            vec![],
            hash_filter,
            pk_descs,
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
