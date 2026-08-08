// Copyright 2026 RisingWave Labs
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

use risingwave_common::array::Op;
use risingwave_common::id::SinkId;
use risingwave_storage::StateStore;

use crate::executor::prelude::*;

/// Index-only terminus of a transient pk-index compaction resolver pipeline.
///
/// Its input rows are survivors in pk-index table order:
/// `[pk_columns.., file_path, position]`. It upserts each row into the pk-index and emits no data
/// downstream. Unlike the streaming [`WriterExecutor`](super::WriterExecutor), it does not own an
/// Iceberg data writer and never participates in writer pausing or control notifications.
pub struct CompactionApplyExecutor<S: StateStore> {
    ctx: ActorContextRef,
    input: Option<Executor>,
    pk_index_state_table: StateTable<S>,
    sink_id: SinkId,
}

impl<S: StateStore> CompactionApplyExecutor<S> {
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        pk_index_state_table: StateTable<S>,
        sink_id: SinkId,
    ) -> Self {
        Self {
            ctx,
            input: Some(input),
            pk_index_state_table,
            sink_id,
        }
    }

    async fn process_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        for (op, row) in chunk.rows() {
            debug_assert_eq!(op, Op::Insert);
            self.pk_index_state_table.insert(row);
        }
        self.pk_index_state_table.try_flush().await?;
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        tracing::info!(
            sink_id = self.sink_id.as_raw_id(),
            actor_id = self.ctx.id.as_raw_id(),
            "starting pk-index compaction apply executor"
        );

        // This transient graph is bootstrapped with a non-checkpoint barrier that takes over the
        // writer's already-started table epoch, so the generic `expect_first_barrier` helper (which
        // only accepts Initial/Checkpoint barriers) does not apply here.
        let first_message = input.next().await.ok_or_else(|| {
            StreamExecutorError::channel_closed("compaction apply input before bootstrap barrier")
        })??;
        let barrier = first_message
            .into_barrier()
            .expect("the first compaction apply message must be a barrier");
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.pk_index_state_table.init_epoch(first_epoch).await?;

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    self.process_chunk(chunk).await?;
                }
                Message::Barrier(barrier) => {
                    self.pk_index_state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    yield Message::Barrier(barrier);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}

impl<S: StateStore> Execute for CompactionApplyExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{StreamChunk, StreamChunkTestExt};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::id::SinkId;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::stream_plan::barrier::BarrierKind;
    use risingwave_storage::memory::MemoryStateStore;

    use super::CompactionApplyExecutor;
    use crate::common::table::state_table::StateTable;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MockSource, StreamExecutorTestExt};
    use crate::executor::{ActorContext, Barrier, Execute};

    async fn create_pk_index_state_table(
        store: MemoryStateStore,
        table_id: TableId,
    ) -> StateTable<MemoryStateStore> {
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
        ];

        StateTable::from_table_catalog_inconsistent_op(
            &gen_pbtable(
                table_id,
                column_descs,
                vec![OrderType::ascending()],
                vec![0],
                0,
            ),
            store,
            None,
        )
        .await
    }

    async fn read_index_entry(
        state_table: &StateTable<MemoryStateStore>,
        pk: i64,
    ) -> Option<(String, i64)> {
        let row = state_table
            .get_row(&OwnedRow::new(vec![Some(ScalarImpl::Int64(pk))]))
            .await
            .unwrap()?;
        Some((
            row.datum_at(1).unwrap().into_utf8().to_owned(),
            row.datum_at(2).unwrap().into_int64(),
        ))
    }

    #[tokio::test]
    async fn test_compaction_apply_executor_upserts_survivors_without_output() {
        let store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        let state_table = create_pk_index_state_table(store.clone(), table_id).await;
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Varchar),
            Field::unnamed(DataType::Int64),
        ]);
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![0]);
        let mut executor = CompactionApplyExecutor::new(
            ActorContext::for_test(123),
            source,
            state_table,
            SinkId::new(0),
        )
        .boxed()
        .execute();

        let mut bootstrap = Barrier::new_test_barrier(test_epoch(1));
        bootstrap.kind = BarrierKind::Barrier;
        tx.send_barrier(bootstrap);
        executor.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " I  T              I
             + 1 input.parquet  5",
        ));
        tx.push_barrier(test_epoch(2), false);
        executor.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " I  T               I
             + 1 output.parquet 100
             + 2 output.parquet 200",
        ));
        tx.push_barrier(test_epoch(3), false);

        // The apply executor is index-only, so the next output is the barrier rather than a chunk.
        executor.expect_barrier().await;

        let mut reader = create_pk_index_state_table(store, table_id).await;
        reader
            .init_epoch(EpochPair::new_test_epoch(test_epoch(4)))
            .await
            .unwrap();
        assert_eq!(
            read_index_entry(&reader, 1).await,
            Some(("output.parquet".to_owned(), 100))
        );
        assert_eq!(
            read_index_entry(&reader, 2).await,
            Some(("output.parquet".to_owned(), 200))
        );
    }
}
