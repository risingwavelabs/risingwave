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

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, TableOption};
use risingwave_common::hash::{VirtualNode, VnodeCountCompat};
use risingwave_common::row::{OwnedRow, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::{
    BasicSerde, ValueRowDeserializer, ValueRowSerializer,
};
use risingwave_hummock_sdk::key::TableKey;
use risingwave_pb::catalog::Table;
use risingwave_storage::row_serde::row_serde_util::serialize_pk_with_vnode;
use risingwave_storage::row_serde::value_serde::ValueRowSerdeNew;
use risingwave_storage::store::{
    CHECK_BYTES_EQUAL, ChangeLogValue, InitOptions, LocalStateStore, NewLocalOptions,
    OpConsistencyLevel, SealCurrentEpochOptions, StateStoreIter,
};
use risingwave_storage::table::TableDistribution;

use crate::executor::prelude::*;

pub struct ChangeBufferExecutor<S>
where
    S: StateStore,
{
    actor_context: ActorContextRef,
    input: Executor,
    state_store: S,
    table: Table,
    vnodes: Arc<Bitmap>,
    buffer_max_size: usize,
}

#[derive(Default)]
struct EpochChunkBuffer {
    chunks: VecDeque<StreamChunk>,
    row_count: usize,
}

impl EpochChunkBuffer {
    fn push_chunk(&mut self, chunk: StreamChunk) {
        self.row_count += chunk.cardinality();
        self.chunks.push_back(chunk);
    }

    fn exceeds(&self, max_rows: usize) -> bool {
        self.row_count > max_rows
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    fn drain(&mut self) -> impl Iterator<Item = StreamChunk> + '_ {
        self.row_count = 0;
        self.chunks.drain(..)
    }
}

struct ChangeBufferTableSerde {
    data_types: Vec<DataType>,
    pk_indices: Vec<usize>,
    value_indices: Option<Vec<usize>>,
    distribution: TableDistribution,
    pk_serde: OrderedRowSerde,
    row_serde: Arc<BasicSerde>,
}

impl ChangeBufferTableSerde {
    fn new(table: &Table, vnodes: Arc<Bitmap>) -> StreamExecutorResult<Self> {
        let table_columns: Vec<ColumnDesc> = table
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();
        let order_types: Vec<OrderType> = table
            .pk
            .iter()
            .map(|col_order| OrderType::from_protobuf(col_order.get_order_type().unwrap()))
            .collect();
        let dist_key_indices: Vec<usize> = table
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();
        let pk_indices = table
            .pk
            .iter()
            .map(|col_order| col_order.column_index as usize)
            .collect_vec();
        let dist_key_in_pk_indices = if table.get_dist_key_in_pk().is_empty() {
            get_dist_key_in_pk_indices(&dist_key_indices, &pk_indices)?
        } else {
            table
                .get_dist_key_in_pk()
                .iter()
                .map(|idx| *idx as usize)
                .collect()
        };
        let vnode_col_idx_in_pk = table.vnode_col_index.as_ref().and_then(|idx| {
            let vnode_col_idx = *idx as usize;
            pk_indices.iter().position(|&i| vnode_col_idx == i)
        });

        let distribution = TableDistribution::new(
            Some(vnodes.clone()),
            dist_key_in_pk_indices,
            vnode_col_idx_in_pk,
        );
        if distribution.vnode_count() != table.vnode_count() {
            return Err(StreamExecutorError::from(anyhow!(
                "vnode count mismatch for change buffer table {}",
                table.name
            )));
        }

        let pk_data_types = pk_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect();
        let pk_serde = OrderedRowSerde::new(pk_data_types, order_types);

        let value_indices: Arc<[usize]> =
            Arc::from_iter(table.value_indices.iter().map(|idx| *idx as usize));
        let data_types = value_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect();
        let no_shuffle_value_indices = (0..table_columns.len()).collect_vec();
        let value_indices = if value_indices.len() == table_columns.len()
            && value_indices.as_ref() == no_shuffle_value_indices
        {
            None
        } else {
            Some(value_indices.to_vec())
        };
        let row_serde = Arc::new(BasicSerde::new(
            Arc::from_iter(table.value_indices.iter().map(|idx| *idx as usize)),
            Arc::from(table_columns.into_boxed_slice()),
        ));

        Ok(Self {
            data_types,
            pk_indices,
            value_indices,
            distribution,
            pk_serde,
            row_serde,
        })
    }

    fn compute_chunk_vnode(&self, chunk: &StreamChunk) -> Vec<VirtualNode> {
        self.distribution
            .compute_chunk_vnode(chunk, &self.pk_indices)
    }

    fn serialize_key(&self, row: impl Row, vnode: VirtualNode) -> TableKey<Bytes> {
        let pk = row.project(&self.pk_indices);
        serialize_pk_with_vnode(pk, &self.pk_serde, vnode)
    }

    fn serialize_value(&self, row: impl Row) -> Bytes {
        let value = if let Some(value_indices) = &self.value_indices {
            self.row_serde.serialize(row.project(value_indices))
        } else {
            self.row_serde.serialize(row)
        };
        Bytes::from(value)
    }

    fn deserialize_value(&self, value: &[u8]) -> StreamExecutorResult<OwnedRow> {
        let row = self.row_serde.deserialize(value).map_err(|err| {
            StreamExecutorError::from(anyhow!("failed to deserialize change buffer row: {err}"))
        })?;
        Ok(OwnedRow::new(row))
    }

    fn record_from_change_log(
        &self,
        change: ChangeLogValue<&[u8]>,
    ) -> StreamExecutorResult<Record<OwnedRow>> {
        match change {
            ChangeLogValue::Insert(new_value) => Ok(Record::Insert {
                new_row: self.deserialize_value(new_value)?,
            }),
            ChangeLogValue::Delete(old_value) => Ok(Record::Delete {
                old_row: self.deserialize_value(old_value)?,
            }),
            ChangeLogValue::Update {
                old_value,
                new_value,
            } => Ok(Record::Update {
                old_row: self.deserialize_value(old_value)?,
                new_row: self.deserialize_value(new_value)?,
            }),
        }
    }
}

struct ChangeBufferStateWriter<L> {
    local_state_store: L,
    serde: ChangeBufferTableSerde,
    current_epoch: u64,
    dirty: bool,
    pending_update_old: Option<(TableKey<Bytes>, Bytes)>,
}

impl<L: LocalStateStore> ChangeBufferStateWriter<L> {
    async fn new(
        mut local_state_store: L,
        serde: ChangeBufferTableSerde,
        epoch: EpochPair,
    ) -> StreamExecutorResult<Self> {
        local_state_store.init(InitOptions::new(epoch)).await?;
        Ok(Self {
            local_state_store,
            serde,
            current_epoch: epoch.curr,
            dirty: false,
            pending_update_old: None,
        })
    }

    fn write_chunk(&mut self, chunk: &StreamChunk) -> StreamExecutorResult<()> {
        let vnodes = self.serde.compute_chunk_vnode(chunk);
        for (idx, optional_row) in chunk.rows_with_holes().enumerate() {
            let Some((op, row)) = optional_row else {
                continue;
            };
            let vnode = vnodes[idx];
            let key = self.serde.serialize_key(row, vnode);
            let value = self.serde.serialize_value(row);
            match op {
                Op::Insert => {
                    self.local_state_store.insert(key, value, None)?;
                    self.dirty = true;
                }
                Op::Delete => {
                    self.local_state_store.delete(key, value)?;
                    self.dirty = true;
                }
                Op::UpdateDelete => {
                    if self.pending_update_old.replace((key, value)).is_some() {
                        return Err(StreamExecutorError::from(anyhow!(
                            "dangling update-delete before update-insert in epoch {}",
                            self.current_epoch
                        )));
                    }
                }
                Op::UpdateInsert => {
                    let Some((old_key, old_value)) = self.pending_update_old.take() else {
                        return Err(StreamExecutorError::from(anyhow!(
                            "update-insert without matching update-delete in epoch {}",
                            self.current_epoch
                        )));
                    };
                    if old_key == key {
                        self.local_state_store.insert(key, value, Some(old_value))?;
                    } else {
                        self.local_state_store.delete(old_key, old_value)?;
                        self.local_state_store.insert(key, value, None)?;
                    }
                    self.dirty = true;
                }
            }
        }

        if self.pending_update_old.is_some() {
            return Err(StreamExecutorError::from(anyhow!(
                "chunk ended with unmatched update-delete in epoch {}",
                self.current_epoch
            )));
        }

        Ok(())
    }

    fn spill_buffer(&mut self, buffer: &mut EpochChunkBuffer) -> StreamExecutorResult<()> {
        for chunk in buffer.drain() {
            self.write_chunk(&chunk)?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> StreamExecutorResult<()> {
        if self.dirty {
            let _ = self.local_state_store.flush().await?;
        }
        Ok(())
    }

    fn seal_current_epoch(&mut self, next_epoch: u64) -> StreamExecutorResult<()> {
        if self.pending_update_old.is_some() {
            return Err(StreamExecutorError::from(anyhow!(
                "cannot seal epoch {} with unmatched update-delete",
                self.current_epoch
            )));
        }
        self.local_state_store.seal_current_epoch(
            next_epoch,
            SealCurrentEpochOptions {
                table_watermarks: None,
                switch_op_consistency_level: None,
            },
        );
        self.current_epoch = next_epoch;
        self.dirty = false;
        Ok(())
    }
}

impl<L> ChangeBufferStateWriter<L>
where
    L: LocalStateStore,
{
    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn into_change_log_chunks(&self, chunk_size: usize) {
        if !self.dirty {
            return Ok(());
        }

        // The executor validates that the state table primary key matches the input stream key in
        // ascending order, so the uncommitted changelog is already ordered as required.
        let mut iter = self.local_state_store.iter_uncommitted_log().await?;
        let mut builder = StreamChunkBuilder::new(chunk_size, self.serde.data_types.clone());

        while let Some((_key, change)) = iter.try_next().await? {
            let record = self.serde.record_from_change_log(change)?;
            if let Some(chunk) = builder.append_record(record) {
                yield chunk;
            }
        }

        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

impl<S> ChangeBufferExecutor<S>
where
    S: StateStore,
{
    pub(crate) fn new(
        actor_context: ActorContextRef,
        input: Executor,
        state_store: S,
        table: Table,
        vnodes: Arc<Bitmap>,
        buffer_max_size: usize,
    ) -> StreamExecutorResult<Self> {
        validate_change_buffer_pk(&table, input.stream_key())?;

        Ok(Self {
            actor_context,
            input,
            state_store,
            table,
            vnodes,
            buffer_max_size,
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self {
            actor_context,
            input,
            state_store,
            table,
            vnodes,
            buffer_max_size,
        } = self;

        let mut input = input.execute();
        let chunk_size = actor_context.config.developer.chunk_size;

        let first_barrier = expect_first_barrier(&mut input).await?;
        if first_barrier
            .as_update_vnode_bitmap(actor_context.id)
            .is_some()
        {
            return Err(StreamExecutorError::from(anyhow!(
                "ChangeBufferExecutor does not support in-place vnode bitmap update"
            )));
        }
        let first_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier);

        // Forward the first barrier before opening the local state table. New internal tables are
        // registered as part of the creating-job barrier flow, so initializing beforehand can
        // deadlock while waiting for the table to appear in hummock.
        let mut writer = create_epoch_writer(
            &state_store,
            &actor_context,
            &table,
            vnodes.clone(),
            first_epoch,
        )
        .await?;
        let mut buffer = EpochChunkBuffer::default();

        #[for_await]
        for msg in input {
            match msg? {
                Message::Chunk(chunk) => {
                    if chunk.cardinality() == 0 {
                        tracing::warn!(
                            epoch = writer.current_epoch,
                            "received empty chunk (cardinality=0), skipping"
                        );
                        continue;
                    }

                    buffer.push_chunk(chunk);
                    if buffer.exceeds(buffer_max_size) {
                        writer.spill_buffer(&mut buffer)?;
                        writer.flush().await?;
                    }
                }
                Message::Barrier(barrier) => {
                    if barrier.as_update_vnode_bitmap(actor_context.id).is_some() {
                        return Err(StreamExecutorError::from(anyhow!(
                            "ChangeBufferExecutor does not support in-place vnode bitmap update"
                        )));
                    }
                    writer.spill_buffer(&mut buffer)?;
                    writer.flush().await?;

                    #[for_await]
                    for chunk in writer.into_change_log_chunks(chunk_size) {
                        yield Message::Chunk(chunk?);
                    }
                    writer.seal_current_epoch(barrier.epoch.curr)?;

                    yield Message::Barrier(barrier);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}

impl<S> Execute for ChangeBufferExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

async fn create_epoch_writer<S>(
    state_store: &S,
    actor_context: &ActorContextRef,
    table: &Table,
    vnodes: Arc<Bitmap>,
    epoch: EpochPair,
) -> StreamExecutorResult<ChangeBufferStateWriter<S::Local>>
where
    S: StateStore,
{
    let table_serde = ChangeBufferTableSerde::new(table, vnodes.clone())?;
    let local_state_store = state_store
        .new_local(NewLocalOptions {
            table_id: table.id,
            fragment_id: actor_context.fragment_id,
            op_consistency_level: OpConsistencyLevel::ConsistentOldValue {
                check_old_value: CHECK_BYTES_EQUAL.clone(),
                is_log_store: true,
            },
            table_option: TableOption::new(table.retention_seconds),
            is_replicated: false,
            vnodes,
            upload_on_flush: false,
        })
        .await;
    ChangeBufferStateWriter::new(local_state_store, table_serde, epoch).await
}

fn get_dist_key_in_pk_indices(
    dist_key_indices: &[usize],
    pk_indices: &[usize],
) -> StreamExecutorResult<Vec<usize>> {
    dist_key_indices
        .iter()
        .map(|dist_key_idx| {
            pk_indices
                .iter()
                .position(|pk_idx| pk_idx == dist_key_idx)
                .ok_or_else(|| {
                    StreamExecutorError::from(anyhow!(
                        "distribution key column {} is not included in primary key {:?}",
                        dist_key_idx,
                        pk_indices
                    ))
                })
        })
        .collect()
}

fn validate_change_buffer_pk(
    table: &Table,
    input_stream_key: &[usize],
) -> StreamExecutorResult<()> {
    if input_stream_key.is_empty() {
        return Err(StreamExecutorError::from(anyhow!(
            "change buffer requires a non-empty input stream key"
        )));
    }

    let table_pk_indices = table
        .pk
        .iter()
        .map(|column_order| column_order.column_index as usize)
        .collect_vec();
    if table_pk_indices != input_stream_key {
        return Err(StreamExecutorError::from(anyhow!(
            "change buffer table pk {:?} must match input stream key {:?}",
            table_pk_indices,
            input_stream_key
        )));
    }

    if table.pk.iter().any(|column_order| {
        OrderType::from_protobuf(column_order.get_order_type().unwrap()) != OrderType::ascending()
    }) {
        return Err(StreamExecutorError::from(anyhow!(
            "change buffer table pk must use ascending order for all columns"
        )));
    }

    Ok(())
}
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use risingwave_common::array::{StreamChunk, StreamChunkTestExt};
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::{EpochExt, EpochPair};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
    use risingwave_pb::catalog::Table;
    use risingwave_storage::StateStore;

    use super::{
        ChangeBufferExecutor, ChangeBufferStateWriter, EpochChunkBuffer, create_epoch_writer,
    };
    use crate::common::table::test_utils::gen_pbtable_with_dist_key;
    use crate::executor::test_utils::{MockSource, StreamExecutorTestExt};
    use crate::executor::{ActorContext, ActorContextRef, Barrier, Execute, Message, StreamKey};

    fn gen_change_buffer_table(table_id: TableId) -> Table {
        gen_change_buffer_table_with_pk(table_id, vec![0], vec![0])
    }

    fn gen_change_buffer_table_with_pk(
        table_id: TableId,
        pk_indices: Vec<usize>,
        distribution_key: Vec<usize>,
    ) -> Table {
        gen_pbtable_with_dist_key(
            table_id,
            vec![
                ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
                ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
            ],
            vec![OrderType::ascending(); pk_indices.len()],
            pk_indices.clone(),
            pk_indices.len(),
            distribution_key,
        )
    }

    async fn prepare_hummock_writer(
        actor_context: &ActorContextRef,
        table: &Table,
    ) -> (
        risingwave_hummock_test::test_utils::HummockTestEnv,
        Arc<Bitmap>,
        u64,
        ChangeBufferStateWriter<<risingwave_storage::hummock::HummockStorage as StateStore>::Local>,
    ) {
        let test_env = prepare_hummock_test_env().await;
        test_env.register_table(table.clone()).await;

        let vnodes = Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST));
        let epoch = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(table.id)
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch, HashSet::from_iter([table.id]));

        let writer = create_epoch_writer(
            &test_env.storage,
            actor_context,
            table,
            vnodes.clone(),
            EpochPair::new_test_epoch(epoch),
        )
        .await
        .unwrap();

        (test_env, vnodes, epoch, writer)
    }

    #[test]
    fn test_epoch_chunk_buffer_tracks_rows() {
        let mut buffer = EpochChunkBuffer::default();
        buffer.push_chunk(StreamChunk::from_pretty(" I\n + 1"));
        buffer.push_chunk(StreamChunk::from_pretty(" I\n + 2\n + 3"));

        assert!(buffer.exceeds(2));
        assert!(!buffer.exceeds(3));
        assert!(!buffer.is_empty());

        let chunks = buffer.drain().collect::<Vec<_>>();
        assert_eq!(chunks.len(), 2);
        assert!(!buffer.exceeds(1));
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn test_state_writer_reads_pending_imms_from_shared_buffer() {
        let actor_context = ActorContext::for_test(0);
        let table = gen_change_buffer_table(TableId::new(233));
        let (_test_env, _vnodes, _curr_epoch, mut writer) =
            prepare_hummock_writer(&actor_context, &table).await;

        writer
            .write_chunk(&StreamChunk::from_pretty(
                " i i
                + 1 10
                + 2 20",
            ))
            .unwrap();
        writer.flush().await.unwrap();

        writer
            .write_chunk(&StreamChunk::from_pretty(
                " i i
                U- 1 10
                U+ 1 11
                - 2 20
                + 3 30",
            ))
            .unwrap();
        writer.flush().await.unwrap();

        let read_version = writer.local_state_store.read_version();
        let staging = read_version.read();
        assert_eq!(2, staging.staging().pending_imms.len());
        assert!(staging.staging().uploading_imms.is_empty());

        let chunks: Vec<_> = writer
            .into_change_log_chunks(1024)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks.into_iter().next().unwrap().compact_vis(),
            StreamChunk::from_pretty(
                " i i
                + 1 11
                + 3 30"
            )
        );
    }

    #[tokio::test]
    async fn test_change_buffer_executor_emits_changes_from_shared_buffer_imms() {
        let actor_context = ActorContext::for_test(0);
        let table = gen_change_buffer_table(TableId::new(234));
        let (test_env, vnodes, curr_epoch, writer) =
            prepare_hummock_writer(&actor_context, &table).await;
        drop(writer);
        let next_epoch = curr_epoch.next_epoch();
        test_env
            .storage
            .start_epoch(next_epoch, HashSet::from_iter([table.id]));

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(curr_epoch)),
            Message::Chunk(StreamChunk::from_pretty(
                " i i
                + 1 10
                + 2 20",
            )),
            Message::Chunk(StreamChunk::from_pretty(
                " i i
                U- 1 10
                U+ 1 11
                - 2 20
                + 3 30",
            )),
            Message::Barrier(Barrier::new_test_barrier(next_epoch)),
        ])
        .into_executor(schema, StreamKey::from(vec![0]));

        let mut executor = ChangeBufferExecutor::new(
            actor_context,
            source,
            test_env.storage.clone(),
            table,
            vnodes,
            1,
        )
        .unwrap()
        .boxed()
        .execute();

        assert_eq!(executor.expect_barrier().await.epoch.curr, curr_epoch);
        assert_eq!(
            executor.expect_chunk().await.compact_vis(),
            StreamChunk::from_pretty(
                " i i
                + 1 11
                + 3 30"
            )
        );
        assert_eq!(executor.expect_barrier().await.epoch.curr, next_epoch);
    }

    #[tokio::test]
    async fn test_change_buffer_executor_outputs_stream_key_order() {
        let actor_context = ActorContext::for_test(0);
        let table = gen_change_buffer_table_with_pk(TableId::new(235), vec![0, 1], vec![0]);
        let (test_env, vnodes, curr_epoch, writer) =
            prepare_hummock_writer(&actor_context, &table).await;
        drop(writer);
        let next_epoch = curr_epoch.next_epoch();
        test_env
            .storage
            .start_epoch(next_epoch, HashSet::from_iter([table.id]));

        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(curr_epoch)),
            Message::Chunk(StreamChunk::from_pretty(
                " i i
                + 2 30
                + 1 40
                + 1 10
                + 1 20",
            )),
            Message::Barrier(Barrier::new_test_barrier(next_epoch)),
        ])
        .into_executor(schema, StreamKey::from(vec![0, 1]));

        let mut executor = ChangeBufferExecutor::new(
            actor_context,
            source,
            test_env.storage.clone(),
            table,
            vnodes,
            16,
        )
        .unwrap()
        .boxed()
        .execute();

        assert_eq!(executor.expect_barrier().await.epoch.curr, curr_epoch);
        assert_eq!(
            executor.expect_chunk().await.compact_vis(),
            StreamChunk::from_pretty(
                " i i
                + 1 10
                + 1 20
                + 1 40
                + 2 30"
            )
        );
        assert_eq!(executor.expect_barrier().await.epoch.curr, next_epoch);
    }
}
