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

use std::mem::replace;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use futures::stream::{FuturesUnordered, Peekable, StreamFuture};
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::constants::log_store::{
    EPOCH_COLUMN_INDEX, EPOCH_COLUMN_TYPE, KV_LOG_STORE_PREDEFINED_COLUMNS, PK_TYPES,
    ROW_OP_COLUMN_INDEX, SEQ_ID_COLUMN_INDEX,
};
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::{
    BasicSerde, ValueRowDeserializer, ValueRowSerializer,
};
use risingwave_connector::sink::log_store::LogStoreResult;
use risingwave_hummock_sdk::key::{next_key, TableKey};
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::catalog::Table;
use risingwave_storage::error::StorageError;
use risingwave_storage::row_serde::row_serde_util::{serialize_pk, serialize_pk_with_vnode};
use risingwave_storage::row_serde::value_serde::ValueRowSerdeNew;
use risingwave_storage::store::StateStoreReadIterStream;
use risingwave_storage::table::{compute_vnode, Distribution};

use crate::common::log_store_impl::kv_log_store::{
    KvLogStoreReadMetrics, ReaderTruncationOffsetType, RowOpCodeType, SeqIdType,
};

const INSERT_OP_CODE: RowOpCodeType = 1;
const DELETE_OP_CODE: RowOpCodeType = 2;
const UPDATE_INSERT_OP_CODE: RowOpCodeType = 3;
const UPDATE_DELETE_OP_CODE: RowOpCodeType = 4;
const BARRIER_OP_CODE: RowOpCodeType = 5;
const CHECKPOINT_BARRIER_OP_CODE: RowOpCodeType = 6;

struct ReadInfo {
    read_size: usize,
    read_count: usize,
}

impl ReadInfo {
    fn new() -> Self {
        Self {
            read_count: 0,
            read_size: 0,
        }
    }

    fn read_one_row(&mut self, size: usize) {
        self.read_count += 1;
        self.read_size += size;
    }

    fn report(&mut self, metrics: &KvLogStoreReadMetrics) {
        metrics.storage_read_size.inc_by(self.read_size as _);
        metrics.storage_read_count.inc_by(self.read_count as _);
        self.read_size = 0;
        self.read_count = 0;
    }
}

#[derive(Eq, PartialEq, Debug)]
enum LogStoreRowOp {
    Row { op: Op, row: OwnedRow },
    Barrier { is_checkpoint: bool },
}

#[derive(Clone)]
pub(crate) struct LogStoreRowSerde {
    /// Used for serializing and deserializing the primary key.
    pk_serde: OrderedRowSerde,

    /// Row deserializer with value encoding
    row_serde: BasicSerde,

    /// Serde of epoch
    epoch_serde: OrderedRowSerde,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    dist_key_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    ///
    /// Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. The table will also check whether the written rows
    /// conform to this partition.
    vnodes: Arc<Bitmap>,

    /// The schema of payload
    payload_schema: Vec<DataType>,
}

impl LogStoreRowSerde {
    pub(crate) fn new(table_catalog: &Table, vnodes: Option<Arc<Bitmap>>) -> Self {
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();
        let dist_key_indices: Vec<usize> = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();

        let input_value_indices = table_catalog
            .value_indices
            .iter()
            .map(|val| *val as usize)
            .collect_vec();

        let data_types = input_value_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect_vec();

        // There are 3 predefined columns for kv log store:
        assert!(data_types.len() > KV_LOG_STORE_PREDEFINED_COLUMNS.len());
        for i in 0..KV_LOG_STORE_PREDEFINED_COLUMNS.len() {
            assert_eq!(data_types[i], KV_LOG_STORE_PREDEFINED_COLUMNS[i].1);
        }

        let payload_schema = data_types[KV_LOG_STORE_PREDEFINED_COLUMNS.len()..].to_vec();

        let row_serde = BasicSerde::new(input_value_indices.into(), table_columns.into());

        let vnodes = match vnodes {
            Some(vnodes) => vnodes,

            None => Distribution::fallback_vnodes(),
        };

        // epoch and seq_id. The seq_id of barrier is set null, and therefore the second order type
        // is nulls last
        let pk_serde = OrderedRowSerde::new(
            Vec::from(PK_TYPES),
            vec![OrderType::ascending(), OrderType::ascending_nulls_last()],
        );

        let epoch_serde =
            OrderedRowSerde::new(vec![EPOCH_COLUMN_TYPE], vec![OrderType::ascending()]);

        Self {
            pk_serde,
            row_serde,
            epoch_serde,
            dist_key_indices,
            vnodes,
            payload_schema,
        }
    }

    pub(crate) fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) {
        self.vnodes = vnodes;
    }

    pub(crate) fn vnodes(&self) -> &Bitmap {
        self.vnodes.as_ref()
    }

    pub(crate) fn encode_epoch(epoch: u64) -> i64 {
        epoch as i64 ^ (1i64 << 63)
    }

    pub(crate) fn decode_epoch(encoded_epoch: i64) -> u64 {
        encoded_epoch as u64 ^ (1u64 << 63)
    }
}

impl LogStoreRowSerde {
    pub(crate) fn serialize_data_row(
        &self,
        epoch: u64,
        seq_id: SeqIdType,
        op: Op,
        row: impl Row,
    ) -> (VirtualNode, TableKey<Bytes>, Bytes) {
        let pk = [
            Some(ScalarImpl::Int64(Self::encode_epoch(epoch))),
            Some(ScalarImpl::Int32(seq_id)),
        ];
        let op_code = match op {
            Op::Insert => INSERT_OP_CODE,
            Op::Delete => DELETE_OP_CODE,
            Op::UpdateDelete => UPDATE_DELETE_OP_CODE,
            Op::UpdateInsert => UPDATE_INSERT_OP_CODE,
        };
        let extended_row = pk
            .clone()
            .chain([Some(ScalarImpl::Int16(op_code))])
            .chain(row);
        let vnode = compute_vnode(&extended_row, &self.dist_key_indices, &self.vnodes);
        let key_bytes = serialize_pk_with_vnode(&pk, &self.pk_serde, vnode);
        let value_bytes = self.row_serde.serialize(extended_row).into();
        (vnode, key_bytes, value_bytes)
    }

    pub(crate) fn serialize_barrier(
        &self,
        epoch: u64,
        vnode: VirtualNode,
        is_checkpoint: bool,
    ) -> (TableKey<Bytes>, Bytes) {
        let pk = [Some(ScalarImpl::Int64(Self::encode_epoch(epoch))), None];

        let op_code = if is_checkpoint {
            CHECKPOINT_BARRIER_OP_CODE
        } else {
            BARRIER_OP_CODE
        };

        let extended_row = pk
            .clone()
            .chain([Some(ScalarImpl::Int16(op_code))])
            .chain(OwnedRow::new(vec![None; self.payload_schema.len()]));
        let key_bytes = serialize_pk_with_vnode(&pk, &self.pk_serde, vnode);
        let value_bytes = self.row_serde.serialize(extended_row).into();
        (key_bytes, value_bytes)
    }

    pub(crate) fn serialize_epoch(&self, epoch: u64) -> Bytes {
        serialize_pk(
            [Some(ScalarImpl::Int64(Self::encode_epoch(epoch)))],
            &self.epoch_serde,
        )
    }

    pub(crate) fn serialize_log_store_pk(
        &self,
        vnode: VirtualNode,
        epoch: u64,
        seq_id: Option<SeqIdType>,
    ) -> TableKey<Bytes> {
        serialize_pk_with_vnode(
            [
                Some(ScalarImpl::Int64(Self::encode_epoch(epoch))),
                seq_id.map(ScalarImpl::Int32),
            ],
            &self.pk_serde,
            vnode,
        )
    }

    pub(crate) fn serialize_truncation_offset_watermark(
        &self,
        vnode: VirtualNode,
        offset: ReaderTruncationOffsetType,
    ) -> Bytes {
        let (epoch, seq_id) = offset;
        let curr_offset = self.serialize_log_store_pk(vnode, epoch, seq_id);
        let ret = Bytes::from(next_key(&curr_offset));
        assert!(!ret.is_empty());
        ret
    }
}

impl LogStoreRowSerde {
    fn deserialize(&self, value_bytes: Bytes) -> LogStoreResult<(u64, LogStoreRowOp)> {
        let row_data = self.row_serde.deserialize(&value_bytes)?;

        let payload_row = OwnedRow::new(row_data[KV_LOG_STORE_PREDEFINED_COLUMNS.len()..].to_vec());
        let epoch = Self::decode_epoch(*row_data[EPOCH_COLUMN_INDEX].as_ref().unwrap().as_int64());
        let row_op_code = *row_data[ROW_OP_COLUMN_INDEX].as_ref().unwrap().as_int16();

        let op = match row_op_code {
            INSERT_OP_CODE => LogStoreRowOp::Row {
                op: Op::Insert,
                row: payload_row,
            },
            DELETE_OP_CODE => LogStoreRowOp::Row {
                op: Op::Delete,
                row: payload_row,
            },
            UPDATE_INSERT_OP_CODE => LogStoreRowOp::Row {
                op: Op::UpdateInsert,
                row: payload_row,
            },
            UPDATE_DELETE_OP_CODE => LogStoreRowOp::Row {
                op: Op::UpdateDelete,
                row: payload_row,
            },
            BARRIER_OP_CODE => {
                assert!(row_data[SEQ_ID_COLUMN_INDEX].is_none());
                LogStoreRowOp::Barrier {
                    is_checkpoint: false,
                }
            }
            CHECKPOINT_BARRIER_OP_CODE => {
                assert!(row_data[SEQ_ID_COLUMN_INDEX].is_none());
                LogStoreRowOp::Barrier {
                    is_checkpoint: true,
                }
            }
            _ => unreachable!("invalid row op code: {}", row_op_code),
        };
        Ok((epoch, op))
    }

    pub(crate) async fn deserialize_stream_chunk(
        &self,
        stream: impl StateStoreReadIterStream,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        expected_epoch: u64,
        metrics: &KvLogStoreReadMetrics,
    ) -> LogStoreResult<StreamChunk> {
        pin_mut!(stream);
        let size_bound = (end_seq_id - start_seq_id + 1) as usize;
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.payload_schema.clone(), size_bound + 1);
        let mut ops = Vec::with_capacity(size_bound);
        let mut read_info = ReadInfo::new();
        while let Some((key, value)) = stream.try_next().await? {
            read_info
                .read_one_row(key.user_key.table_key.estimated_size() + value.estimated_size());
            match self.deserialize(value)? {
                (epoch, LogStoreRowOp::Row { op, row }) => {
                    if epoch != expected_epoch {
                        return Err(anyhow!(
                            "decoded epoch {} not match expected epoch {}",
                            epoch,
                            expected_epoch
                        ));
                    }
                    ops.push(op);
                    if ops.len() > size_bound {
                        return Err(anyhow!(
                            "row count {} exceed size bound {}",
                            ops.len(),
                            size_bound
                        ));
                    }
                    assert!(data_chunk_builder.append_one_row(row).is_none());
                }
                (_, LogStoreRowOp::Barrier { .. }) => {
                    return Err(anyhow!("should not get barrier when decoding stream chunk"));
                }
            }
        }
        if ops.is_empty() {
            return Err(anyhow!(
                "should not get empty row when decoding stream chunk. start seq id: {}, end seq id {}",
                start_seq_id,
                end_seq_id)
            );
        }
        read_info.report(metrics);
        Ok(StreamChunk::from_parts(
            ops,
            data_chunk_builder
                .consume_all()
                .expect("should not be empty"),
        ))
    }
}

#[derive(Debug)]
enum StreamState {
    /// The stream has not emitted any row op yet.
    Uninitialized,
    /// All parallelism of stream are consuming row.
    AllConsumingRow { curr_epoch: u64 },
    /// Some parallelism has reached the barrier, and is waiting for other parallelism to reach the
    /// barrier.
    BarrierAligning {
        curr_epoch: u64,
        is_checkpoint: bool,
    },
    /// All parallelism has reached the barrier, and the barrier is emitted.
    BarrierEmitted { prev_epoch: u64 },
}

pub(crate) enum KvLogStoreItem {
    StreamChunk(StreamChunk),
    Barrier { is_checkpoint: bool },
}

type BoxPeekableLogStoreItemStream<S> = Pin<Box<Peekable<LogStoreItemStream<S>>>>;

struct LogStoreRowOpStream<S: StateStoreReadIterStream> {
    serde: LogStoreRowSerde,

    /// Streams that have not reached a barrier
    row_streams: FuturesUnordered<StreamFuture<BoxPeekableLogStoreItemStream<S>>>,

    /// Streams that have reached a barrier
    barrier_streams: Vec<BoxPeekableLogStoreItemStream<S>>,

    not_started_streams: Vec<(u64, BoxPeekableLogStoreItemStream<S>)>,

    stream_state: StreamState,

    metrics: KvLogStoreReadMetrics,
}

impl<S: StateStoreReadIterStream> LogStoreRowOpStream<S> {
    pub(crate) fn new(
        streams: Vec<S>,
        serde: LogStoreRowSerde,
        metrics: KvLogStoreReadMetrics,
    ) -> Self {
        assert!(!streams.is_empty());
        Self {
            serde: serde.clone(),
            barrier_streams: streams
                .into_iter()
                .map(|s| Box::pin(deserialize_stream(s, serde.clone()).peekable()))
                .collect(),
            row_streams: FuturesUnordered::new(),
            not_started_streams: Vec::new(),
            stream_state: StreamState::Uninitialized,
            metrics,
        }
    }

    fn check_is_checkpoint(&self, is_checkpoint: bool) -> LogStoreResult<()> {
        if let StreamState::BarrierAligning {
            is_checkpoint: curr_is_checkpoint,
            ..
        } = &self.stream_state
        {
            if is_checkpoint == *curr_is_checkpoint {
                Ok(())
            } else {
                Err(anyhow!(
                    "current aligning barrier is_checkpoint: {}, current barrier is_checkpoint {}",
                    curr_is_checkpoint,
                    is_checkpoint
                ))
            }
        } else {
            Ok(())
        }
    }

    #[try_stream(ok = (u64, KvLogStoreItem), error = anyhow::Error)]
    async fn into_log_store_item_stream(mut self, chunk_size: usize) {
        let mut ops = Vec::with_capacity(chunk_size);
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.serde.payload_schema.clone(), chunk_size);

        if !self.init().await? {
            // no data in all stream
            return Ok(());
        }

        let this = self;
        pin_mut!(this);

        while let Some((epoch, row_op, row_read_size)) = this.next_op().await? {
            let mut read_info = ReadInfo::new();
            read_info.read_one_row(row_read_size);
            match row_op {
                LogStoreRowOp::Row { op, row } => {
                    ops.push(op);
                    if let Some(chunk) = data_chunk_builder.append_one_row(row) {
                        let ops = replace(&mut ops, Vec::with_capacity(chunk_size));
                        read_info.report(&this.metrics);
                        yield (
                            epoch,
                            KvLogStoreItem::StreamChunk(StreamChunk::from_parts(ops, chunk)),
                        );
                    }
                }
                LogStoreRowOp::Barrier { is_checkpoint } => {
                    read_info.report(&this.metrics);
                    if let Some(chunk) = data_chunk_builder.consume_all() {
                        let ops = replace(&mut ops, Vec::with_capacity(chunk_size));
                        yield (
                            epoch,
                            KvLogStoreItem::StreamChunk(StreamChunk::from_parts(ops, chunk)),
                        );
                    }
                    yield (epoch, KvLogStoreItem::Barrier { is_checkpoint })
                }
            }
        }
    }
}

pub(crate) type LogStoreItemMergeStream<S> =
    impl Stream<Item = LogStoreResult<(u64, KvLogStoreItem)>>;
pub(crate) fn merge_log_store_item_stream<S: StateStoreReadIterStream>(
    streams: Vec<S>,
    serde: LogStoreRowSerde,
    chunk_size: usize,
    metrics: KvLogStoreReadMetrics,
) -> LogStoreItemMergeStream<S> {
    LogStoreRowOpStream::new(streams, serde, metrics).into_log_store_item_stream(chunk_size)
}

type LogStoreItemStream<S: StateStoreReadIterStream> =
    impl Stream<Item = LogStoreResult<(u64, LogStoreRowOp, usize)>> + Send;
fn deserialize_stream<S: StateStoreReadIterStream>(
    stream: S,
    serde: LogStoreRowSerde,
) -> LogStoreItemStream<S> {
    stream.map(
        move |result: Result<_, StorageError>| -> LogStoreResult<(u64, LogStoreRowOp, usize)> {
            match result {
                Ok((key, value)) => {
                    let read_size =
                        key.user_key.table_key.estimated_size() + value.estimated_size();
                    let (epoch, op) = serde.deserialize(value)?;
                    Ok((epoch, op, read_size))
                }
                Err(e) => Err(e.into()),
            }
        },
    )
}

impl<S: StateStoreReadIterStream> LogStoreRowOpStream<S> {
    // Return Ok(false) means all streams have reach the end.
    async fn init(&mut self) -> LogStoreResult<bool> {
        match &self.stream_state {
            StreamState::Uninitialized => {}
            _ => unreachable!("cannot call init for twice"),
        };

        // before init, all streams are in `barrier_streams`
        assert!(
            self.row_streams.is_empty(),
            "when uninitialized, row_streams should be empty"
        );
        assert!(self.not_started_streams.is_empty());
        assert!(!self.barrier_streams.is_empty());

        for mut stream in self.barrier_streams.drain(..) {
            match stream.as_mut().peek().await {
                Some(Ok((epoch, _, _))) => {
                    self.not_started_streams.push((*epoch, stream));
                }
                Some(Err(_)) => match stream.next().await {
                    Some(Err(e)) => {
                        return Err(e);
                    }
                    _ => unreachable!("on peek we have checked it's Some(Err(_))"),
                },
                None => {
                    continue;
                }
            }
        }

        if self.not_started_streams.is_empty() {
            // No stream has data
            return Ok(false);
        }

        // sorted by epoch descending. Earlier epoch at the end
        self.not_started_streams
            .sort_by_key(|(epoch, _)| HummockEpoch::MAX - *epoch);

        let (epoch, stream) = self
            .not_started_streams
            .pop()
            .expect("have check non-empty");
        self.row_streams.push(stream.into_future());
        while let Some((stream_epoch, _)) = self.not_started_streams.last()
            && *stream_epoch == epoch
        {
            let (_, stream) = self.not_started_streams.pop().expect("should not be empty");
            self.row_streams.push(stream.into_future());
        }
        self.stream_state = StreamState::AllConsumingRow { curr_epoch: epoch };
        Ok(true)
    }

    fn may_init_epoch(&mut self, epoch: u64) -> LogStoreResult<()> {
        let prev_epoch = match &self.stream_state {
            StreamState::Uninitialized => unreachable!("should have init"),
            StreamState::BarrierEmitted { prev_epoch } => *prev_epoch,
            StreamState::AllConsumingRow { curr_epoch }
            | StreamState::BarrierAligning { curr_epoch, .. } => {
                return if *curr_epoch != epoch {
                    Err(anyhow!(
                        "epoch {} does not match with current epoch {}",
                        epoch,
                        curr_epoch
                    ))
                } else {
                    Ok(())
                };
            }
        };

        if prev_epoch >= epoch {
            return Err(anyhow!(
                "epoch {} should be greater than prev epoch {}",
                epoch,
                prev_epoch
            ));
        }

        while let Some((stream_epoch, _)) = self.not_started_streams.last() {
            if *stream_epoch > epoch {
                // Current epoch has not reached the first epoch of
                // the stream. Later streams must also have greater epoch, so break here.
                break;
            }
            if *stream_epoch < epoch {
                return Err(anyhow!(
                    "current epoch {} has exceed epoch {} of stream not started",
                    epoch,
                    stream_epoch
                ));
            }
            let (_, stream) = self.not_started_streams.pop().expect("should not be empty");
            self.row_streams.push(stream.into_future());
        }

        self.stream_state = StreamState::AllConsumingRow { curr_epoch: epoch };
        Ok(())
    }

    async fn next_op(&mut self) -> LogStoreResult<Option<(u64, LogStoreRowOp, usize)>> {
        while let (Some(result), stream) = self
            .row_streams
            .next()
            .await
            .expect("row stream should not be empty when polled")
        {
            let (decoded_epoch, op, read_size) = result?;
            self.may_init_epoch(decoded_epoch)?;
            match op {
                LogStoreRowOp::Row { op, row } => {
                    self.row_streams.push(stream.into_future());
                    return Ok(Some((
                        decoded_epoch,
                        LogStoreRowOp::Row { op, row },
                        read_size,
                    )));
                }
                LogStoreRowOp::Barrier { is_checkpoint } => {
                    self.check_is_checkpoint(is_checkpoint)?;
                    // Put the current stream to the barrier streams
                    self.barrier_streams.push(stream);

                    if self.row_streams.is_empty() {
                        self.stream_state = StreamState::BarrierEmitted {
                            prev_epoch: decoded_epoch,
                        };
                        while let Some(stream) = self.barrier_streams.pop() {
                            self.row_streams.push(stream.into_future());
                        }
                        return Ok(Some((
                            decoded_epoch,
                            LogStoreRowOp::Barrier { is_checkpoint },
                            read_size,
                        )));
                    } else {
                        self.stream_state = StreamState::BarrierAligning {
                            curr_epoch: decoded_epoch,
                            is_checkpoint,
                        };
                        continue;
                    }
                }
            }
        }
        // End of stream
        match &self.stream_state {
            StreamState::BarrierEmitted { .. } => {},
            s => return Err(
                anyhow!(
                    "when any of the stream reaches the end, it should be right after emitting an barrier. Current state: {:?}",
                    s
                )
            ),
        }
        assert!(
            self.barrier_streams.is_empty(),
            "should not have any pending barrier received stream after barrier emit"
        );
        if !self.not_started_streams.is_empty() {
            return Err(anyhow!(
                "a stream has reached the end but some other stream has not started yet"
            ));
        }
        if cfg!(debug_assertion) {
            while let Some((opt, _stream)) = self.row_streams.next().await {
                if let Some(result) = opt {
                    return Err(
                        anyhow!("when any of the stream reaches the end, other stream should also reaches the end, but poll result: {:?}", result)
                    );
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::task::Poll;

    use bytes::Bytes;
    use futures::stream::empty;
    use futures::{pin_mut, stream, StreamExt, TryStreamExt};
    use itertools::Itertools;
    use rand::prelude::SliceRandom;
    use rand::thread_rng;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::buffer::Bitmap;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::DataType;
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_hummock_sdk::key::FullKey;
    use risingwave_storage::store::StateStoreReadIterStream;
    use risingwave_storage::table::DEFAULT_VNODE;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::Sender;

    use crate::common::log_store_impl::kv_log_store::serde::{
        merge_log_store_item_stream, KvLogStoreItem, LogStoreRowOp, LogStoreRowOpStream,
        LogStoreRowSerde,
    };
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        check_rows_eq, gen_test_data, gen_test_log_store_table, TEST_TABLE_ID,
    };
    use crate::common::log_store_impl::kv_log_store::{KvLogStoreReadMetrics, SeqIdType};

    const EPOCH0: u64 = 233;
    const EPOCH1: u64 = EPOCH0 + 1;
    const EPOCH2: u64 = EPOCH1 + 1;

    #[test]
    fn test_serde() {
        let table = gen_test_log_store_table();

        let serde = LogStoreRowSerde::new(&table, Some(Arc::new(Bitmap::ones(VirtualNode::COUNT))));

        let (ops, rows) = gen_test_data(0);

        let mut builder = DataChunkBuilder::new(vec![DataType::Int64, DataType::Varchar], 1000000);
        for row in &rows {
            assert!(builder.append_one_row(row).is_none());
        }
        let data_chunk = builder.consume_all().unwrap();
        let stream_chunk = StreamChunk::from_parts(ops, data_chunk);

        let mut epoch = 233u64;

        let mut serialized_keys = vec![];
        let mut seq_id = 1;

        fn remove_vnode_prefix(key: &Bytes) -> Bytes {
            key.slice(VirtualNode::SIZE..)
        }
        let delete_range_right1 = remove_vnode_prefix(
            &serde.serialize_truncation_offset_watermark(DEFAULT_VNODE, (epoch, None)),
        );

        for (op, row) in stream_chunk.rows() {
            let (_, key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
            let key = remove_vnode_prefix(&key.0);
            assert!(key < delete_range_right1);
            serialized_keys.push(key);
            let (decoded_epoch, row_op) = serde.deserialize(value).unwrap();
            assert_eq!(decoded_epoch, epoch);
            match row_op {
                LogStoreRowOp::Row {
                    op: deserialized_op,
                    row: deserialized_row,
                } => {
                    assert_eq!(&op, &deserialized_op);
                    assert_eq!(row.to_owned_row(), deserialized_row);
                }
                LogStoreRowOp::Barrier { .. } => unreachable!(),
            }
            seq_id += 1;
        }

        let (key, encoded_barrier) = serde.serialize_barrier(epoch, DEFAULT_VNODE, false);
        let key = remove_vnode_prefix(&key.0);
        match serde.deserialize(encoded_barrier).unwrap() {
            (decoded_epoch, LogStoreRowOp::Barrier { is_checkpoint }) => {
                assert!(!is_checkpoint);
                assert_eq!(decoded_epoch, epoch);
            }
            _ => unreachable!(),
        }
        assert!(key.as_ref() < delete_range_right1);
        serialized_keys.push(key);

        seq_id = 1;
        epoch += 1;

        let delete_range_right2 = remove_vnode_prefix(
            &serde.serialize_truncation_offset_watermark(DEFAULT_VNODE, (epoch, None)),
        );

        for (op, row) in stream_chunk.rows() {
            let (_, key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
            let key = remove_vnode_prefix(&key.0);
            assert!(key >= delete_range_right1);
            assert!(key < delete_range_right2);
            serialized_keys.push(key);
            let (decoded_epoch, row_op) = serde.deserialize(value).unwrap();
            assert_eq!(decoded_epoch, epoch);
            match row_op {
                LogStoreRowOp::Row {
                    op: deserialized_op,
                    row: deserialized_row,
                } => {
                    assert_eq!(&op, &deserialized_op);
                    assert_eq!(row.to_owned_row(), deserialized_row);
                }
                LogStoreRowOp::Barrier { .. } => unreachable!(),
            }
            seq_id += 1;
        }

        let (key, encoded_checkpoint_barrier) = serde.serialize_barrier(epoch, DEFAULT_VNODE, true);
        let key = remove_vnode_prefix(&key.0);
        match serde.deserialize(encoded_checkpoint_barrier).unwrap() {
            (decoded_epoch, LogStoreRowOp::Barrier { is_checkpoint }) => {
                assert_eq!(decoded_epoch, epoch);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }
        assert!(key.as_ref() >= delete_range_right1);
        assert!(key.as_ref() < delete_range_right2);
        serialized_keys.push(key);

        assert_eq!(serialized_keys.len(), 2 * rows.len() + 2);
        assert!(serialized_keys.is_sorted());
    }

    #[test]
    fn test_encode_epoch() {
        let epochs = vec![
            u64::MIN,
            1u64,
            2,
            3,
            1 + (1 << 63),
            2 + (1 << 63),
            3 + (1 << 63),
            u64::MAX,
        ];
        assert!(epochs.is_sorted());
        let encoded_epochs = epochs
            .iter()
            .map(|epoch| LogStoreRowSerde::encode_epoch(*epoch))
            .collect_vec();
        assert!(encoded_epochs.is_sorted());
        assert_eq!(
            epochs,
            encoded_epochs
                .into_iter()
                .map(LogStoreRowSerde::decode_epoch)
                .collect_vec()
        );
    }

    #[tokio::test]
    async fn test_deserialize_stream_chunk() {
        let table = gen_test_log_store_table();
        let serde = LogStoreRowSerde::new(&table, Some(Arc::new(Bitmap::ones(VirtualNode::COUNT))));
        let (ops, rows) = gen_test_data(0);

        let mut seq_id = 1;
        let start_seq_id = seq_id;

        let (stream, tx) = gen_row_stream(
            serde.clone(),
            ops.clone(),
            rows.clone(),
            EPOCH1,
            &mut seq_id,
        );
        let end_seq_id = seq_id - 1;
        tx.send(()).unwrap();
        let chunk = serde
            .deserialize_stream_chunk(
                stream,
                start_seq_id,
                end_seq_id,
                EPOCH1,
                &KvLogStoreReadMetrics::for_test(),
            )
            .await
            .unwrap();
        for (i, (op, row)) in chunk.rows().enumerate() {
            assert_eq!(ops[i], op);
            assert_eq!(rows[i], row.to_owned_row());
        }
    }

    fn gen_row_stream(
        serde: LogStoreRowSerde,
        ops: Vec<Op>,
        rows: Vec<OwnedRow>,
        epoch: u64,
        seq_id: &mut SeqIdType,
    ) -> (impl StateStoreReadIterStream, Sender<()>) {
        let (tx, rx) = oneshot::channel();
        let row_data = ops
            .into_iter()
            .zip_eq(rows)
            .map(|(op, row)| {
                let (_, key, value) = serde.serialize_data_row(epoch, *seq_id, op, row);
                *seq_id += 1;
                Ok((FullKey::new(TEST_TABLE_ID, key, epoch), value))
            })
            .collect_vec();
        (
            stream::once(async move {
                rx.await.unwrap();
                stream::iter(row_data)
            })
            .flatten(),
            tx,
        )
    }

    fn gen_single_test_stream(
        serde: LogStoreRowSerde,
        seq_id: &mut SeqIdType,
        base: i64,
    ) -> (
        impl StateStoreReadIterStream,
        oneshot::Sender<()>,
        oneshot::Sender<()>,
        Vec<Op>,
        Vec<OwnedRow>,
    ) {
        let (ops, rows) = gen_test_data(base);
        let first_barrier = {
            let (key, value) = serde.serialize_barrier(EPOCH0, DEFAULT_VNODE, true);
            Ok((FullKey::new(TEST_TABLE_ID, key, EPOCH0), value))
        };
        let stream = stream::once(async move { first_barrier });
        let (row_stream, tx1) =
            gen_row_stream(serde.clone(), ops.clone(), rows.clone(), EPOCH1, seq_id);
        let stream = stream.chain(row_stream);
        let stream = stream.chain(stream::once({
            let serde = serde.clone();
            async move {
                let (key, value) = serde.serialize_barrier(EPOCH1, DEFAULT_VNODE, false);
                Ok((FullKey::new(TEST_TABLE_ID, key, EPOCH1), value))
            }
        }));
        let (row_stream, tx2) =
            gen_row_stream(serde.clone(), ops.clone(), rows.clone(), EPOCH2, seq_id);
        let stream = stream.chain(row_stream).chain(stream::once({
            async move {
                let (key, value) = serde.serialize_barrier(EPOCH2, DEFAULT_VNODE, true);
                Ok((FullKey::new(TEST_TABLE_ID, key, EPOCH2), value))
            }
        }));
        (stream, tx1, tx2, ops, rows)
    }

    #[allow(clippy::type_complexity)]
    fn gen_multi_test_stream(
        serde: LogStoreRowSerde,
        size: usize,
    ) -> (
        LogStoreRowOpStream<impl StateStoreReadIterStream>,
        Vec<Option<Sender<()>>>,
        Vec<Option<Sender<()>>>,
        Vec<Vec<Op>>,
        Vec<Vec<OwnedRow>>,
    ) {
        let mut seq_id = 1;
        let mut streams = Vec::new();
        let mut tx1 = Vec::new();
        let mut tx2 = Vec::new();
        let mut ops = Vec::new();
        let mut rows = Vec::new();
        for i in 0..size {
            let (s, t1, t2, op_list, row_list) =
                gen_single_test_stream(serde.clone(), &mut seq_id, (100 * i) as _);
            streams.push(s);
            tx1.push(Some(t1));
            tx2.push(Some(t2));
            ops.push(op_list);
            rows.push(row_list);
        }

        let stream = LogStoreRowOpStream::new(streams, serde, KvLogStoreReadMetrics::for_test());

        for i in 0..size {
            let (o, r) = gen_test_data((100 * i) as _);
            ops.push(o);
            rows.push(r);
        }

        (stream, tx1, tx2, ops, rows)
    }

    #[tokio::test]
    async fn test_row_stream_basic() {
        let table = gen_test_log_store_table();

        let serde = LogStoreRowSerde::new(&table, Some(Arc::new(Bitmap::ones(VirtualNode::COUNT))));

        const MERGE_SIZE: usize = 10;

        let (mut stream, mut tx1, mut tx2, ops, rows) = gen_multi_test_stream(serde, MERGE_SIZE);

        stream.init().await.unwrap();

        pin_mut!(stream);

        let (epoch, op, _) = stream.next_op().await.unwrap().unwrap();

        assert_eq!(
            (
                EPOCH0,
                LogStoreRowOp::Barrier {
                    is_checkpoint: true
                }
            ),
            (epoch, op)
        );

        let mut index = (0..MERGE_SIZE).collect_vec();
        index.shuffle(&mut thread_rng());

        for i in index {
            tx1[i].take().unwrap().send(()).unwrap();
            for j in 0..ops[i].len() {
                let (epoch, op, _) = stream.next_op().await.unwrap().unwrap();
                assert_eq!(
                    (
                        EPOCH1,
                        LogStoreRowOp::Row {
                            op: ops[i][j],
                            row: rows[i][j].clone(),
                        }
                    ),
                    (epoch, op)
                );
            }
        }

        let (epoch, op, _) = stream.next_op().await.unwrap().unwrap();

        assert_eq!(
            (
                EPOCH1,
                LogStoreRowOp::Barrier {
                    is_checkpoint: false
                }
            ),
            (epoch, op)
        );

        let mut index = (0..MERGE_SIZE).collect_vec();
        index.shuffle(&mut thread_rng());

        for i in index {
            tx2[i].take().unwrap().send(()).unwrap();
            for j in 0..ops[i].len() {
                let (epoch, op, _) = stream.next_op().await.unwrap().unwrap();
                assert_eq!(
                    (
                        EPOCH2,
                        LogStoreRowOp::Row {
                            op: ops[i][j],
                            row: rows[i][j].clone(),
                        }
                    ),
                    (epoch, op)
                );
            }
        }

        let (epoch, op, _) = stream.next_op().await.unwrap().unwrap();
        assert_eq!(
            (
                EPOCH2,
                LogStoreRowOp::Barrier {
                    is_checkpoint: true,
                }
            ),
            (epoch, op)
        );

        assert!(stream.next_op().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_log_store_stream_basic() {
        let table = gen_test_log_store_table();

        let serde = LogStoreRowSerde::new(&table, Some(Arc::new(Bitmap::ones(VirtualNode::COUNT))));

        let mut seq_id = 1;
        let (stream, tx1, tx2, ops, rows) = gen_single_test_stream(serde.clone(), &mut seq_id, 0);

        const CHUNK_SIZE: usize = 3;

        let stream = merge_log_store_item_stream(
            vec![stream],
            serde,
            CHUNK_SIZE,
            KvLogStoreReadMetrics::for_test(),
        );

        pin_mut!(stream);

        let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
        assert_eq!(EPOCH0, epoch);
        match item {
            KvLogStoreItem::StreamChunk(_) => unreachable!(),
            KvLogStoreItem::Barrier { is_checkpoint } => {
                assert!(is_checkpoint);
            }
        }

        assert!(poll_fn(|cx| Poll::Ready(stream.poll_next_unpin(cx)))
            .await
            .is_pending());

        tx1.send(()).unwrap();

        {
            let mut remain = ops.len();
            while remain > 0 {
                let size = min(remain, CHUNK_SIZE);
                let start_index = ops.len() - remain;
                remain -= size;
                let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
                assert_eq!(EPOCH1, epoch);
                match item {
                    KvLogStoreItem::StreamChunk(chunk) => {
                        assert_eq!(chunk.cardinality(), size);
                        assert!(check_rows_eq(
                            chunk.rows(),
                            (start_index..(start_index + size)).map(|i| (ops[i], &rows[i]))
                        ));
                    }
                    _ => unreachable!(),
                }
            }
        }

        let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
        assert_eq!(EPOCH1, epoch);
        match item {
            KvLogStoreItem::StreamChunk(_) => unreachable!(),
            KvLogStoreItem::Barrier { is_checkpoint } => {
                assert!(!is_checkpoint);
            }
        }

        assert!(poll_fn(|cx| Poll::Ready(stream.poll_next_unpin(cx)))
            .await
            .is_pending());

        tx2.send(()).unwrap();

        {
            let mut remain = ops.len();
            while remain > 0 {
                let size = min(remain, CHUNK_SIZE);
                let start_index = ops.len() - remain;
                remain -= size;
                let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
                assert_eq!(EPOCH2, epoch);
                match item {
                    KvLogStoreItem::StreamChunk(chunk) => {
                        assert_eq!(chunk.cardinality(), size);
                        assert!(check_rows_eq(
                            chunk.rows(),
                            (start_index..(start_index + size)).map(|i| (ops[i], &rows[i]))
                        ));
                    }
                    _ => unreachable!(),
                }
            }
        }

        let (epoch, item): (_, KvLogStoreItem) = stream.try_next().await.unwrap().unwrap();
        assert_eq!(EPOCH2, epoch);
        match item {
            KvLogStoreItem::StreamChunk(_) => unreachable!(),
            KvLogStoreItem::Barrier { is_checkpoint } => {
                assert!(is_checkpoint);
            }
        }

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let table = gen_test_log_store_table();

        let serde = LogStoreRowSerde::new(&table, Some(Arc::new(Bitmap::ones(VirtualNode::COUNT))));

        const CHUNK_SIZE: usize = 3;

        let stream = merge_log_store_item_stream(
            vec![empty(), empty()],
            serde,
            CHUNK_SIZE,
            KvLogStoreReadMetrics::for_test(),
        );

        pin_mut!(stream);

        assert!(stream.next().await.is_none());
    }
}
