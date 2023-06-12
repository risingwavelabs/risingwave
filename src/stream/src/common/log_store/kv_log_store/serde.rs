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

use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use futures::{pin_mut, TryStreamExt};
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::{
    BasicSerde, ValueRowDeserializer, ValueRowSerializer,
};
use risingwave_hummock_sdk::key::next_key;
use risingwave_pb::catalog::Table;
use risingwave_storage::row_serde::row_serde_util::serialize_pk_with_vnode;
use risingwave_storage::store::StateStoreReadIterStream;
use risingwave_storage::table::{compute_vnode, Distribution};
use risingwave_storage::value_serde::ValueRowSerdeNew;

use crate::common::log_store::kv_log_store::{
    ReaderTruncationOffsetType, RowOpCodeType, SeqIdType,
};
use crate::common::log_store::{LogStoreError, LogStoreResult};

/// `epoch`, `seq_id`, `op`
const PREDEFINED_COLUMNS_TYPES: [DataType; 3] = [DataType::Int64, DataType::Int32, DataType::Int16];
const EPOCH_INDEX: usize = 0;
const SEQ_ID_INDEX: usize = 1;
const ROW_OP_INDEX: usize = 2;
/// `epoch`, `seq_id`
const PK_TYPES: [DataType; 2] = [DataType::Int64, DataType::Int32];
/// epoch
const EPOCH_TYPES: [DataType; 1] = [DataType::Int64];

const INSERT_OP_CODE: RowOpCodeType = 1;
const DELETE_OP_CODE: RowOpCodeType = 2;
const UPDATE_INSERT_OP_CODE: RowOpCodeType = 3;
const UPDATE_DELETE_OP_CODE: RowOpCodeType = 4;
const BARRIER_OP_CODE: RowOpCodeType = 5;
const CHECKPOINT_BARRIER_OP_CODE: RowOpCodeType = 6;

#[derive(Eq, PartialEq, Debug)]
enum LogStoreRowOp {
    Row { op: Op, row: OwnedRow },
    Barrier { is_checkpoint: bool },
}

#[derive(Clone)]
pub struct LogStoreRowSerde {
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
    pub fn new(table_catalog: &Table, vnodes: Option<Arc<Bitmap>>) -> Self {
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
        assert!(data_types.len() > PREDEFINED_COLUMNS_TYPES.len());
        for i in 0..PREDEFINED_COLUMNS_TYPES.len() {
            assert_eq!(data_types[i], PREDEFINED_COLUMNS_TYPES[i]);
        }

        let payload_schema = data_types[PREDEFINED_COLUMNS_TYPES.len()..].to_vec();

        let row_serde = BasicSerde::new(
            Arc::from_iter(std::iter::empty()),
            Arc::from(data_types.into_boxed_slice()),
            Arc::from_iter(std::iter::empty()),
        );

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
            OrderedRowSerde::new(Vec::from(EPOCH_TYPES), vec![OrderType::ascending()]);

        Self {
            pk_serde,
            row_serde,
            epoch_serde,
            dist_key_indices,
            vnodes,
            payload_schema,
        }
    }

    pub fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) {
        self.vnodes = vnodes;
    }

    pub fn vnodes(&self) -> &Bitmap {
        self.vnodes.as_ref()
    }

    fn encode_epoch(epoch: u64) -> i64 {
        epoch as i64 ^ (1i64 << 63)
    }

    fn decode_epoch(encoded_epoch: i64) -> u64 {
        encoded_epoch as u64 ^ (1u64 << 63)
    }
}

impl LogStoreRowSerde {
    pub fn serialize_data_row(
        &self,
        epoch: u64,
        seq_id: SeqIdType,
        op: Op,
        row: impl Row,
    ) -> (VirtualNode, Bytes, Bytes) {
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

    pub fn serialize_barrier(
        &self,
        epoch: u64,
        vnode: VirtualNode,
        is_checkpoint: bool,
    ) -> (Bytes, Bytes) {
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

    pub fn serialize_epoch(&self, vnode: VirtualNode, epoch: u64) -> Bytes {
        serialize_pk_with_vnode(
            [Some(ScalarImpl::Int64(Self::encode_epoch(epoch)))],
            &self.epoch_serde,
            vnode,
        )
    }

    pub fn serialize_log_store_pk(
        &self,
        vnode: VirtualNode,
        epoch: u64,
        seq_id: SeqIdType,
    ) -> Bytes {
        serialize_pk_with_vnode(
            [
                Some(ScalarImpl::Int64(Self::encode_epoch(epoch))),
                Some(ScalarImpl::Int32(seq_id)),
            ],
            &self.pk_serde,
            vnode,
        )
    }

    pub fn serialize_truncation_offset_watermark(
        &self,
        vnode: VirtualNode,
        offset: ReaderTruncationOffsetType,
    ) -> Bytes {
        let curr_offset = self.serialize_epoch(vnode, offset);
        let ret = Bytes::from(next_key(&curr_offset));
        assert!(!ret.is_empty());
        ret
    }
}

impl LogStoreRowSerde {
    fn deserialize(&self, value_bytes: Bytes) -> LogStoreResult<(u64, LogStoreRowOp)> {
        let row_data = self.row_serde.deserialize(&value_bytes)?;

        let payload_row = OwnedRow::new(row_data[PREDEFINED_COLUMNS_TYPES.len()..].to_vec());
        let epoch = Self::decode_epoch(*row_data[EPOCH_INDEX].as_ref().unwrap().as_int64());
        let row_op_code = *row_data[ROW_OP_INDEX].as_ref().unwrap().as_int16();

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
                assert!(row_data[SEQ_ID_INDEX].is_none());
                LogStoreRowOp::Barrier {
                    is_checkpoint: false,
                }
            }
            CHECKPOINT_BARRIER_OP_CODE => {
                assert!(row_data[SEQ_ID_INDEX].is_none());
                LogStoreRowOp::Barrier {
                    is_checkpoint: true,
                }
            }
            _ => unreachable!("invalid row op code: {}", row_op_code),
        };
        Ok((epoch, op))
    }

    pub async fn deserialize_stream_chunk(
        &self,
        stream: impl StateStoreReadIterStream,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        expected_epoch: u64,
    ) -> LogStoreResult<StreamChunk> {
        pin_mut!(stream);
        let size_bound = (end_seq_id - start_seq_id + 1) as usize;
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.payload_schema.clone(), size_bound + 1);
        let mut ops = Vec::with_capacity(size_bound);
        while let Some((_, value)) = stream.try_next().await? {
            match self.deserialize(value)? {
                (epoch, LogStoreRowOp::Row { op, row }) => {
                    if epoch != expected_epoch {
                        return Err(LogStoreError::Internal(anyhow!(
                            "decoded epoch {} not match expected epoch {}",
                            epoch,
                            expected_epoch
                        )));
                    }
                    ops.push(op);
                    if ops.len() > size_bound {
                        return Err(LogStoreError::Internal(anyhow!(
                            "row count {} exceed size bound {}",
                            ops.len(),
                            size_bound
                        )));
                    }
                    assert!(data_chunk_builder.append_one_row(row).is_none());
                }
                (_, LogStoreRowOp::Barrier { .. }) => {
                    return Err(LogStoreError::Internal(anyhow!(
                        "should not get barrier when decoding stream chunk"
                    )));
                }
            }
        }
        if ops.is_empty() {
            return Err(LogStoreError::Internal(anyhow!(
                "should not get empty row when decoding stream chunk. start seq id: {}, end seq id {}",
                start_seq_id,
                end_seq_id))
            );
        }
        Ok(StreamChunk::from_parts(
            ops,
            data_chunk_builder
                .consume_all()
                .expect("should not be empty"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use futures::{stream, StreamExt};
    use itertools::Itertools;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, ScalarImpl, ScalarRef};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};
    use risingwave_pb::catalog::PbTable;
    use risingwave_storage::store::StateStoreReadIterStream;
    use risingwave_storage::table::DEFAULT_VNODE;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::Sender;

    use crate::common::log_store::kv_log_store::serde::{LogStoreRowOp, LogStoreRowSerde};
    use crate::common::log_store::kv_log_store::SeqIdType;
    use crate::common::table::test_utils::gen_prost_table;

    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    const EPOCH1: u64 = 233;

    fn gen_test_table() -> PbTable {
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::from(0), DataType::Int64), // epoch
            ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32), // Seq id
            ColumnDesc::unnamed(ColumnId::from(2), DataType::Int16), // op code
            // Payload
            ColumnDesc::unnamed(ColumnId::from(3), DataType::Int64), // id
            ColumnDesc::unnamed(ColumnId::from(2), DataType::Varchar), // name
        ];
        let order_types = vec![OrderType::ascending(), OrderType::ascending_nulls_last()];
        let pk_index = vec![0_usize, 1_usize];
        let read_prefix_len_hint = 0;
        gen_prost_table(
            TEST_TABLE_ID,
            column_descs,
            order_types,
            pk_index,
            read_prefix_len_hint,
        )
    }

    fn gen_test_data(base: i64) -> (Vec<Op>, Vec<OwnedRow>) {
        let ops = vec![Op::Insert, Op::Delete, Op::UpdateDelete, Op::UpdateInsert];
        let rows = vec![
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(1 + base)),
                Some(ScalarImpl::Utf8("name1".to_owned_scalar())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(2 + base)),
                Some(ScalarImpl::Utf8("name2".to_owned_scalar())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(3 + base)),
                Some(ScalarImpl::Utf8("name3".to_owned_scalar())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(3 + base)),
                Some(ScalarImpl::Utf8("name4".to_owned_scalar())),
            ]),
        ];
        (ops, rows)
    }

    #[test]
    fn test_serde() {
        let table = gen_test_table();

        let serde = LogStoreRowSerde::new(&table, None);

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

        let delete_range_right1 = serde.serialize_truncation_offset_watermark(DEFAULT_VNODE, epoch);

        for (op, row) in stream_chunk.rows() {
            let (_, key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
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
        match serde.deserialize(encoded_barrier).unwrap() {
            (decoded_epoch, LogStoreRowOp::Barrier { is_checkpoint }) => {
                assert!(!is_checkpoint);
                assert_eq!(decoded_epoch, epoch);
            }
            _ => unreachable!(),
        }
        assert!(key < delete_range_right1);
        serialized_keys.push(key);

        seq_id = 1;
        epoch += 1;

        let delete_range_right2 = serde.serialize_truncation_offset_watermark(DEFAULT_VNODE, epoch);

        for (op, row) in stream_chunk.rows() {
            let (_, key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
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
        match serde.deserialize(encoded_checkpoint_barrier).unwrap() {
            (decoded_epoch, LogStoreRowOp::Barrier { is_checkpoint }) => {
                assert_eq!(decoded_epoch, epoch);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }
        assert!(key >= delete_range_right1);
        assert!(key < delete_range_right2);
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
        let table = gen_test_table();
        let serde = LogStoreRowSerde::new(&table, None);

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
            .deserialize_stream_chunk(stream, start_seq_id, end_seq_id, EPOCH1)
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
            .zip_eq(rows.into_iter())
            .map(|(op, row)| {
                let (_, key, value) = serde.serialize_data_row(epoch, *seq_id, op, row);
                *seq_id += 1;
                Ok((FullKey::new(TEST_TABLE_ID, TableKey(key), epoch), value))
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
}
