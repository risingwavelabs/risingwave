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

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::array::{Op, RowRef};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::{
    BasicSerde, ValueRowDeserializer, ValueRowSerdeNew, ValueRowSerializer,
};
use risingwave_hummock_sdk::key::next_key;
use risingwave_pb::catalog::Table;
use risingwave_storage::row_serde::row_serde_util::serialize_pk_with_vnode;
use risingwave_storage::table::{compute_vnode, Distribution};

use crate::common::log_store::kv_log_store::{
    LogStoreRowOp, ReaderTruncationOffsetType, RowOpCodeType, SeqIdType,
};

/// `epoch`, `seq_id`, `op`
const PREDEFINED_COLUMNS_TYPES: [DataType; 3] = [DataType::Int64, DataType::Int32, DataType::Int16];
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

    /// The number of columns in the payload, i.e. columns other than the predefined columns in the
    /// catalog table schema.
    payload_col_count: usize,
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
        assert!(data_types.len() > PREDEFINED_COLUMNS_TYPES.len());
        for i in 0..PREDEFINED_COLUMNS_TYPES.len() {
            assert_eq!(data_types[i], PREDEFINED_COLUMNS_TYPES[i]);
        }
        let payload_col_count = data_types.len() - 3;

        let row_serde = BasicSerde::new(&[], Arc::from(data_types.into_boxed_slice()));

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
            payload_col_count,
        }
    }

    pub(crate) fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) {
        self.vnodes = vnodes;
    }

    pub(crate) fn vnodes(&self) -> &Bitmap {
        self.vnodes.as_ref()
    }
}

impl LogStoreRowSerde {
    pub(crate) fn serialize_data_row(
        &self,
        epoch: u64,
        seq_id: SeqIdType,
        op: Op,
        row: RowRef<'_>,
    ) -> (Bytes, Bytes) {
        let pk = [
            Some(ScalarImpl::Int64(epoch as i64)),
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
        let key_bytes = serialize_pk_with_vnode(
            &pk,
            &self.pk_serde,
            compute_vnode(&extended_row, &self.dist_key_indices, &self.vnodes),
        );
        let value_bytes = self.row_serde.serialize(extended_row).into();
        (key_bytes, value_bytes)
    }

    pub(crate) fn serialize_barrier(
        &self,
        epoch: u64,
        vnode: VirtualNode,
        is_checkpoint: bool,
    ) -> (Bytes, Bytes) {
        let pk = [Some(ScalarImpl::Int64(epoch as i64)), None];

        let op_code = if is_checkpoint {
            CHECKPOINT_BARRIER_OP_CODE
        } else {
            BARRIER_OP_CODE
        };

        let extended_row = pk
            .clone()
            .chain([Some(ScalarImpl::Int16(op_code))])
            .chain(OwnedRow::new(vec![None; self.payload_col_count]));
        let key_bytes = serialize_pk_with_vnode(&pk, &self.pk_serde, vnode);
        let value_bytes = self.row_serde.serialize(extended_row).into();
        (key_bytes, value_bytes)
    }

    pub(crate) fn serialize_epoch(&self, vnode: VirtualNode, epoch: u64) -> Bytes {
        serialize_pk_with_vnode(
            [Some(ScalarImpl::Int64(epoch as i64))],
            &self.epoch_serde,
            vnode,
        )
    }

    pub(crate) fn serialize_truncation_offset_watermark(
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
    fn deserialize(&self, value_bytes: Bytes) -> LogStoreRowOp {
        let row_data = self
            .row_serde
            .deserialize(&value_bytes)
            .expect("should success");

        let payload_row = OwnedRow::new(row_data[PREDEFINED_COLUMNS_TYPES.len()..].to_vec());
        let row_op_code = *row_data[ROW_OP_INDEX].as_ref().unwrap().as_int16();

        match row_op_code {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, ScalarImpl, ScalarRef};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::table::DEFAULT_VNODE;

    use crate::common::log_store::kv_log_store::serde::LogStoreRowSerde;
    use crate::common::log_store::kv_log_store::LogStoreRowOp;
    use crate::common::table::test_utils::gen_prost_table;

    #[tokio::test]
    async fn test_serde() {
        const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

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
        let table = gen_prost_table(
            TEST_TABLE_ID,
            column_descs,
            order_types,
            pk_index,
            read_prefix_len_hint,
        );

        let serde = LogStoreRowSerde::new(&table, None);

        let ops = vec![Op::Insert, Op::Delete, Op::UpdateDelete, Op::UpdateInsert];
        let rows = vec![
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(1)),
                Some(ScalarImpl::Utf8("name1".to_owned_scalar())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(2)),
                Some(ScalarImpl::Utf8("name2".to_owned_scalar())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(3)),
                Some(ScalarImpl::Utf8("name3".to_owned_scalar())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(3)),
                Some(ScalarImpl::Utf8("name4".to_owned_scalar())),
            ]),
        ];
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
            let (key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
            assert!(key < delete_range_right1);
            serialized_keys.push(key);
            let row_op = serde.deserialize(value);
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
        match serde.deserialize(encoded_barrier) {
            LogStoreRowOp::Row { .. } => unreachable!(),
            LogStoreRowOp::Barrier { is_checkpoint } => {
                assert!(!is_checkpoint);
            }
        }
        assert!(key < delete_range_right1);
        serialized_keys.push(key);

        seq_id = 1;
        epoch += 1;

        let delete_range_right2 = serde.serialize_truncation_offset_watermark(DEFAULT_VNODE, epoch);

        for (op, row) in stream_chunk.rows() {
            let (key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
            assert!(key >= delete_range_right1);
            assert!(key < delete_range_right2);
            serialized_keys.push(key);
            let row_op = serde.deserialize(value);
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
        match serde.deserialize(encoded_checkpoint_barrier) {
            LogStoreRowOp::Row { .. } => unreachable!(),
            LogStoreRowOp::Barrier { is_checkpoint } => {
                assert!(is_checkpoint);
            }
        }
        assert!(key >= delete_range_right1);
        assert!(key < delete_range_right2);
        serialized_keys.push(key);

        assert_eq!(serialized_keys.len(), 2 * rows.len() + 2);
        assert!(serialized_keys.is_sorted());
    }
}
