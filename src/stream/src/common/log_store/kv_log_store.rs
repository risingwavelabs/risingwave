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

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, TableId, TableOption};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::{
    BasicSerde, ValueRowDeserializer, ValueRowSerdeNew, ValueRowSerializer,
};
use risingwave_pb::catalog::Table;
use risingwave_storage::row_serde::row_serde_util::serialize_pk_with_vnode;
use risingwave_storage::store::{LocalStateStore, NewLocalOptions};
use risingwave_storage::table::{compute_vnode, Distribution};
use risingwave_storage::StateStore;

use crate::common::log_store::{
    LogReader, LogStoreFactory, LogStoreReadItem, LogStoreResult, LogWriter,
};

type SeqIdType = i32;
type RowOpCodeType = i16;

const INSERT_OP_CODE: RowOpCodeType = 1;
const DELETE_OP_CODE: RowOpCodeType = 2;
const UPDATE_INSERT_OP_CODE: RowOpCodeType = 3;
const UPDATE_DELETE_OP_CODE: RowOpCodeType = 4;
const BARRIER_OP_CODE: RowOpCodeType = 5;
const CHECKPOINT_BARRIER_OP_CODE: RowOpCodeType = 6;

#[derive(Clone)]
struct LogStoreRowSerde {
    /// Used for serializing and deserializing the primary key.
    pk_serde: OrderedRowSerde,

    /// Row deserializer with value encoding
    row_serde: BasicSerde,

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
    fn new(table_catalog: &Table, vnodes: Option<Arc<Bitmap>>) -> Self {
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
        assert!(data_types.len() > 3);
        assert_eq!(data_types[0], DataType::Int64); // epoch
        assert_eq!(data_types[1], DataType::Int32); // seq_id
        assert_eq!(data_types[2], DataType::Int16); // op code
        let payload_col_count = data_types.len() - 3;

        let row_serde = BasicSerde::new(&[], Arc::from(data_types.into_boxed_slice()));

        let vnodes = match vnodes {
            Some(vnodes) => vnodes,

            None => Distribution::fallback_vnodes(),
        };

        // epoch and seq_id. The seq_id of barrier is set null, and therefore the second order type
        // is nulls last
        let pk_serde = OrderedRowSerde::new(
            vec![DataType::Int64, DataType::Int32],
            vec![OrderType::ascending(), OrderType::ascending_nulls_last()],
        );

        Self {
            pk_serde,
            row_serde,
            dist_key_indices,
            vnodes,
            payload_col_count,
        }
    }

    fn serialize_data_row(
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

    fn serialize_barrier(
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
}

enum LogStoreRowOp {
    Row { op: Op, row: OwnedRow },
    Barrier { is_checkpoint: bool },
}

impl LogStoreRowSerde {
    fn deserialize(&self, value_bytes: Bytes) -> LogStoreRowOp {
        let row_data = self
            .row_serde
            .deserialize(&value_bytes)
            .expect("should success");

        let payload_row = OwnedRow::new(row_data[3..].to_vec());
        let row_op_code = *row_data[2].as_ref().unwrap().as_int16();

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
                assert!(row_data[1].is_none());
                LogStoreRowOp::Barrier {
                    is_checkpoint: false,
                }
            }
            CHECKPOINT_BARRIER_OP_CODE => {
                assert!(row_data[1].is_none());
                LogStoreRowOp::Barrier {
                    is_checkpoint: true,
                }
            }
            _ => unreachable!("invalid row op code: {}", row_op_code),
        }
    }
}

pub struct KvLogStoreReader<S: StateStore> {
    table_id: TableId,

    state_store: S,

    serde: LogStoreRowSerde,
}

impl<S: StateStore> LogReader for KvLogStoreReader<S> {
    type InitFuture<'a> = impl Future<Output = LogStoreResult<u64>>;
    type NextItemFuture<'a> = impl Future<Output = LogStoreResult<LogStoreReadItem>>;
    type TruncateFuture<'a> = impl Future<Output = LogStoreResult<()>>;

    fn init(&mut self) -> Self::InitFuture<'_> {
        async move { todo!() }
    }

    fn next_item(&mut self) -> Self::NextItemFuture<'_> {
        async move { todo!() }
    }

    fn truncate(&mut self) -> Self::TruncateFuture<'_> {
        async move { todo!() }
    }
}

pub struct KvLogStoreWriter<LS: LocalStateStore> {
    table_id: TableId,

    state_store: LS,

    serde: LogStoreRowSerde,
}

impl<LS: LocalStateStore> LogWriter for KvLogStoreWriter<LS> {
    type FlushCurrentEpoch<'a> = impl Future<Output = LogStoreResult<()>>;
    type InitFuture<'a> = impl Future<Output = LogStoreResult<()>>;
    type WriteChunkFuture<'a> = impl Future<Output = LogStoreResult<()>>;

    fn init(&mut self, epoch: u64) -> Self::InitFuture<'_> {
        async move { todo!() }
    }

    fn write_chunk(&mut self, chunk: StreamChunk) -> Self::WriteChunkFuture<'_> {
        async move { todo!() }
    }

    fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> Self::FlushCurrentEpoch<'_> {
        async move { todo!() }
    }

    fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) {
        todo!()
    }
}

pub struct KvLogStoreFactory<S: StateStore> {
    state_store: S,

    table_catalog: Table,

    vnodes: Option<Arc<Bitmap>>,
}

impl<S: StateStore> KvLogStoreFactory<S> {
    pub fn new(state_store: S, table_catalog: Table, vnodes: Option<Arc<Bitmap>>) -> Self {
        Self {
            state_store,
            table_catalog,
            vnodes,
        }
    }
}

impl<S: StateStore> LogStoreFactory for KvLogStoreFactory<S> {
    type Reader = KvLogStoreReader<S>;
    type Writer = KvLogStoreWriter<S::Local>;

    type BuildFuture = impl Future<Output = (Self::Reader, Self::Writer)>;

    fn build(self) -> Self::BuildFuture {
        async move {
            let table_id = TableId::new(self.table_catalog.id);
            let serde = LogStoreRowSerde::new(&self.table_catalog, self.vnodes);
            let local_state_store = self
                .state_store
                .new_local(NewLocalOptions {
                    table_id: TableId {
                        table_id: self.table_catalog.id,
                    },
                    is_consistent_op: false,
                    table_option: TableOption {
                        retention_seconds: None,
                    },
                })
                .await;

            let reader = KvLogStoreReader {
                table_id,
                state_store: self.state_store,
                serde: serde.clone(),
            };

            let writer = KvLogStoreWriter {
                table_id,
                state_store: local_state_store,
                serde,
            };

            (reader, writer)
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl, ScalarRef};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_common::util::sort_util::OrderType;

    use crate::common::log_store::kv_log_store::{LogStoreRowOp, LogStoreRowSerde};
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

        let ops = vec![Op::Insert, Op::Delete, Op::UpdateInsert, Op::UpdateDelete];
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
                Some(ScalarImpl::Int64(4)),
                Some(ScalarImpl::Utf8("name4".to_owned_scalar())),
            ]),
        ];
        let mut builder = DataChunkBuilder::new(vec![DataType::Int64, DataType::Varchar], 1000000);
        for row in &rows {
            assert!(builder.append_one_row(row).is_none());
        }
        let data_chunk = builder.consume_all().unwrap();
        let stream_chunk = StreamChunk::from_parts(ops.clone(), data_chunk);

        const TEST_EPOCH: u64 = 233u64;

        let mut serialized_bytes = vec![];
        let mut seq_id = 1;
        for (op, row) in stream_chunk.rows() {
            serialized_bytes.push(serde.serialize_data_row(TEST_EPOCH, seq_id, op, row));
            seq_id += 1;
        }

        for (i, _) in stream_chunk.rows().enumerate() {
            let row_op = serde.deserialize(serialized_bytes[i].1.clone());
            match row_op {
                LogStoreRowOp::Row { op, row } => {
                    assert_eq!(&op, &ops[i]);
                    assert_eq!(&row, &rows[i]);
                }
                LogStoreRowOp::Barrier { .. } => unreachable!(),
            }
        }

        let (_, encoded_barrier) =
            serde.serialize_barrier(TEST_EPOCH, VirtualNode::from_index(1), false);
        match serde.deserialize(encoded_barrier) {
            LogStoreRowOp::Row { .. } => unreachable!(),
            LogStoreRowOp::Barrier { is_checkpoint } => {
                assert!(!is_checkpoint);
            }
        }

        let (_, encoded_checkpoint_barrier) =
            serde.serialize_barrier(TEST_EPOCH, VirtualNode::from_index(1), true);
        match serde.deserialize(encoded_checkpoint_barrier) {
            LogStoreRowOp::Row { .. } => unreachable!(),
            LogStoreRowOp::Barrier { is_checkpoint } => {
                assert!(is_checkpoint);
            }
        }
    }
}
