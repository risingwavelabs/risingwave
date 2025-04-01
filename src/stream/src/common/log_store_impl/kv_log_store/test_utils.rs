// Copyright 2025 RisingWave Labs
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

use itertools::{Itertools, zip_eq};
use rand::RngCore;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ScalarImpl, ScalarRef};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::log_store::{FlushCurrentEpochOptions, LogStoreResult, LogWriter};
use risingwave_pb::catalog::PbTable;

use crate::common::log_store_impl::kv_log_store::KvLogStorePkInfo;
use crate::common::table::test_utils::gen_pbtable_with_dist_key;

pub(crate) const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
pub(crate) const TEST_DATA_SIZE: usize = 10;

pub(crate) fn gen_test_data(base: i64) -> (Vec<Op>, Vec<OwnedRow>) {
    gen_sized_test_data(base, TEST_DATA_SIZE)
}

pub(crate) fn gen_sized_test_data(base: i64, max_count: usize) -> (Vec<Op>, Vec<OwnedRow>) {
    let mut ops = Vec::new();
    let mut rows = Vec::new();
    while ops.len() < max_count - 1 {
        let index = ops.len() as i64;
        match rand::rng().next_u32() % 3 {
            0 => {
                ops.push(Op::Insert);
                rows.push(OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(index + base)),
                    Some(ScalarImpl::Utf8(
                        format!("name{}", index).as_str().to_owned_scalar(),
                    )),
                ]));
            }
            1 => {
                ops.push(Op::Delete);
                rows.push(OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(index + base)),
                    Some(ScalarImpl::Utf8(
                        format!("name{}", index).as_str().to_owned_scalar(),
                    )),
                ]));
            }
            2 => {
                ops.push(Op::UpdateDelete);
                rows.push(OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(index + base)),
                    Some(ScalarImpl::Utf8(
                        format!("name{}", index).as_str().to_owned_scalar(),
                    )),
                ]));
                ops.push(Op::UpdateInsert);
                rows.push(OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(index + base)),
                    Some(ScalarImpl::Utf8(
                        format!("name{}", index + 1).as_str().to_owned_scalar(),
                    )),
                ]));
            }
            _ => unreachable!(),
        }
    }
    (ops, rows)
}

pub(crate) fn test_payload_schema(pk_info: &'static KvLogStorePkInfo) -> Vec<ColumnDesc> {
    vec![
        ColumnDesc::unnamed(
            ColumnId::from(pk_info.predefined_column_len() as i32),
            DataType::Int64,
        ), // id
        ColumnDesc::unnamed(
            ColumnId::from((pk_info.predefined_column_len() + 1) as i32),
            DataType::Varchar,
        ), // name
    ]
}

pub(crate) fn test_log_store_table_schema(pk_info: &'static KvLogStorePkInfo) -> Vec<ColumnDesc> {
    let mut column_descs = pk_info
        .predefined_columns
        .iter()
        .enumerate()
        .map(|(i, (_, data_type))| ColumnDesc::unnamed(ColumnId::from(i as i32), data_type.clone()))
        .collect_vec();
    column_descs.extend(test_payload_schema(pk_info));
    column_descs
}

pub(crate) fn gen_stream_chunk_with_info(
    base: i64,
    pk_info: &'static KvLogStorePkInfo,
) -> StreamChunk {
    let (ops, rows) = gen_test_data(base);
    let mut builder = DataChunkBuilder::new(
        test_payload_schema(pk_info)
            .iter()
            .map(|col| col.data_type.clone())
            .collect_vec(),
        1000000,
    );
    for row in &rows {
        assert!(builder.append_one_row(row).is_none());
    }
    let data_chunk = builder.consume_all().unwrap();
    StreamChunk::from_parts(ops, data_chunk)
}

pub(crate) fn gen_multi_vnode_stream_chunks<const MOD_COUNT: usize>(
    base: i64,
    max_count: usize,
    pk_info: &'static KvLogStorePkInfo,
) -> [StreamChunk; MOD_COUNT] {
    let mut data_builder = (0..MOD_COUNT)
        .map(|_| {
            (
                Vec::new() as Vec<Op>,
                DataChunkBuilder::new(
                    test_payload_schema(pk_info)
                        .iter()
                        .map(|col| col.data_type.clone())
                        .collect_vec(),
                    max_count,
                ),
            )
        })
        .collect_vec();
    let (ops, rows) = gen_sized_test_data(base, max_count);
    for (op, row) in zip_eq(ops, rows) {
        let vnode = VirtualNode::compute_row_for_test(&row, &[TEST_SCHEMA_DIST_KEY_INDEX]);
        let (ops, builder) = &mut data_builder[vnode.to_index() % MOD_COUNT];
        ops.push(op);
        assert!(builder.append_one_row(row).is_none());
    }

    data_builder
        .into_iter()
        .map(|(ops, mut builder)| StreamChunk::from_parts(ops, builder.consume_all().unwrap()))
        .collect_vec()
        .try_into()
        .unwrap()
}

pub(crate) const TEST_SCHEMA_DIST_KEY_INDEX: usize = 0;

pub(crate) fn gen_test_log_store_table(pk_info: &'static KvLogStorePkInfo) -> PbTable {
    let schema = test_log_store_table_schema(pk_info);
    let order_types = pk_info.pk_orderings.to_vec();
    let pk_index = (0..pk_info.pk_len()).collect();
    let read_prefix_len_hint = 0;
    gen_pbtable_with_dist_key(
        TEST_TABLE_ID,
        schema,
        order_types,
        pk_index,
        read_prefix_len_hint,
        vec![pk_info.predefined_column_len()],
    )
}

pub(crate) fn calculate_vnode_bitmap<'a>(
    test_data: impl Iterator<Item = (Op, RowRef<'a>)>,
) -> Bitmap {
    let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
    for vnode in test_data
        .map(|(_, row)| VirtualNode::compute_row_for_test(row, &[TEST_SCHEMA_DIST_KEY_INDEX]))
    {
        builder.set(vnode.to_index(), true);
    }
    builder.finish()
}

pub(crate) fn check_rows_eq<R1: Row, R2: Row>(
    first: impl Iterator<Item = (Op, R1)>,
    second: impl Iterator<Item = (Op, R2)>,
) -> bool {
    for ((op1, row1), (op2, row2)) in zip_eq(
        first.sorted_by_key(|(_, row)| {
            row.datum_at(TEST_SCHEMA_DIST_KEY_INDEX)
                .unwrap()
                .into_int64()
        }),
        second.sorted_by_key(|(_, row)| {
            row.datum_at(TEST_SCHEMA_DIST_KEY_INDEX)
                .unwrap()
                .into_int64()
        }),
    ) {
        if op1 != op2 {
            return false;
        }
        if row1.to_owned_row() != row2.to_owned_row() {
            return false;
        }
    }
    true
}

pub(crate) fn check_stream_chunk_eq(first: &StreamChunk, second: &StreamChunk) -> bool {
    check_rows_eq(first.rows(), second.rows())
}

pub(crate) trait LogWriterTestExt: LogWriter {
    async fn flush_current_epoch_for_test(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> LogStoreResult<()> {
        let post_flush = self
            .flush_current_epoch(
                next_epoch,
                FlushCurrentEpochOptions {
                    is_checkpoint,
                    new_vnode_bitmap: None,
                    is_stop: false,
                },
            )
            .await?;
        (post_flush).post_yield_barrier().await?;
        Ok(())
    }
}

impl<W: LogWriter> LogWriterTestExt for W {}
