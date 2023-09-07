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

use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl, ScalarRef};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::PbTable;

use crate::common::table::test_utils::gen_prost_table;

pub(crate) const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

pub(crate) fn gen_test_data(base: i64) -> (Vec<Op>, Vec<OwnedRow>) {
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

pub(crate) fn test_payload_schema() -> Vec<ColumnDesc> {
    vec![
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Int64), // id
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Varchar), // name
    ]
}

pub(crate) fn test_log_store_table_schema() -> Vec<ColumnDesc> {
    let mut column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int64), // epoch
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32), // Seq id
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int16), // op code
    ];
    column_descs.extend(test_payload_schema().into_iter());
    column_descs
}

pub(crate) fn gen_stream_chunk(base: i64) -> StreamChunk {
    let (ops, rows) = gen_test_data(base);
    let mut builder = DataChunkBuilder::new(
        test_payload_schema()
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

pub(crate) fn gen_test_log_store_table() -> PbTable {
    let schema = test_log_store_table_schema();
    let order_types = vec![OrderType::ascending(), OrderType::ascending_nulls_last()];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    gen_prost_table(
        TEST_TABLE_ID,
        schema,
        order_types,
        pk_index,
        read_prefix_len_hint,
    )
}
