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
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::common::{PbColumnOrder, PbOrderType};
use risingwave_pb::plan_common::ColumnCatalog;

pub(crate) fn gen_prost_table(
    table_id: TableId,
    column_descs: Vec<ColumnDesc>,
    order_types: Vec<OrderType>,
    pk_index: Vec<usize>,
    read_prefix_len_hint: u32,
) -> ProstTable {
    let col_len = column_descs.len() as i32;
    gen_prost_table_with_value_indices(
        table_id,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
        (0..col_len).collect_vec(),
    )
}

pub(crate) fn gen_prost_table_with_value_indices(
    table_id: TableId,
    column_descs: Vec<ColumnDesc>,
    order_types: Vec<OrderType>,
    pk_index: Vec<usize>,
    read_prefix_len_hint: u32,
    value_indices: Vec<i32>,
) -> ProstTable {
    let prost_pk = pk_index
        .iter()
        .zip_eq_fast(order_types.iter())
        .map(|(idx, order)| PbColumnOrder {
            column_index: *idx as _,
            order_type: Some(PbOrderType {
                direction: order.to_protobuf() as _,
            }),
        })
        .collect();
    let prost_columns = column_descs
        .iter()
        .map(|col| ColumnCatalog {
            column_desc: Some(col.to_protobuf()),
            is_hidden: false,
        })
        .collect();

    ProstTable {
        id: table_id.table_id(),
        columns: prost_columns,
        pk: prost_pk,
        read_prefix_len_hint,
        value_indices,
        ..Default::default()
    }
}
