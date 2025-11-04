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

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::common::PbColumnOrder;
use risingwave_pb::plan_common::ColumnCatalog;

pub fn gen_pbtable(
    table_id: TableId,
    column_descs: Vec<ColumnDesc>,
    order_types: Vec<OrderType>,
    pk_indices: Vec<usize>,
    read_prefix_len_hint: usize,
) -> PbTable {
    let value_indices = (0..column_descs.len()).collect_vec();
    gen_pbtable_with_value_indices(
        table_id,
        column_descs,
        order_types,
        pk_indices,
        read_prefix_len_hint,
        value_indices,
    )
}

pub fn gen_pbtable_with_dist_key(
    table_id: TableId,
    column_descs: Vec<ColumnDesc>,
    order_types: Vec<OrderType>,
    pk_indices: Vec<usize>,
    read_prefix_len_hint: usize,
    distribution_key: Vec<usize>,
) -> PbTable {
    let value_indices = (0..column_descs.len()).collect_vec();
    gen_pbtable_inner(
        table_id,
        column_descs,
        order_types,
        pk_indices,
        read_prefix_len_hint,
        value_indices,
        distribution_key,
    )
}

pub fn gen_pbtable_with_value_indices(
    table_id: TableId,
    column_descs: Vec<ColumnDesc>,
    order_types: Vec<OrderType>,
    pk_indices: Vec<usize>,
    read_prefix_len_hint: usize,
    value_indices: Vec<usize>,
) -> PbTable {
    gen_pbtable_inner(
        table_id,
        column_descs,
        order_types,
        pk_indices,
        read_prefix_len_hint,
        value_indices,
        Vec::default(),
    )
}

pub fn gen_pbtable_inner(
    table_id: TableId,
    column_descs: Vec<ColumnDesc>,
    order_types: Vec<OrderType>,
    pk_indices: Vec<usize>,
    read_prefix_len_hint: usize,
    value_indices: Vec<usize>,
    distribution_key: Vec<usize>,
) -> PbTable {
    let prost_pk = pk_indices
        .iter()
        .zip_eq_fast(order_types.iter())
        .map(|(idx, order)| PbColumnOrder {
            column_index: *idx as _,
            order_type: Some(order.to_protobuf()),
        })
        .collect();
    let prost_columns = column_descs
        .iter()
        .map(|col| ColumnCatalog {
            column_desc: Some(col.to_protobuf()),
            is_hidden: false,
        })
        .collect();

    let value_indices = value_indices.into_iter().map(|i| i as i32).collect_vec();
    let distribution_key = distribution_key.into_iter().map(|i| i as i32).collect_vec();

    PbTable {
        id: table_id.as_raw_id(),
        columns: prost_columns,
        pk: prost_pk,
        read_prefix_len_hint: read_prefix_len_hint as u32,
        value_indices,
        distribution_key,
        ..Default::default()
    }
}
