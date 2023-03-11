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

use std::collections::HashSet;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, Schema, USER_COLUMN_ID_OFFSET};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use super::PlanRef;
use crate::optimizer::property::Order;

pub(crate) fn derive_columns(
    input_schema: &Schema,
    out_names: Vec<String>,
    user_cols: &FixedBitSet,
) -> Result<Vec<ColumnCatalog>> {
    // Validate and deduplicate column names.
    let mut col_names = HashSet::new();
    for name in &out_names {
        if !col_names.insert(name.clone()) {
            Err(ErrorCode::InvalidInputSyntax(format!(
                "column \"{}\" specified more than once",
                name
            )))?;
        }
    }

    let mut out_name_iter = out_names.into_iter();
    let columns = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let mut c = ColumnCatalog {
                column_desc: ColumnDesc::from_field_with_column_id(
                    field,
                    i as i32 + USER_COLUMN_ID_OFFSET,
                ),
                is_hidden: !user_cols.contains(i),
            };
            c.column_desc.name = if !c.is_hidden {
                out_name_iter.next().unwrap()
            } else {
                let mut name = field.name.clone();
                let mut count = 0;

                while !col_names.insert(name.clone()) {
                    count += 1;
                    name = format!("{}#{}", field.name, count);
                }

                name
            };
            c
        })
        .collect_vec();

    // We should use up all of the `out_name`s
    assert_eq!(out_name_iter.next(), None);

    Ok(columns)
}

/// Derive the pk and the stream key for tables and sinks.
pub(crate) fn derive_pk(
    input: PlanRef,
    user_order_by: Order,
    columns: &[ColumnCatalog],
) -> (Vec<ColumnOrder>, Vec<usize>) {
    // Note(congyi): avoid pk duplication
    let stream_key = input.logical_pk().iter().copied().unique().collect_vec();
    let schema = input.schema();

    // Assert the uniqueness of column names and IDs, including hidden columns.
    if let Some(name) = columns.iter().map(|c| c.name()).duplicates().next() {
        panic!("duplicated column name \"{name}\"");
    }
    if let Some(id) = columns.iter().map(|c| c.column_id()).duplicates().next() {
        panic!("duplicated column ID {id}");
    }
    // Assert that the schema of given `columns` is correct.
    assert_eq!(
        columns.iter().map(|c| c.data_type().clone()).collect_vec(),
        input.schema().data_types()
    );

    let mut in_order = FixedBitSet::with_capacity(schema.len());
    let mut pk = vec![];

    for order in &user_order_by.column_orders {
        let idx = order.column_index;
        pk.push(order.clone());
        in_order.insert(idx);
    }

    for &idx in &stream_key {
        if in_order.contains(idx) {
            continue;
        }
        pk.push(ColumnOrder::new(idx, OrderType::default_ascending()));
        in_order.insert(idx);
    }

    (pk, stream_key)
}
