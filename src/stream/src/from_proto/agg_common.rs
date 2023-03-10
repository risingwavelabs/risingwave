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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::expr::{build_from_prost, AggKind};

use super::*;
use crate::common::table::state_table::StateTable;
use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::{AggArgs, AggCall, AggStateStorage};

pub fn build_agg_call_from_prost(
    append_only: bool,
    agg_call_proto: &risingwave_pb::expr::AggCall,
) -> StreamResult<AggCall> {
    let agg_kind = AggKind::try_from(agg_call_proto.get_type()?)?;
    let args = match &agg_call_proto.get_args()[..] {
        [] => AggArgs::None,
        [arg] if agg_kind != AggKind::StringAgg => {
            AggArgs::Unary(DataType::from(arg.get_type()?), arg.get_index() as usize)
        }
        [agg_arg, extra_arg] if agg_kind == AggKind::StringAgg => AggArgs::Binary(
            [
                DataType::from(agg_arg.get_type()?),
                DataType::from(extra_arg.get_type()?),
            ],
            [agg_arg.get_index() as usize, extra_arg.get_index() as usize],
        ),
        _ => bail!("Too many/few arguments for {:?}", agg_kind),
    };
    let column_orders = agg_call_proto
        .get_order_by()
        .iter()
        .map(|col_order| {
            let col_idx = col_order.get_column_index() as usize;
            let order_type = OrderType::from_protobuf(col_order.get_order_type().unwrap());
            // TODO(yuchao): `nulls first/last` is not supported yet, so it's ignore here,
            // see also `risingwave_common::util::sort_util::compare_values`
            ColumnOrder::new(col_idx, order_type)
        })
        .collect();
    let filter = match agg_call_proto.filter {
        Some(ref prost_filter) => Some(Arc::from(build_from_prost(prost_filter)?)),
        None => None,
    };
    Ok(AggCall {
        kind: agg_kind,
        args,
        return_type: DataType::from(agg_call_proto.get_return_type()?),
        column_orders,
        append_only,
        filter,
        distinct: agg_call_proto.distinct,
    })
}

/// Parse from stream proto plan agg call states, generate state tables and column mappings.
/// The `vnodes` is generally `Some` for Hash Agg and `None` for Simple Agg.
pub async fn build_agg_state_storages_from_proto<S: StateStore>(
    agg_call_states: &[risingwave_pb::stream_plan::AggCallState],
    store: S,
    vnodes: Option<Arc<Bitmap>>,
) -> Vec<AggStateStorage<S>> {
    use risingwave_pb::stream_plan::agg_call_state;

    let mut result = vec![];
    for agg_call_state in agg_call_states {
        let agg_state_store = match agg_call_state.get_inner().unwrap() {
            agg_call_state::Inner::ResultValueState(..) => AggStateStorage::ResultValue,
            agg_call_state::Inner::TableState(state) => {
                let table = StateTable::from_table_catalog(
                    state.get_table().unwrap(),
                    store.clone(),
                    vnodes.clone(),
                )
                .await;
                AggStateStorage::Table { table }
            }
            agg_call_state::Inner::MaterializedInputState(state) => {
                let table = StateTable::from_table_catalog(
                    state.get_table().unwrap(),
                    store.clone(),
                    vnodes.clone(),
                )
                .await;
                let mapping = StateTableColumnMapping::new(
                    state
                        .get_included_upstream_indices()
                        .iter()
                        .map(|idx| *idx as usize)
                        .collect(),
                    Some(
                        state
                            .get_table_value_indices()
                            .iter()
                            .map(|idx| *idx as usize)
                            .collect(),
                    ),
                );
                AggStateStorage::MaterializedInput { table, mapping }
            }
        };

        result.push(agg_state_store)
    }

    result
}

pub async fn build_distinct_dedup_table_from_proto<S: StateStore>(
    dedup_tables: &HashMap<u32, risingwave_pb::catalog::Table>,
    store: S,
    vnodes: Option<Arc<Bitmap>>,
) -> HashMap<usize, StateTable<S>> {
    if dedup_tables.is_empty() {
        return HashMap::new();
    }
    futures::future::join_all(dedup_tables.iter().map(|(distinct_col, table_pb)| async {
        let table = StateTable::from_table_catalog(table_pb, store.clone(), vnodes.clone()).await;
        (*distinct_col as usize, table)
    }))
    .await
    .into_iter()
    .collect()
}
