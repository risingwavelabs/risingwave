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
use std::sync::Arc;

use risingwave_common::buffer::Bitmap;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::AggStateStorage;

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
