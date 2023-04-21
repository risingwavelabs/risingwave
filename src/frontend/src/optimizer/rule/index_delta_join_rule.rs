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
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::ChainType;

use super::super::plan_node::*;
use super::{BoxedRule, Rule};

/// Use index scan and delta joins for supported queries.
pub struct IndexDeltaJoinRule {}

impl Rule for IndexDeltaJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join = plan.as_stream_hash_join()?;
        if join.eq_join_predicate().has_non_eq() || join.join_type() != JoinType::Inner {
            return Some(plan);
        }

        /// FIXME: Exchanges still may exist after table scan, because table scan's distribution
        /// follows upstream materialize node(e.g. subset of pk), whereas join distributes by join
        /// key.
        fn match_through_exchange(plan: PlanRef) -> Option<PlanRef> {
            if let Some(exchange) = plan.as_stream_exchange() {
                match_through_exchange(exchange.input())
            } else if plan.as_stream_table_scan().is_some() {
                Some(plan)
            } else {
                None
            }
        }

        let input_left_dyn = match_through_exchange(join.inputs()[0].clone())?;
        let input_left = input_left_dyn.as_stream_table_scan()?;
        let input_right_dyn = match_through_exchange(join.inputs()[1].clone())?;
        let input_right = input_right_dyn.as_stream_table_scan()?;
        let left_indices = join.eq_join_predicate().left_eq_indexes();
        let right_indices = join.eq_join_predicate().right_eq_indexes();

        fn match_indexes(
            join_indices: &[usize],
            table_scan: &StreamTableScan,
            chain_type: ChainType,
        ) -> Option<PlanRef> {
            for index in table_scan.logical().indexes {
                // Only full covering index can be used in delta join
                if !index.full_covering() {
                    continue;
                }

                let p2s_mapping = index.primary_to_secondary_mapping();

                // 1. Check if distribution keys are the same.
                // We don't assume the hash function we are using satisfies commutativity
                // `Hash(A, B) == Hash(B, A)`, so we consider order of each item in distribution
                // keys here.
                let join_indices_ref_to_index_table = join_indices
                    .iter()
                    .map(|&i| table_scan.logical().output_col_idx[i])
                    .map(|x| *p2s_mapping.get(&x).unwrap())
                    .collect_vec();

                if index.index_table.distribution_key != join_indices_ref_to_index_table {
                    continue;
                }

                // 2. Check join key is prefix of index order key
                let index_order_key_prefix = index
                    .index_table
                    .pk
                    .iter()
                    .map(|x| x.column_index)
                    .take(index.index_table.distribution_key.len())
                    .collect_vec();

                if index_order_key_prefix != join_indices_ref_to_index_table {
                    continue;
                }

                return Some(
                    table_scan
                        .to_index_scan(
                            index.index_table.name.as_str(),
                            index.index_table.table_desc().into(),
                            p2s_mapping,
                            index.function_mapping(),
                            chain_type,
                        )
                        .into(),
                );
            }

            // Primary table is also an index.
            let primary_table = table_scan.logical();
            if let Some(primary_table_distribution_key) = primary_table.distribution_key()
                && primary_table_distribution_key == join_indices {
                // Check join key is prefix of primary table order key
                let primary_table_order_key_prefix = primary_table.table_desc.pk.iter()
                    .map(|x| x.column_index)
                    .take(primary_table_distribution_key.len())
                    .collect_vec();

                if primary_table_order_key_prefix != join_indices {
                    return None;
                }

                if chain_type != table_scan.chain_type() {
                    Some(
                        StreamTableScan::new_with_chain_type(table_scan.logical().clone(), chain_type).into()
                    )
                } else {
                    Some(table_scan.clone().into())
                }
            } else {
                None
            }
        }

        // Delta join only needs to backfill one stream flow and others should be upstream only
        // chain. Here we choose the left one to backfill and right one to upstream only
        // chain.
        if let Some(left) = match_indexes(&left_indices, input_left, ChainType::Backfill) {
            if let Some(right) = match_indexes(&right_indices, input_right, ChainType::UpstreamOnly)
            {
                // We already ensured that index and join use the same distribution, so we directly
                // replace the children with stream index scan without inserting any exchanges.
                Some(
                    join.clone()
                        .into_delta_join()
                        .clone_with_left_right(left, right)
                        .into(),
                )
            } else {
                Some(plan)
            }
        } else {
            Some(plan)
        }
    }
}

impl IndexDeltaJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}
