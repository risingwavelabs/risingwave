// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::rc::Rc;

use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::optimizer::property::{Distribution, Order, RequiredDist};

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

        let input_left_dyn = match_through_exchange(Rc::clone(&join.inputs()[0]))?;
        let input_left = input_left_dyn.as_stream_table_scan()?;
        let input_right_dyn = match_through_exchange(Rc::clone(&join.inputs()[1]))?;
        let input_right = input_right_dyn.as_stream_table_scan()?;
        let left_indices = join.eq_join_predicate().left_eq_indexes();
        let right_indices = join.eq_join_predicate().right_eq_indexes();

        fn match_indexes(join_indices: &[usize], table_scan: &StreamTableScan) -> Option<PlanRef> {
            if table_scan.logical().indexes().is_empty() {
                return None;
            }

            for index in table_scan.logical().indexes() {
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
                    .map(|&i| *table_scan.logical().output_col_idx().get(i).unwrap())
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
                    .map(|x| x.index)
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
                        )
                        .into(),
                );
            }

            None
        }

        if let Some(left) = match_indexes(&left_indices, input_left) {
            if let Some(right) = match_indexes(&right_indices, input_right) {
                // We already ensured that index and join use the same distribution, so we directly
                // replace the children with stream index scan without inserting any exchanges.

                fn upstream_hash_shard_to_hash_shard(plan: PlanRef) -> PlanRef {
                    if let Distribution::UpstreamHashShard(key) = plan.distribution() {
                        RequiredDist::hash_shard(key)
                            .enforce_if_not_satisfies(plan, &Order::any())
                            .unwrap()
                    } else {
                        plan
                    }
                }
                let left = upstream_hash_shard_to_hash_shard(left);
                let right = upstream_hash_shard_to_hash_shard(right);

                Some(
                    join.to_delta_join()
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
