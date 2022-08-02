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

use std::collections::HashMap;
use std::rc::Rc;

use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;

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
                // 1. Check if distribution keys are the same.
                // We don't assume the hash function we are using satisfies commutativity
                // `Hash(A, B) == Hash(B, A)`, so we consider order of each item in distribution
                // keys here.
                let join_indices_ref_to_table_desc = join_indices
                    .iter()
                    .map(|&i| *table_scan.logical().output_col_idx().get(i).unwrap())
                    .collect_vec();

                let index_column = index.index_columns.iter().map(|x| x.index).collect_vec();
                if index_column != join_indices_ref_to_table_desc {
                    continue;
                }

                let (index_table_name, index_table_desc) = {
                    let reader = table_scan
                        .base
                        .ctx
                        .inner()
                        .session_ctx
                        .env()
                        .catalog_reader()
                        .read_guard();
                    let index_table_result = reader.get_table_by_index_catalog(index);
                    if index_table_result.is_err() {
                        // maybe concurrent ddl happen.
                        continue;
                    }
                    let index_table = index_table_result.unwrap();
                    (index_table.name.clone(), index_table.table_desc())
                };

                let primary_table_desc = table_scan.logical().table_desc();
                // check index is a fully covering index
                let mut primary_to_secondary: HashMap<usize, usize> = HashMap::new();
                index
                    .index_columns
                    .iter()
                    .chain(index.include_columns.iter())
                    .enumerate()
                    .for_each(|(i, input_ref)| {
                        primary_to_secondary.insert(input_ref.index, i);
                    });
                primary_table_desc
                    .pk
                    .iter()
                    .zip_eq(index_table_desc.pk.iter())
                    .for_each(|(&i, &j)| {
                        primary_to_secondary.insert(i, j);
                    });

                if primary_to_secondary.len() == primary_table_desc.columns.len() {
                    let len = primary_table_desc.columns.len();
                    let mut index_mapping = Vec::with_capacity(len);
                    for i in 0..len {
                        index_mapping.push(*primary_to_secondary.get(&i).unwrap());
                    }
                    return Some(
                        table_scan
                            .to_index_scan(
                                index_table_name.as_str(),
                                index_table_desc.into(),
                                index_mapping,
                            )
                            .into(),
                    );
                }
            }

            None
        }

        if let Some(left) = match_indexes(&left_indices, input_left) {
            if let Some(right) = match_indexes(&right_indices, input_right) {
                // We already ensured that index and join use the same distribution, so we directly
                // replace the children with stream index scan without inserting any exchanges.

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
