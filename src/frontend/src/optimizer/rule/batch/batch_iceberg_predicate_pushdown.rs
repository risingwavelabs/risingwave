// Copyright 2024 RisingWave Labs
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

use iceberg::expr::Predicate as IcebergPredicate;

use super::prelude::*;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{BatchFilter, BatchIcebergScan, PlanTreeNodeUnary};
use crate::utils::{ExtractIcebergPredicateResult, extract_iceberg_predicate};

/// NOTE(kwannoel): We do predicate pushdown to the iceberg-sdk here.
/// zone-map is used to evaluate predicates on iceberg tables.
/// Without zone-map, iceberg-sdk will still apply the predicate on its own.
/// See: <https://github.com/apache/iceberg-rust/blob/5c1a9e68da346819072a15327080a498ad91c488/crates/iceberg/src/arrow/reader.rs#L229-L235>.
pub struct BatchIcebergPredicatePushDownRule {}

impl Rule<Batch> for BatchIcebergPredicatePushDownRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &BatchFilter = plan.as_batch_filter()?;
        let input = filter.input();
        let scan: &BatchIcebergScan = input.as_batch_iceberg_scan()?;
        // NOTE(kwannoel): We only fill iceberg predicate here.
        assert_eq!(scan.predicate, IcebergPredicate::AlwaysTrue);

        let predicate = filter.predicate().clone();
        let ExtractIcebergPredicateResult {
            iceberg_predicate,
            extracted_condition: _,
            remaining_condition,
        } = extract_iceberg_predicate(predicate, scan.schema().fields());
        let scan = scan.clone_with_predicate(iceberg_predicate);
        if remaining_condition.always_true() {
            Some(scan.into())
        } else {
            let filter = filter
                .clone_with_input(scan.into())
                .clone_with_predicate(remaining_condition);
            Some(filter.into())
        }
    }
}

impl BatchIcebergPredicatePushDownRule {
    pub fn create() -> BoxedRule {
        Box::new(BatchIcebergPredicatePushDownRule {})
    }
}
