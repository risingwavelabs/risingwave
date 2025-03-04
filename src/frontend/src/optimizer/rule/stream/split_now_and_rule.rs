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

use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalFilter, PlanTreeNodeUnary};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::utils::Condition;

/// Split `LogicalFilter` with many AND conjunctions with now into multiple `LogicalFilter`, prepared for `SplitNowOrRule`
///
/// Before:
/// ```text
/// `LogicalFilter`
///  (now() or c11 or c12 ..) and (now() or c21 or c22 ...) and .. and other exprs
///        |
///      Input
/// ```
///
/// After:
/// ```text
/// `LogicalFilter`(now() or c11 or c12 ..)
///        |
/// `LogicalFilter`(now() or c21 or c22 ...)
///        |
///      ......
///        |
/// `LogicalFilter` other exprs
///        |
///      Input
/// ```
pub struct SplitNowAndRule {}
impl Rule for SplitNowAndRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        let input = filter.input();
        if filter.predicate().conjunctions.len() == 1 {
            return None;
        }

        if filter
            .predicate()
            .conjunctions
            .iter()
            .all(|e| e.count_nows() == 0)
        {
            return None;
        }

        let [with_now, others] = filter
            .predicate()
            .clone()
            .group_by::<_, 2>(|e| if e.count_nows() > 0 { 0 } else { 1 });

        let mut plan = LogicalFilter::create(input, others);
        for e in with_now {
            plan = LogicalFilter::new(
                plan,
                Condition {
                    conjunctions: vec![e],
                },
            )
            .into();
        }
        Some(plan)
    }
}

impl SplitNowAndRule {
    pub fn create() -> BoxedRule {
        Box::new(SplitNowAndRule {})
    }
}
