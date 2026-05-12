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

use risingwave_common::types::DataType;

use crate::expr::{ExprImpl, ExprType, FunctionCall};
use crate::optimizer::plan_node::{LogicalFilter, LogicalShare, LogicalUnion, PlanTreeNodeUnary};
use crate::optimizer::rule::prelude::{PlanRef, *};

/// Convert `LogicalFilter` with now or others predicates to a `UNION ALL`
///
/// Before:
/// ```
/// `LogicalFilter`
///  now() or others
///        |
///      Input
/// ```
///
/// After:
/// ```text
///         `LogicalUnionAll`
///         /              \
/// `LogicalFilter`     `LogicalFilter`
/// now() & others IS      others
///        NOT TRUE
///         |               |
///         \              /
///         `LogicalShare`
///               |
///             Input
/// ```text
pub struct SplitNowOrRule {}
impl Rule<Logical> for SplitNowOrRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        let input = filter.input();

        if filter.predicate().conjunctions.len() != 1 {
            return None;
        }

        let disjunctions = filter.predicate().conjunctions[0].as_or_disjunctions()?;

        if disjunctions.len() < 2 {
            return None;
        }

        let (now, others): (Vec<ExprImpl>, Vec<ExprImpl>) =
            disjunctions.into_iter().partition(|x| x.count_nows() != 0);

        // Only support now in one arm of disjunctions
        if now.len() != 1 {
            return None;
        }

        // A or B or C ... or Z, where A is the now() arm.
        // =>
        // + A & (B | C ... | Z) IS NOT TRUE
        // + B | C ... | Z
        //
        // Do not use `NOT (B | C ... | Z)` here: in SQL three-valued logic,
        // `NOT NULL` is still `NULL`, while this branch must keep rows where
        // the non-now arms are not true and the now() arm is true.

        let arm2 = ExprImpl::or(others);
        let arm1 = ExprImpl::and([
            now.into_iter()
                .next()
                .expect("there should be exactly one now() arm"),
            FunctionCall::new_unchecked(ExprType::IsNotTrue, vec![arm2.clone()], DataType::Boolean)
                .into(),
        ]);

        let share = LogicalShare::create(input);
        let filter1 = LogicalFilter::create_with_expr(share.clone(), arm1);
        let filter2 = LogicalFilter::create_with_expr(share, arm2);
        let union_all = LogicalUnion::create(true, vec![filter1, filter2]);
        Some(union_all)
    }
}

impl SplitNowOrRule {
    pub fn create() -> BoxedRule {
        Box::new(SplitNowOrRule {})
    }
}
