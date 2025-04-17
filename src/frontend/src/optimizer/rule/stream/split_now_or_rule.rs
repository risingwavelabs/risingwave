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

use risingwave_common::types::DataType;

use crate::expr::{ExprImpl, ExprType, FunctionCall};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalFilter, LogicalShare, LogicalUnion, PlanTreeNodeUnary};
use crate::optimizer::rule::{BoxedRule, Rule};

/// Convert `LogicalFilter` with now or others predicates to a `UNION ALL`
///
/// Before:
/// ```text
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
/// now() & !others        others
///         |               |
///         \              /
///         `LogicalShare`
///               |
///             Input
/// ```text
pub struct SplitNowOrRule {}
impl Rule for SplitNowOrRule {
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

        // A or B or C ... or Z
        // =>
        // + A & !B & !C ... &!Z
        // + B | C ... | Z

        let arm1 = ExprImpl::and(now.into_iter().chain(others.iter().map(|pred| {
            FunctionCall::new_unchecked(ExprType::Not, vec![pred.clone()], DataType::Boolean).into()
        })));
        let arm2 = ExprImpl::or(others);

        let share = LogicalShare::create(input);
        let filter1 = LogicalFilter::create_with_expr(share.clone(), arm1);
        let filter2 = LogicalFilter::create_with_expr(share.clone(), arm2);
        let union_all = LogicalUnion::create(true, vec![filter1, filter2]);
        Some(union_all)
    }
}

impl SplitNowOrRule {
    pub fn create() -> BoxedRule {
        Box::new(SplitNowOrRule {})
    }
}
