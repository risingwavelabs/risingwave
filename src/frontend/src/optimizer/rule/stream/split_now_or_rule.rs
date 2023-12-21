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
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::optimizer::PlanRef;

/// Convert `LogicalFilter` with now or others predicates to a `UNION ALL`
///
/// Before:
/// `LogicalFilter`
///  now() or others
///        |
///      Input
///
/// After:
///         `LogicalUnionAll`
///       /                  \
/// `LogicalFilter`       `LogicalFilter`
/// now() & !others           others
///     |                     |
///     \                    /
///         `LogicalShare`
///               |
///             Input
pub struct SplitNowOrRule {}
impl Rule for SplitNowOrRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        let input = filter.input();

        if filter.predicate().conjunctions.len() != 1 {
            return None;
        }

        let disjunctions = filter.predicate().conjunctions[0].as_or_disjunctions()?;

        // TODO: handle disjuctions with arms more than 2
        if disjunctions.len() != 2 {
            return None;
        }

        let cnt1 = disjunctions[0].count_nows();
        let cnt2 = disjunctions[1].count_nows();

        let (now_part, others) = if cnt1 == 1 && cnt2 == 0 {
            (disjunctions[0].clone(), disjunctions[1].clone())
        } else if cnt1 == 0 && cnt2 == 1 {
            (disjunctions[1].clone(), disjunctions[0].clone())
        } else {
            return None;
        };

        let share = LogicalShare::create(input);

        let not_others: ExprImpl =
            FunctionCall::new_unchecked(ExprType::Not, vec![others.clone()], DataType::Boolean)
                .into();
        let now_and_not_others = FunctionCall::new_unchecked(
            ExprType::And,
            vec![now_part, not_others],
            DataType::Boolean,
        )
        .into();
        let filter_with_now = LogicalFilter::create_with_expr(share.clone(), now_and_not_others);
        let filter_without_now = LogicalFilter::create_with_expr(share, others);

        let union_all = LogicalUnion::create(true, vec![filter_with_now, filter_without_now]);

        Some(union_all)
    }
}

impl SplitNowOrRule {
    pub fn create() -> BoxedRule {
        Box::new(SplitNowOrRule {})
    }
}
