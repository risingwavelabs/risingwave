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
use risingwave_common::types::DataType;

use crate::expr::{ExprImpl, ExprType, FunctionCall};
use crate::optimizer::plan_node::{LogicalFilter, LogicalShare, LogicalUnion, PlanTreeNodeUnary};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::optimizer::PlanRef;

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

        if now.len() != 1 {
            return None;
        }

        // Put the now at the first position
        let predicates = now.into_iter().chain(others).collect_vec();

        // A or B
        // =>
        // + A & !B
        // + B
        //
        // A or B or C
        // =>
        // + A & !B & !C
        // + B & !C
        // + C

        let len = predicates.len();
        let mut new_arms = Vec::with_capacity(len);
        #[allow(clippy::needless_range_loop)]
        for i in 0..len {
            let mut arm = predicates[i].clone();
            for j in (i + 1)..len {
                let others = predicates[j].clone();
                let not_others: ExprImpl = FunctionCall::new_unchecked(
                    ExprType::Not,
                    vec![others.clone()],
                    DataType::Boolean,
                )
                .into();
                arm = FunctionCall::new_unchecked(
                    ExprType::And,
                    vec![arm, not_others],
                    DataType::Boolean,
                )
                .into();
            }
            new_arms.push(arm)
        }

        let share = LogicalShare::create(input);
        let filters = new_arms
            .into_iter()
            .map(|x| LogicalFilter::create_with_expr(share.clone(), x))
            .collect_vec();
        let union_all = LogicalUnion::create(true, filters);
        Some(union_all)
    }
}

impl SplitNowOrRule {
    pub fn create() -> BoxedRule {
        Box::new(SplitNowOrRule {})
    }
}
