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

use std::cell::RefCell;
use std::fmt;

use risingwave_common::error::ErrorCode::NotImplemented;
use risingwave_common::error::Result;

use super::generic::{self, GenericPlanNode};
use super::{
    ColPrunable, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown, ToBatch,
    ToStream,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamShare,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalShare` operator is used to represent reusing of existing operators.
/// It is the key operator for DAG plan.
/// It could have multiple parents which makes it different from other operators.
/// Currently, it has been used to the following scenarios:
/// 1. Share source.
/// 2. Subquery unnesting domain calculation.
///
/// A DAG plan example: A self join shares the same source.
/// ```text
///     LogicalJoin
///    /           \
///   |            |
///   \           /
///   LogicalShare
///        |
///   LogicalSource
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalShare {
    pub base: PlanBase,
    core: generic::Share<PlanRef>,
}

impl LogicalShare {
    pub fn new(input: PlanRef) -> Self {
        let ctx = input.ctx();
        let functional_dependency = input.functional_dependency().clone();
        let core = generic::Share {
            input: RefCell::new(input),
        };
        let schema = core.schema();
        let pk_indices = core.logical_pk();
        let base = PlanBase::new_logical(
            ctx,
            schema,
            pk_indices.unwrap_or_default(),
            functional_dependency,
        );
        LogicalShare { base, core }
    }

    pub fn create(input: PlanRef) -> PlanRef {
        LogicalShare::new(input).into()
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(f, "{} {{ id = {} }}", name, &self.id().0)
    }
}

impl PlanTreeNodeUnary for LogicalShare {
    fn input(&self) -> PlanRef {
        self.core.input.borrow().clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input)
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (Self::new(input), input_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalShare}

impl LogicalShare {
    pub fn replace_input(&self, plan: PlanRef) {
        *self.core.input.borrow_mut() = plan;
    }
}

impl fmt::Display for LogicalShare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalShare")
    }
}

impl ColPrunable for LogicalShare {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        unimplemented!("call prune_col of the PlanRef instead of calling directly on LogicalShare")
    }
}

impl ExprRewritable for LogicalShare {}

impl PredicatePushdown for LogicalShare {
    fn predicate_pushdown(
        &self,
        _predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        unimplemented!(
            "call predicate_pushdown of the PlanRef instead of calling directly on LogicalShare"
        )
    }
}

impl ToBatch for LogicalShare {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(NotImplemented(
            "batch query doesn't support share operator for now".into(),
            None.into(),
        )
        .into())
    }
}

impl ToStream for LogicalShare {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        match ctx.get_to_stream_result(self.id()) {
            None => {
                let new_input = self.input().to_stream(ctx)?;
                let new_logical = self.clone_with_input(new_input);
                let stream_share_ref: PlanRef = StreamShare::new(new_logical).into();
                ctx.add_to_stream_result(self.id(), stream_share_ref.clone());
                Ok(stream_share_ref)
            }
            Some(cache) => Ok(cache.clone()),
        }
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        match ctx.get_rewrite_result(self.id()) {
            None => {
                let (new_input, col_change) = self.input().logical_rewrite_for_stream(ctx)?;
                let new_share: PlanRef = self.clone_with_input(new_input).into();

                // FIXME: Add an identity project here to avoid parent exchange connecting directly
                // to the share operator.
                // let identity = ColIndexMapping::identity(new_share.schema().len());
                // let project: PlanRef = LogicalProject::with_mapping(new_share, identity).into();

                ctx.add_rewrite_result(self.id(), new_share.clone(), col_change.clone());
                Ok((new_share, col_change))
            }
            Some(cache) => Ok(cache.clone()),
        }
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_pb::expr::expr_node::Type;
    use risingwave_pb::plan_common::JoinType;

    use super::*;
    use crate::expr::{ExprImpl, FunctionCall, InputRef, Literal};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::{
        LogicalFilter, LogicalJoin, LogicalValues, PlanTreeNodeBinary,
    };

    #[tokio::test]
    async fn test_share_predicate_pushdown() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values1 = LogicalValues::new(vec![], Schema { fields }, ctx);

        let share: PlanRef = LogicalShare::create(values1.into());

        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty.clone()))),
                ],
            )
            .unwrap(),
        ));

        let predicate1: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(0, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(
                        Some(ScalarImpl::from(100)),
                        DataType::Int32,
                    ))),
                ],
            )
            .unwrap(),
        ));

        let predicate2: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(4, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(
                        Some(ScalarImpl::from(200)),
                        DataType::Int32,
                    ))),
                ],
            )
            .unwrap(),
        ));

        let join: PlanRef = LogicalJoin::create(share.clone(), share, JoinType::Inner, on);

        let filter1: PlanRef = LogicalFilter::create_with_expr(join, predicate1);

        let filter2: PlanRef = LogicalFilter::create_with_expr(filter1, predicate2);

        let result = filter2.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(filter2.clone()),
        );

        // LogicalJoin { type: Inner, on: (v2 = v1) }
        // ├─LogicalFilter { predicate: (v1 = 100:Int32) }
        // | └─LogicalShare { id = 2 }
        // |   └─LogicalFilter { predicate: ((v1 = 100:Int32) OR (v2 = 200:Int32)) }
        // |     └─LogicalValues { schema: Schema { fields: [v1:Int32, v2:Int32, v3:Int32] } }
        // └─LogicalFilter { predicate: (v2 = 200:Int32) }
        //   └─LogicalShare { id = 2 }
        //     └─LogicalFilter { predicate: ((v1 = 100:Int32) OR (v2 = 200:Int32)) }
        //       └─LogicalValues { schema: Schema { fields: [v1:Int32, v2:Int32, v3:Int32] } }

        let logical_join: &LogicalJoin = result.as_logical_join().unwrap();
        let left = logical_join.left();
        let left_filter: &LogicalFilter = left.as_logical_filter().unwrap();
        let left_filter_input = left_filter.input();
        let logical_share: &LogicalShare = left_filter_input.as_logical_share().unwrap();
        let share_input = logical_share.input();
        let share_input_filter: &LogicalFilter = share_input.as_logical_filter().unwrap();
        let disjunctions = share_input_filter.predicate().conjunctions[0]
            .as_or_disjunctions()
            .unwrap();
        assert_eq!(disjunctions.len(), 2);
        let (input_ref1, _const1) = disjunctions[0].as_eq_const().unwrap();
        let (input_ref2, _const2) = disjunctions[1].as_eq_const().unwrap();
        if input_ref1.index() == 0 {
            assert_eq!(input_ref2.index(), 1);
        } else {
            assert_eq!(input_ref1.index(), 1);
            assert_eq!(input_ref2.index(), 0);
        }
    }
}
