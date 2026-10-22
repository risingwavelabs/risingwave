// Copyright 2026 RisingWave Labs
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

use super::prelude::*;
use crate::expr::{Expr, ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{PlanTreeNodeUnary, StreamProject, generic};

/// Separate synchronous computations from asynchronous expressions. Column references are allowed
/// in both layers to preserve stream keys, distribution, watermarks and the final output order.
pub struct StreamProjectSplitRule;

impl Rule<Stream> for StreamProjectSplitRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project = plan.as_stream_project()?;
        if !has_mixed_computations(project.exprs()) {
            return None;
        }

        let input = project.input();
        let input_len = input.schema().len();
        let mut async_exprs: Vec<ExprImpl> = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| InputRef::new(idx, field.data_type()).into())
            .collect();
        let mut sync_core = project.core().clone();
        sync_core.field_names = project
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| (idx, field.name.clone()))
            .collect();
        for expr in &mut sync_core.exprs {
            if expr.is_async() {
                let result = InputRef::new(async_exprs.len(), expr.return_type()).into();
                // Move each occurrence exactly once. Never extract children from lazy expressions
                // and never inline a shared result into multiple consumers.
                async_exprs.push(std::mem::replace(expr, result));
            }
        }
        debug_assert!(async_exprs.len() > input_len);
        sync_core.input = StreamProject::new(generic::Project::new(async_exprs, input))
            .with_noop_update_hint(false)
            .into();
        Some(
            StreamProject::new(sync_core)
                .with_noop_update_hint(project.noop_update_hint())
                .into(),
        )
    }
}

pub(super) fn has_mixed_computations(exprs: &[ExprImpl]) -> bool {
    exprs.iter().any(ExprImpl::is_async)
        && exprs
            .iter()
            .any(|expr| !matches!(expr, ExprImpl::InputRef(_)) && !expr.is_async())
}

impl StreamProjectSplitRule {
    pub fn create() -> BoxedRule {
        Box::new(Self)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::iter_util::ZipEqFast;

    use super::*;
    use crate::expr::{ExprType, FunctionCall};
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::StreamValues;
    use crate::optimizer::plan_node::generic::PhysicalPlanRef;
    use crate::optimizer::rule::StreamProjectMergeRule;

    #[tokio::test]
    async fn split_preserves_nested_evaluation_and_shared_results() {
        let input: PlanRef = StreamValues::new(generic::Values::new_with_stream_key(
            vec![],
            Schema::new(vec![Field::with_name(DataType::Float64, "seconds")]),
            OptimizerContext::mock(),
            vec![0],
        ))
        .into();
        let sleep: ExprImpl = FunctionCall::new_unchecked(
            ExprType::PgSleep,
            vec![InputRef::new(0, DataType::Float64).into()],
            DataType::Int32,
        )
        .into();
        // A false CASE branch must not become an unconditional external evaluation.
        let conditional: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Case,
            vec![
                ExprImpl::literal_bool(false),
                sleep,
                ExprImpl::literal_int(9),
            ],
            DataType::Int32,
        )
        .into();
        let exprs = vec![
            conditional.clone(),
            ExprImpl::literal_int(7),
            InputRef::new(0, DataType::Float64).into(),
        ];
        let original: PlanRef = StreamProject::new(generic::Project::new(exprs, input)).into();
        let split = StreamProjectSplitRule.apply(original.clone()).unwrap();
        assert_eq!(split.schema(), original.schema());
        assert_eq!(split.stream_key(), original.stream_key());
        assert_eq!(split.distribution(), original.distribution());
        for (before, after) in original
            .schema()
            .fields()
            .iter()
            .zip_eq_fast(split.schema().fields())
        {
            assert_eq!(before.name, after.name);
        }
        let outer = split.as_stream_project().unwrap();
        assert!(outer.exprs().iter().all(|expr| !expr.is_async()));
        let inner_plan = outer.input();
        let inner = inner_plan.as_stream_project().unwrap();
        assert_eq!(
            inner.exprs(),
            &vec![InputRef::new(0, DataType::Float64).into(), conditional]
        );
        assert!(StreamProjectSplitRule.apply(inner_plan.clone()).is_none());
        assert!(StreamProjectMergeRule {}.apply(split.clone()).is_none());

        // A later merge must not move an evaluated async result under a lazy branch, even when
        // the merged project would contain just one (asynchronous) expression.
        let lazy_consumer = StreamProject::new(generic::Project::new(
            vec![
                FunctionCall::new_unchecked(
                    ExprType::Case,
                    vec![
                        ExprImpl::literal_bool(false),
                        InputRef::new(1, DataType::Int32).into(),
                        ExprImpl::literal_int(0),
                    ],
                    DataType::Int32,
                )
                .into(),
            ],
            inner_plan,
        ));
        assert!(
            StreamProjectMergeRule {}
                .apply(lazy_consumer.into())
                .is_none()
        );

        // Consumers retain one reference to the evaluated result for each use, rather than
        // substituting the conditional async tree and performing the call twice.
        let consumers = StreamProject::new(generic::Project::new(
            vec![InputRef::new(0, DataType::Int32).into(); 2],
            split,
        ));
        let merged = StreamProjectMergeRule {}.apply(consumers.into()).unwrap();
        assert!(
            merged
                .as_stream_project()
                .unwrap()
                .exprs()
                .iter()
                .all(|e| !e.is_async())
        );
        let lower = merged.as_stream_project().unwrap().input();
        assert_eq!(
            lower
                .as_stream_project()
                .unwrap()
                .exprs()
                .iter()
                .filter(|e| e.is_async())
                .count(),
            1
        );
    }
}
