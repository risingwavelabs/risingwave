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

use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};

use super::{ColPrunable, LogicalFilter, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream};
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::BatchGenerateSeries;
use crate::session::OptimizerContextRef;
use crate::utils::Condition;
/// `LogicalGenerateSeries` implements Hop Table Function.
#[derive(Debug, Clone)]
pub struct LogicalGenerateSeries {
    pub base: PlanBase,
    pub(super) start: ExprImpl,
    pub(super) stop: ExprImpl,
    pub(super) step: ExprImpl,
}

impl LogicalGenerateSeries {
    /// Create a [`LogicalGenerateSeries`] node. Used internally by optimizer.
    pub fn new(
        start: ExprImpl,
        stop: ExprImpl,
        step: ExprImpl,
        schema: Schema,
        ctx: OptimizerContextRef,
    ) -> Self {
        let base = PlanBase::new_logical(ctx, schema, vec![]);

        Self {
            base,
            start,
            stop,
            step,
        }
    }

    /// Create a [`LogicalGenerateSeries`] node. Used by planner.
    pub fn create(
        start: ExprImpl,
        stop: ExprImpl,
        step: ExprImpl,
        schema: Schema,
        ctx: OptimizerContextRef,
    ) -> PlanRef {
        // No additional checks after binder.
        Self::new(start, stop, step, schema, ctx).into()
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ start: {:?} stop: {:?} step: {:?} }}",
            name, self.start, self.stop, self.step,
        )
    }
}

impl_plan_tree_node_for_leaf! { LogicalGenerateSeries }

impl fmt::Display for LogicalGenerateSeries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_name(f, "LogicalGenerateSeries")
    }
}

// the leaf node don't need colprunable
impl ColPrunable for LogicalGenerateSeries {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let _ = required_cols;
        self.clone().into()
    }
}

impl PredicatePushdown for LogicalGenerateSeries {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalGenerateSeries {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchGenerateSeries::new(self.clone()).into())
    }
}

impl ToStream for LogicalGenerateSeries {
    fn to_stream(&self) -> Result<PlanRef> {
        Err(
            ErrorCode::NotImplemented("LogicalGenerateSeries::to_stream".to_string(), None.into())
                .into(),
        )
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, crate::utils::ColIndexMapping)> {
        Err(ErrorCode::NotImplemented(
            "LogicalGenerateSeries::logical_rewrite_for_stream".to_string(),
            None.into(),
        )
        .into())
    }
}
