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
use risingwave_common::types::DataType;

use super::{ColPrunable, LogicalFilter, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::BatchTableFunction;
use crate::session::OptimizerContextRef;
use crate::utils::Condition;
/// `LogicalGenerateSeries` implements Hop Table Function.
#[derive(Debug, Clone)]
pub struct LogicalTableFunction {
    pub base: PlanBase,
    pub(super) args: Vec<ExprImpl>,
    pub series_type: FunctionType,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionType {
    Generate,
    Unnest,
}

impl LogicalTableFunction {
    /// Create a [`LogicalTableFunction`] node. Used internally by optimizer.
    pub fn new(
        args: Vec<ExprImpl>,
        schema: Schema,
        series_type: FunctionType,
        data_type: DataType,
        ctx: OptimizerContextRef,
    ) -> Self {
        let base = PlanBase::new_logical(ctx, schema, vec![]);

        Self {
            base,
            args,
            series_type,
            data_type,
        }
    }

    /// Create a [`LogicalTableFunction`] node. Used by planner.
    pub fn create_generate_series(
        start: ExprImpl,
        stop: ExprImpl,
        step: ExprImpl,
        schema: Schema,
        ctx: OptimizerContextRef,
    ) -> PlanRef {
        // No additional checks after binder.
        let data_type = start.return_type();
        Self::new(
            vec![start, stop, step],
            schema,
            FunctionType::Generate,
            data_type,
            ctx,
        )
        .into()
    }

    pub fn create_unnest(args: Vec<ExprImpl>, schema: Schema, ctx: OptimizerContextRef) -> PlanRef {
        // No additional checks after binder.
        let data_type = args[0].return_type();
        Self::new(args, schema, FunctionType::Unnest, data_type, ctx).into()
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        match self.series_type {
            FunctionType::Generate => {
                write!(
                    f,
                    "{} {{ start: {:?} stop: {:?} step: {:?} }}",
                    name, self.args[0], self.args[1], self.args[2],
                )
            }
            FunctionType::Unnest => {
                write!(f, "{} {{ {:?} }}", name, self.args,)
            }
        }
    }
}

impl_plan_tree_node_for_leaf! { LogicalTableFunction }

impl fmt::Display for LogicalTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_name(f, "LogicalTableFunction")
    }
}

// the leaf node don't need colprunable
impl ColPrunable for LogicalTableFunction {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let _ = required_cols;
        self.clone().into()
    }
}

impl PredicatePushdown for LogicalTableFunction {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalTableFunction {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchTableFunction::new(self.clone()).into())
    }
}

impl ToStream for LogicalTableFunction {
    fn to_stream(&self) -> Result<PlanRef> {
        Err(
            ErrorCode::NotImplemented("LogicalTableFunction::to_stream".to_string(), None.into())
                .into(),
        )
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, crate::utils::ColIndexMapping)> {
        Err(ErrorCode::NotImplemented(
            "LogicalTableFunction::logical_rewrite_for_stream".to_string(),
            None.into(),
        )
        .into())
    }
}
