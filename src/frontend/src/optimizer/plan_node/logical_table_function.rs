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

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use super::{ColPrunable, LogicalFilter, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream};
use crate::binder::FunctionType;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::BatchTableFunction;
use crate::session::OptimizerContextRef;
use crate::utils::Condition;

/// `LogicalGenerateSeries` implements Hop Table Function.
#[derive(Debug, Clone)]
pub struct LogicalTableFunction {
    pub base: PlanBase,
    pub(super) args: Vec<ExprImpl>,
    pub function_type: FunctionType,
    pub data_type: DataType,
}

impl LogicalTableFunction {
    /// Create a [`LogicalTableFunction`] node. Used internally by optimizer.
    pub fn new(
        args: Vec<ExprImpl>,
        function_type: FunctionType,
        data_type: DataType,
        ctx: OptimizerContextRef,
    ) -> Self {
        let schema = Schema {
            fields: vec![Field::with_name(data_type.clone(), function_type.name())],
        };
        let base = PlanBase::new_logical(ctx, schema, vec![]);
        Self {
            base,
            args,
            function_type,
            data_type,
        }
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        match self.function_type {
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
