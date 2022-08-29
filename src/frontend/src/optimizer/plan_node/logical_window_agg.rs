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

use risingwave_common::error::{ErrorCode, Result};

use super::{ColPrunable, LogicalFilter, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream};
use crate::expr::ExprImpl;
use crate::utils::Condition;

#[derive(Debug, Clone)]
pub struct LogicalWindowAgg {
    pub base: PlanBase,
}

impl LogicalWindowAgg {
    // fn new(ctx: OptimizerContextRef) -> Self {
    //     let schema = Schema { fields: vec![] };
    //     let functional_dependency = FunctionalDependencySet::new(schema.len());
    //     let base = PlanBase::new_logical(ctx, schema, vec![], functional_dependency);
    //     Self { base }
    // }

    pub fn create(
        _input: PlanRef,
        _select_exprs: Vec<ExprImpl>,
    ) -> Result<(PlanRef, Vec<ExprImpl>)> {
        Err(ErrorCode::NotImplemented("plan LogicalWindowAgg".to_string(), 4847.into()).into())
    }
}

impl_plan_tree_node_for_leaf! { LogicalWindowAgg }

impl fmt::Display for LogicalWindowAgg {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
        // write!(f, "LogicalWindowAgg {{ {:?} }}", )
    }
}

impl ColPrunable for LogicalWindowAgg {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let _ = required_cols;
        self.clone().into()
    }
}

impl PredicatePushdown for LogicalWindowAgg {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalWindowAgg {
    fn to_batch(&self) -> Result<PlanRef> {
        todo!();
    }
}

impl ToStream for LogicalWindowAgg {
    fn to_stream(&self) -> Result<PlanRef> {
        todo!()
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, crate::utils::ColIndexMapping)> {
        todo!()
    }
}
