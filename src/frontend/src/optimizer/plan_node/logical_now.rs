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

use std::fmt;

use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::utils::IndicesDisplay;
use super::{
    ColPrunable, ColumnPruningContext, ExprRewritable, LogicalFilter, PlanBase, PlanRef,
    PredicatePushdown, RewriteStreamContext, StreamNow, ToBatch, ToStream, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::ColIndexMapping;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalNow {
    pub base: PlanBase,
}

impl LogicalNow {
    pub fn new(ctx: OptimizerContextRef) -> Self {
        let schema = Schema::new(vec![Field {
            data_type: DataType::Timestamptz,
            name: String::from("now"),
            sub_fields: vec![],
            type_name: String::default(),
        }]);
        let base = PlanBase::new_logical(ctx, schema, vec![], FunctionalDependencySet::default());
        Self { base }
    }
}

impl fmt::Display for LogicalNow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("LogicalNow");

        if verbose {
            // For now, output all columns from the left side. Make it explicit here.
            builder.field(
                "output",
                &IndicesDisplay {
                    indices: &(0..self.schema().fields.len()).collect_vec(),
                    input_schema: self.schema(),
                },
            );
        }

        builder.finish()
    }
}

impl_plan_tree_node_for_leaf! { LogicalNow }

impl ExprRewritable for LogicalNow {}

impl PredicatePushdown for LogicalNow {
    fn predicate_pushdown(
        &self,
        predicate: crate::utils::Condition,
        _ctx: &mut super::PredicatePushdownContext,
    ) -> crate::PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToStream for LogicalNow {
    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((self.clone().into(), ColIndexMapping::new(vec![Some(0)])))
    }

    /// `to_stream` is equivalent to `to_stream_with_dist_required(RequiredDist::Any)`
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        Ok(StreamNow::new(self.clone(), self.ctx()).into())
    }
}

impl ToBatch for LogicalNow {
    fn to_batch(&self) -> Result<PlanRef> {
        bail!("`LogicalNow` can only be converted to stream")
    }
}

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
impl ColPrunable for LogicalNow {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        self.clone().into()
    }
}
