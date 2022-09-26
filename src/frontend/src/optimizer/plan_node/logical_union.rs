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

use itertools::Itertools;
use risingwave_common::error::Result;

use super::{ColPrunable, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream};
use crate::optimizer::plan_node::{BatchHashAgg, BatchUnion, LogicalAgg, PlanTreeNode};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalUnion` returns the union of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone)]
pub struct LogicalUnion {
    pub base: PlanBase,
    all: bool,
    inputs: Vec<PlanRef>,
}

impl LogicalUnion {
    pub fn new(all: bool, inputs: Vec<PlanRef>) -> Self {
        let ctx = inputs[0].ctx();
        let schema = inputs[0].schema().clone();
        let mut pk_indices = vec![];
        for input in &inputs {
            for pk in input.logical_pk() {
                if !pk_indices.contains(pk) {
                    pk_indices.push(*pk);
                }
            }
        }
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);
        LogicalUnion { base, all, inputs }
    }

    pub fn create(all: bool, inputs: Vec<PlanRef>) -> PlanRef {
        LogicalUnion::new(all, inputs).into()
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(f, "{} {{ all: {} }}", name, self.all)
    }

    pub fn all(&self) -> bool {
        self.all
    }
}

impl PlanTreeNode for LogicalUnion {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.inputs.clone().into_iter());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new(self.all, inputs.to_vec()).into()
    }
}

impl fmt::Display for LogicalUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalUnion")
    }
}

impl ColPrunable for LogicalUnion {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.prune_col(required_cols))
            .collect_vec();
        self.clone_with_inputs(&new_inputs)
    }
}

impl PredicatePushdown for LogicalUnion {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        let new_inputs = self
            .inputs()
            .iter()
            .map(|input| input.predicate_pushdown(predicate.clone()))
            .collect_vec();
        self.clone_with_inputs(&new_inputs)
    }
}

impl ToBatch for LogicalUnion {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_inputs: Result<Vec<_>> =
            self.inputs().iter().map(|input| input.to_batch()).collect();
        let new_logical = Self::new(true, new_inputs?);
        // convert union to union all + agg
        if !self.all {
            let batch_union = BatchUnion::new(new_logical).into();
            Ok(BatchHashAgg::new(LogicalAgg::new(
                vec![],
                (0..self.base.schema.len()).collect_vec(),
                batch_union,
            ))
            .into())
        } else {
            Ok(BatchUnion::new(new_logical).into())
        }
    }
}

impl ToStream for LogicalUnion {
    fn to_stream(&self) -> Result<PlanRef> {
        todo!()
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::optimizer::plan_node::{LogicalValues, PlanTreeNodeUnary};
    use crate::session::OptimizerContext;

    #[tokio::test]
    async fn test_prune_union() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values1 = LogicalValues::new(vec![], Schema { fields }, ctx);

        let values2 = values1.clone();

        let union = LogicalUnion::new(false, vec![values1.into(), values2.into()]);

        // Perform the prune
        let required_cols = vec![1, 2];
        let plan = union.prune_col(&required_cols);

        // Check the result
        let union = plan.as_logical_union().unwrap();
        assert_eq!(union.base.schema.len(), 2);
    }

    #[tokio::test]
    async fn test_union_to_batch() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values1 = LogicalValues::new(vec![], Schema { fields }, ctx);

        let values2 = values1.clone();

        let union = LogicalUnion::new(false, vec![values1.into(), values2.into()]);

        let plan = union.to_batch().unwrap();
        let agg: &BatchHashAgg = plan.as_batch_hash_agg().unwrap();
        let agg_input = agg.input();
        let union = agg_input.as_batch_union().unwrap();

        assert_eq!(union.inputs().len(), 2);
    }
}
