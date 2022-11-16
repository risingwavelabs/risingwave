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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Scalar};

use super::{ColPrunable, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream};
use crate::expr::{ExprImpl, InputRef, Literal};
use crate::optimizer::plan_node::generic::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::stream_union::StreamUnion;
use crate::optimizer::plan_node::{
    generic, BatchHashAgg, BatchUnion, LogicalAgg, LogicalProject, PlanTreeNode,
};
use crate::optimizer::property::{FunctionalDependencySet, RequiredDist};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalUnion` returns the union of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone)]
pub struct LogicalUnion {
    pub base: PlanBase,
    core: generic::Union<PlanRef>,
}

impl LogicalUnion {
    pub fn new(all: bool, inputs: Vec<PlanRef>) -> Self {
        Self::new_with_source_col(all, inputs, None)
    }

    /// It is used by streaming processing. We need to use `source_col` to identify the record came
    /// from which source input.
    pub fn new_with_source_col(all: bool, inputs: Vec<PlanRef>, source_col: Option<usize>) -> Self {
        let core = generic::Union {
            all,
            inputs,
            source_col,
        };
        let ctx = core.ctx();
        let pk_indices = core.logical_pk();
        let schema = core.schema();
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(
            ctx,
            schema,
            pk_indices.unwrap_or_default(),
            functional_dependency,
        );
        LogicalUnion { base, core }
    }

    pub fn create(all: bool, inputs: Vec<PlanRef>) -> PlanRef {
        LogicalUnion::new(all, inputs).into()
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(f, "{} {{ all: {} }}", name, self.core.all)
    }

    pub fn all(&self) -> bool {
        self.core.all
    }

    pub fn source_col(&self) -> Option<usize> {
        self.core.source_col
    }
}

impl PlanTreeNode for LogicalUnion {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.core.inputs.clone().into_iter());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new_with_source_col(self.all(), inputs.to_vec(), self.core.source_col).into()
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
        if !self.all() {
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
        // TODO: use round robin distribution instead of using single distribution of all inputs.
        let new_inputs: Result<Vec<_>> = self
            .inputs()
            .iter()
            .map(|input| input.to_stream_with_dist_required(&RequiredDist::single()))
            .collect();
        let new_logical = Self::new_with_source_col(true, new_inputs?, self.core.source_col);
        if !self.all() {
            // TODO: we should rely on optimizer to transform not all to all. after that, we can use
            // assert instead of return an error.
            Err(
                ErrorCode::NotImplemented("Union for streaming query".to_string(), 2911.into())
                    .into(),
            )
        } else {
            Ok(StreamUnion::new(new_logical).into())
        }
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let original_schema = self.base.schema.clone();
        let original_schema_len = original_schema.len();
        let mut rewrites = vec![];
        for input in &self.core.inputs {
            rewrites.push(input.logical_rewrite_for_stream()?);
        }

        let original_schema_contain_all_input_pks =
            rewrites.iter().all(|(new_input, col_index_mapping)| {
                let original_schema_new_pos = (0..original_schema_len)
                    .map(|x| col_index_mapping.map(x))
                    .collect_vec();
                new_input
                    .logical_pk()
                    .iter()
                    .all(|x| original_schema_new_pos.contains(x))
            });

        if original_schema_contain_all_input_pks {
            // Add one more column at the end of the original schema to identify the record came
            // from which input. [original_schema + source_col]
            let new_inputs = rewrites
                .into_iter()
                .enumerate()
                .map(|(i, (new_input, col_index_mapping))| {
                    // original_schema
                    let mut exprs = (0..original_schema_len)
                        .map(|x| {
                            ExprImpl::InputRef(
                                InputRef::new(
                                    col_index_mapping.map(x),
                                    original_schema.fields[x].data_type.clone(),
                                )
                                .into(),
                            )
                        })
                        .collect_vec();
                    // source_col
                    exprs.push(ExprImpl::Literal(
                        Literal::new(Some((i as i32).to_scalar_value()), DataType::Int32).into(),
                    ));
                    LogicalProject::create(new_input, exprs)
                })
                .collect_vec();
            let new_union = LogicalUnion::new_with_source_col(
                self.all(),
                new_inputs,
                Some(original_schema_len),
            );
            // We have already used project to map rewrite input to the origin schema, so we can use
            // identity with the new schema len.
            let out_col_change =
                ColIndexMapping::identity_or_none(original_schema_len, new_union.schema().len());
            Ok((new_union.into(), out_col_change))
        } else {
            // In order to ensure all inputs have the same schema for new union, we construct new
            // schema like that: [original_schema + input1_pk + input2_pk + ... +
            // source_col]
            let input_pk_types = rewrites
                .iter()
                .flat_map(|(new_input, _)| {
                    new_input
                        .logical_pk()
                        .iter()
                        .map(|x| new_input.schema().fields[*x].data_type())
                })
                .collect_vec();
            let input_pk_nulls = input_pk_types
                .iter()
                .map(|t| ExprImpl::Literal(Literal::new(None, t.clone()).into()))
                .collect_vec();
            let input_pk_lens = rewrites
                .iter()
                .map(|(new_input, _)| new_input.logical_pk().len())
                .collect_vec();
            let mut input_pk_offsets = vec![0];
            for (i, len) in input_pk_lens.into_iter().enumerate() {
                input_pk_offsets.push(input_pk_offsets[i] + len)
            }
            let new_inputs = rewrites
                .into_iter()
                .enumerate()
                .map(|(i, (new_input, col_index_mapping))| {
                    // original_schema
                    let mut exprs = (0..original_schema_len)
                        .map(|x| {
                            ExprImpl::InputRef(
                                InputRef::new(
                                    col_index_mapping.map(x),
                                    original_schema.fields[x].data_type.clone(),
                                )
                                .into(),
                            )
                        })
                        .collect_vec();
                    // input1_pk + input2_pk + ...
                    let mut input_pks = input_pk_nulls.clone();
                    for (j, pk_idx) in new_input.logical_pk().iter().enumerate() {
                        input_pks[input_pk_offsets[i] + j] = ExprImpl::InputRef(
                            InputRef::new(
                                *pk_idx,
                                new_input.schema().fields[*pk_idx].data_type.clone(),
                            )
                            .into(),
                        );
                    }
                    exprs.extend(input_pks);
                    // source_col
                    exprs.push(ExprImpl::Literal(
                        Literal::new(Some((i as i32).to_scalar_value()), DataType::Int32).into(),
                    ));
                    LogicalProject::create(new_input, exprs)
                })
                .collect_vec();

            let new_union = LogicalUnion::new_with_source_col(
                self.all(),
                new_inputs,
                Some(original_schema_len + input_pk_types.len()),
            );
            // We have already used project to map rewrite input to the origin schema, so we can use
            // identity with the new schema len.
            let out_col_change =
                ColIndexMapping::identity_or_none(original_schema_len, new_union.schema().len());
            Ok((new_union.into(), out_col_change))
        }
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
