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
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::{
    ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::risingwave_common::error::Result;
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone)]
pub struct LogicalExpand {
    pub base: PlanBase,
    expanded_keys: Vec<Vec<usize>>,
    input: PlanRef,
}

impl LogicalExpand {
    pub fn new(input: PlanRef, expanded_keys: Vec<Vec<usize>>) -> Self {
        let input_schema_len = input.schema().len();
        for key in expanded_keys.iter().flatten() {
            assert!(*key < input_schema_len);
        }
        // The last column should be the flag.
        // TODO: rethink here.
        let mut pk_indices = input.pk_indices().to_vec();
        pk_indices.push(input_schema_len);

        let schema = Self::derive_schema(input.schema());
        let ctx = input.ctx();
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalExpand {
            base,
            expanded_keys,
            input,
        }
    }

    pub fn create(input: PlanRef, expanded_keys: Vec<Vec<usize>>) -> PlanRef {
        Self::new(input, expanded_keys).into()
    }

    fn derive_schema(input_schema: &Schema) -> Schema {
        let mut fields = input_schema.clone().into_fields();
        fields.push(Field::with_name(DataType::Int64, "flag"));
        Schema::new(fields)
    }
}

impl PlanTreeNodeUnary for LogicalExpand {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.expanded_keys.clone())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let mut expanded_keys = self.expanded_keys.clone();
        for key in expanded_keys.iter_mut().flat_map(|r| r.iter_mut()) {
            *key = input_col_change.map(*key);
        }
        // TODO: rethink here.
        let (mut map, new_input_col_num) = input_col_change.into_parts();
        assert_eq!(new_input_col_num, input.schema().len());
        map.push(Some(new_input_col_num));

        (Self::new(input, expanded_keys), ColIndexMapping::new(map))
    }
}

impl_plan_tree_node_for_unary! {LogicalExpand}

impl fmt::Display for LogicalExpand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: rewrite here.
        write!(
            f,
            "LogicalExpand {{ expanded_keys: {:?} }}",
            self.expanded_keys
        )
    }
}

impl ColPrunable for LogicalExpand {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        // TODO: rethink `prune_col`.

        let pos_of_flag = self.input.schema().len();
        // TODO: rewrite here.
        let input_required_cols = required_cols
            .iter()
            .filter_map(|i| if *i == pos_of_flag { None } else { Some(*i) })
            .collect_vec();
        let new_input = self.input.prune_col(&input_required_cols);

        // `input_required_cols` should be a subset of `expanded_keys`
        let input_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input.schema().len(),
        );
        let expanded_keys = self
            .expanded_keys
            .iter()
            .filter_map(|keys| {
                let keys = keys
                    .iter()
                    .filter_map(|key| input_change.try_map(*key))
                    .collect_vec();
                if keys.is_empty() {
                    None
                } else {
                    Some(keys)
                }
            })
            .collect_vec();
        LogicalExpand::create(new_input, expanded_keys)
    }
}

impl PredicatePushdown for LogicalExpand {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        // TODO: rethink here.
        let new_input = self.input.predicate_pushdown(predicate);
        self.clone_with_input(new_input).into()
    }
}

impl ToBatch for LogicalExpand {
    fn to_batch(&self) -> Result<PlanRef> {
        todo!()
    }
}

impl ToStream for LogicalExpand {
    fn to_stream(&self) -> Result<PlanRef> {
        todo!()
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        todo!()
    }
}
