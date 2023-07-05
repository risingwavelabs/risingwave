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

use fixedbitset::FixedBitSet;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::Schema;
use risingwave_common::util::sort_util::OrderType;
pub use risingwave_pb::expr::expr_node::Type as ExprType;

use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::{ExprImpl, FunctionCall, InputRef};
use crate::optimizer::plan_node::stream;
use crate::optimizer::plan_node::utils::TableCatalogBuilder;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{Condition, ConditionDisplay};
use crate::{OptimizerContextRef, TableCatalog};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DynamicFilter<PlanRef> {
    /// The predicate (formed with exactly one of < , <=, >, >=)
    comparator: ExprType,
    left_index: usize,
    pub left: PlanRef,
    /// The right input can only have one column.
    pub right: PlanRef,
}
impl<PlanRef> DynamicFilter<PlanRef> {
    pub fn comparator(&self) -> ExprType {
        self.comparator
    }

    pub fn left_index(&self) -> usize {
        self.left_index
    }

    pub fn left(&self) -> &PlanRef {
        &self.left
    }

    pub fn right(&self) -> &PlanRef {
        &self.right
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for DynamicFilter<PlanRef> {
    fn schema(&self) -> Schema {
        self.left.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.left.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.left.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.left.functional_dependency().clone()
    }
}

impl<PlanRef: GenericPlanRef> DynamicFilter<PlanRef> {
    pub fn new(comparator: ExprType, left_index: usize, left: PlanRef, right: PlanRef) -> Self {
        assert_eq!(right.schema().len(), 1);
        Self {
            comparator,
            left_index,
            left,
            right,
        }
    }

    pub fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(self.comparator, self.left_index, left, right)
    }

    /// normalize to the join predicate
    pub fn predicate(&self) -> Condition {
        Condition {
            conjunctions: vec![ExprImpl::from(
                FunctionCall::new(
                    self.comparator,
                    vec![
                        ExprImpl::from(InputRef::new(
                            self.left_index,
                            self.left.schema().fields()[self.left_index].data_type(),
                        )),
                        ExprImpl::from(InputRef::new(
                            self.left.schema().len(),
                            self.right.schema().fields()[0].data_type(),
                        )),
                    ],
                )
                .unwrap(),
            )],
        }
    }

    pub fn watermark_columns(&self, right_watermark: bool) -> FixedBitSet {
        let mut watermark_columns = FixedBitSet::with_capacity(self.left.schema().len());
        if right_watermark {
            match self.comparator {
                ExprType::Equal | ExprType::GreaterThan | ExprType::GreaterThanOrEqual => {
                    watermark_columns.set(self.left_index, true)
                }
                _ => {}
            }
        }
        watermark_columns
    }

    fn condition_display(&self) -> (Condition, Schema) {
        let mut concat_schema = self.left.schema().fields.clone();
        concat_schema.extend(self.right.schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);

        let predicate = self.predicate();
        (predicate, concat_schema)
    }

    pub fn fmt_fields_with_builder(&self, builder: &mut fmt::DebugStruct<'_, '_>) {
        let (condition, input_schema) = &self.condition_display();
        builder.field(
            "predicate",
            &ConditionDisplay {
                condition,
                input_schema,
            },
        );
    }

    pub fn pretty_field<'a>(&self) -> Pretty<'a> {
        let (condition, input_schema) = &self.condition_display();
        Pretty::debug(&ConditionDisplay {
            condition,
            input_schema,
        })
    }
}

pub fn infer_left_internal_table_catalog(
    me: &impl stream::StreamPlanRef,
    left_key_index: usize,
) -> TableCatalog {
    let schema = me.schema();

    let dist_keys = me.distribution().dist_column_indices().to_vec();

    // The pk of dynamic filter internal table should be left_key + input_pk.
    let mut pk_indices = vec![left_key_index];
    let read_prefix_len_hint = pk_indices.len();

    for i in me.logical_pk() {
        if *i != left_key_index {
            pk_indices.push(*i);
        }
    }

    let mut internal_table_catalog_builder =
        TableCatalogBuilder::new(me.ctx().with_options().internal_table_subset());

    schema.fields().iter().for_each(|field| {
        internal_table_catalog_builder.add_column(field);
    });

    pk_indices.iter().for_each(|idx| {
        internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending())
    });

    internal_table_catalog_builder.build(dist_keys, read_prefix_len_hint)
}

pub fn infer_right_internal_table_catalog(input: &impl stream::StreamPlanRef) -> TableCatalog {
    let schema = input.schema();

    // We require that the right table has distribution `Single`
    assert_eq!(
        input.distribution().dist_column_indices().to_vec(),
        Vec::<usize>::new()
    );

    let mut internal_table_catalog_builder =
        TableCatalogBuilder::new(input.ctx().with_options().internal_table_subset());

    schema.fields().iter().for_each(|field| {
        internal_table_catalog_builder.add_column(field);
    });

    // No distribution keys
    internal_table_catalog_builder.build(vec![], 0)
}
