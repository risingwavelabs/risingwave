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

use risingwave_common::util::sort_util::OrderType;
pub use risingwave_pb::expr::expr_node::Type as ExprType;

use super::GenericPlanRef;
use crate::expr::{ExprImpl, FunctionCall, InputRef};
use crate::optimizer::plan_node::stream;
use crate::optimizer::plan_node::utils::TableCatalogBuilder;
use crate::utils::Condition;
use crate::TableCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DynamicFilter<PlanRef> {
    /// The predicate (formed with exactly one of < , <=, >, >=)
    pub comparator: ExprType,
    pub left_index: usize,
    pub left: PlanRef,
    /// The right input can only have one column.
    pub right: PlanRef,
}

impl<PlanRef: GenericPlanRef> DynamicFilter<PlanRef> {
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
}

pub fn infer_left_internal_table_catalog(
    me: &impl stream::StreamPlanRef,
    left_key_index: usize,
) -> TableCatalog {
    let schema = me.schema();

    let dist_keys = me.distribution().dist_column_indices().to_vec();

    // The pk of dynamic filter internal table should be left_key + input_pk.
    let mut pk_indices = vec![left_key_index];
    // TODO(yuhao): dedup the dist key and pk.
    pk_indices.extend(me.logical_pk());

    let mut internal_table_catalog_builder =
        TableCatalogBuilder::new(me.ctx().with_options().internal_table_subset());

    schema.fields().iter().for_each(|field| {
        internal_table_catalog_builder.add_column(field);
    });

    pk_indices.iter().for_each(|idx| {
        internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending())
    });

    internal_table_catalog_builder.build(dist_keys)
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
    internal_table_catalog_builder.build(vec![])
}
