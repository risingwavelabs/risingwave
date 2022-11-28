use risingwave_common::util::sort_util::OrderType;

use crate::optimizer::plan_node::stream;
use crate::optimizer::plan_node::utils::TableCatalogBuilder;
use crate::utils::Condition;
use crate::TableCatalog;

#[derive(Clone, Debug)]
pub struct DynamicFilter<PlanRef> {
    /// The predicate (formed with exactly one of < , <=, >, >=)
    pub predicate: Condition,
    // dist_key_l: Distribution,
    pub left_index: usize,
    pub left: PlanRef,
    pub right: PlanRef,
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
        TableCatalogBuilder::new(me.ctx().inner().with_options.internal_table_subset());

    schema.fields().iter().for_each(|field| {
        internal_table_catalog_builder.add_column(field);
    });

    pk_indices.iter().for_each(|idx| {
        internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending)
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
        TableCatalogBuilder::new(input.ctx().inner().with_options.internal_table_subset());

    schema.fields().iter().for_each(|field| {
        internal_table_catalog_builder.add_column(field);
    });

    // No distribution keys
    internal_table_catalog_builder.build(vec![])
}
