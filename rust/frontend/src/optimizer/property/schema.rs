use fixedbitset::FixedBitSet;
use paste::paste;
use risingwave_common::catalog::Schema;

use super::super::plan_node::*;
use crate::for_logical_plan_nodes;

pub trait WithSchema {
    fn schema(&self) -> &Schema;

    fn must_contain_columns(&self, required_cols: &FixedBitSet) {
        assert!(
            required_cols.is_subset(&FixedBitSet::from_iter(0..self.schema().fields().len())),
            "Invalid required cols: {}, only {} columns available",
            required_cols,
            self.schema().fields().len()
        );
    }
}

/// Define module for each node.
macro_rules! impl_with_schema_for_logical_node {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithSchema for [<$convention $name>] {
                fn schema(&self) -> &Schema {
                    &self.base().schema
                }
            }
        })*
    }
}
for_logical_plan_nodes! {impl_with_schema_for_logical_node }
