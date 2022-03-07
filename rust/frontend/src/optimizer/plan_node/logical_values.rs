use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, PlanRef, ToBatch, ToStream};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalValues {
    rows: Vec<Vec<ExprImpl>>,
    schema: Schema,
}

impl LogicalValues {
    /// Create a LogicalValues node. Used internally by optimizer.
    pub fn new(rows: Vec<Vec<ExprImpl>>, schema: Schema) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type())
            }
        }
        Self { rows, schema }
    }

    /// Create a LogicalValues node. Used by planner.
    pub fn create(rows: Vec<Vec<ExprImpl>>, schema: Schema) -> Result<Self> {
        // No additional checks after binder.
        Ok(Self::new(rows, schema))
    }

    /// Get a reference to the logical values' rows.
    pub fn rows(&self) -> &[Vec<ExprImpl>] {
        self.rows.as_ref()
    }
}

impl WithSchema for LogicalValues {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl_plan_tree_node_for_leaf! {LogicalValues}

impl WithOrder for LogicalValues {}

impl WithDistribution for LogicalValues {}

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ColPrunable for LogicalValues {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        assert!(
            required_cols.is_subset(&FixedBitSet::from_iter(0..self.schema().fields().len())),
            "Invalid required cols: {}, only {} columns available",
            required_cols,
            self.schema().fields().len()
        );

        let (rows, fields) = required_cols
            .ones()
            .map(|id| (self.rows[id].clone(), self.schema.fields[id].clone()))
            .unzip();
        Self {
            rows,
            schema: Schema { fields },
        }
        .into()
    }
}

impl ToBatch for LogicalValues {
    fn to_batch(&self) -> PlanRef {
        todo!()
    }
}

impl ToStream for LogicalValues {
    fn to_stream(&self) -> PlanRef {
        unimplemented!("Stream values executor is unimplemented!")
    }
}
