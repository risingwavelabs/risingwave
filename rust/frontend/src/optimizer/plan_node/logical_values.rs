use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, PlanRef, ToBatch, ToStream};
use crate::expr::{BoundExpr, BoundExprImpl};
use crate::optimizer::plan_node::IntoPlanRef;
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};
#[derive(Debug, Clone)]
pub struct LogicalValues {
    rows: Vec<Vec<BoundExprImpl>>,
    schema: Schema,
}

impl LogicalValues {
    /// Create a LogicalValues node. Used internally by optimizer.
    pub fn new(rows: Vec<Vec<BoundExprImpl>>, schema: Schema) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type())
            }
        }
        Self { rows, schema }
    }

    /// Create a LogicalValues node. Used by planner.
    pub fn create(rows: Vec<Vec<BoundExprImpl>>, schema: Schema) -> Result<Self> {
        // No additional checks after binder.
        Ok(Self::new(rows, schema))
    }

    /// Get a reference to the logical values's rows.
    pub fn rows(&self) -> &[Vec<BoundExprImpl>] {
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
impl ColPrunable for LogicalValues {}
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
