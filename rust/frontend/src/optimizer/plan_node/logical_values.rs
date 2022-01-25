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
    pub fn new(rows: Vec<Vec<BoundExprImpl>>, schema: Schema) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type())
            }
        }
        Self { rows, schema }
    }

    /// this function will check each expressions satisfy the schema
    pub fn create(_rows: Vec<Vec<BoundExprImpl>>, _schema: Schema) -> Result<Self> {
        todo!()
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
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
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
