use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, LogicalBase, PlanRef, ToBatch, ToStream};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::property::WithSchema;
use crate::session::QueryContextRef;

/// `LogicalValues` builds rows according to a list of expressions
#[derive(Debug, Clone)]
pub struct LogicalValues {
    pub base: LogicalBase,
    rows: Vec<Vec<ExprImpl>>,
}

impl LogicalValues {
    /// Create a LogicalValues node. Used internally by optimizer.
    pub fn new(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: QueryContextRef) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type())
            }
        }
        let base = LogicalBase {
            schema,
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };
        Self { rows, base }
    }

    /// Create a LogicalValues node. Used by planner.
    pub fn create(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: QueryContextRef) -> Result<Self> {
        // No additional checks after binder.
        Ok(Self::new(rows, schema, ctx))
    }

    /// Get a reference to the logical values' rows.
    pub fn rows(&self) -> &[Vec<ExprImpl>] {
        self.rows.as_ref()
    }
}

impl_plan_tree_node_for_leaf! { LogicalValues }

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogicalValues")
            .field("rows", &self.rows)
            .field("schema", &self.schema())
            .finish()
    }
}

impl ColPrunable for LogicalValues {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let rows = self
            .rows
            .iter()
            .map(|row| required_cols.ones().map(|i| row[i].clone()).collect())
            .collect();
        let fields = required_cols
            .ones()
            .map(|i| self.schema().fields[i].clone())
            .collect();
        Self::new(rows, Schema { fields }, self.base.ctx.clone()).into()
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataType, Datum};

    use super::*;
    use crate::expr::Literal;
    use crate::session::QueryContext;

    fn literal(val: i32) -> ExprImpl {
        Literal::new(Datum::Some(val.into()), DataType::Int32).into()
    }

    /// Pruning
    /// ```text
    /// Values([[0, 1, 2], [3, 4, 5])
    /// ```
    /// with required columns [0, 2] will result in
    /// ```text
    /// Values([[0, 2], [3, 5])
    /// ```
    #[tokio::test]
    async fn test_prune_filter() {
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ]);
        // Values([[0, 1, 2], [3, 4, 5])
        let values = LogicalValues::new(
            vec![
                vec![literal(0), literal(1), literal(2)],
                vec![literal(3), literal(4), literal(5)],
            ],
            schema,
            ctx,
        );

        let required_cols = FixedBitSet::from_iter([0, 2].into_iter());
        let pruned = values.prune_col(&required_cols);

        let values = pruned.as_logical_values().unwrap();
        let rows: &[Vec<ExprImpl>] = values.rows();

        // expected output: Values([[0, 2], [3, 5])
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
        assert_eq!(rows[0][0], literal(0));
        assert_eq!(rows[0][1], literal(2));
        assert_eq!(rows[1].len(), 2);
        assert_eq!(rows[1][0], literal(3));
        assert_eq!(rows[1][1], literal(5));
    }
}
