use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{
    ColPrunable, CollectInputRef, LogicalProject, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream,
};
use crate::expr::{assert_input_ref, ExprImpl};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalFilter` iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone)]
pub struct LogicalFilter {
    predicate: Condition,
    input: PlanRef,
    schema: Schema,
}

impl LogicalFilter {
    pub fn new(input: PlanRef, predicate: Condition) -> Self {
        for cond in &predicate.conjunctions {
            assert_input_ref(cond, input.schema().fields().len());
        }
        let schema = input.schema().clone();
        LogicalFilter {
            input,
            schema,
            predicate,
        }
    }

    /// the function will check if the predicate is bool expression
    pub fn create(input: PlanRef, predicate: ExprImpl) -> Result<PlanRef> {
        let predicate = Condition::with_expr(predicate);
        Ok(Self::new(input, predicate).into())
    }

    /// Get the predicate of the logical join.
    pub fn predicate(&self) -> &Condition {
        &self.predicate
    }
}

impl PlanTreeNodeUnary for LogicalFilter {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.predicate.clone())
    }
}
impl_plan_tree_node_for_unary! {LogicalFilter}
impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogicalFilter")
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl WithOrder for LogicalFilter {}

impl WithDistribution for LogicalFilter {}

impl WithSchema for LogicalFilter {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ColPrunable for LogicalFilter {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        assert!(
            required_cols.is_subset(&FixedBitSet::from_iter(0..self.schema().fields().len())),
            "Invalid required cols: {}, only {} columns available",
            required_cols,
            self.schema().fields().len()
        );

        let mut visitor = CollectInputRef {
            input_bits: required_cols.clone(),
        };
        self.predicate.visit_expr(&mut visitor);

        let mut predicate = self.predicate.clone();
        let mut mapping = ColIndexMapping::with_remaining_columns(&visitor.input_bits);
        predicate = predicate.rewrite_expr(&mut mapping);

        let filter = LogicalFilter::new(self.input.prune_col(&visitor.input_bits), predicate);

        if required_cols == &visitor.input_bits {
            filter.into()
        } else {
            LogicalProject::with_mapping(
                filter.into(),
                ColIndexMapping::with_remaining_columns(
                    &required_cols.ones().map(|i| mapping.map(i)).collect(),
                ),
            )
            .into()
        }
    }
}

impl ToBatch for LogicalFilter {
    fn to_batch(&self) -> PlanRef {
        todo!()
    }
}

impl ToStream for LogicalFilter {
    fn to_stream(&self) -> PlanRef {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, TableId};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::LogicalScan;

    #[test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [2] will result in
    /// ```text
    /// Project(input_ref(1))
    ///   Filter(cond: input_ref(0)<5)
    ///     TableScan(v2, v3)
    /// ```
    fn test_prune_filter() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field {
                data_type: ty.clone(),
                name: "v1".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v2".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v3".to_string(),
            },
        ];
        let table_scan = LogicalScan::new(
            "test".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields.clone(),
            },
        );
        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::Literal(Box::new(Literal::new(None, ty))),
                ],
            )
            .unwrap(),
        ));
        let filter = LogicalFilter::new(table_scan.into(), Condition::with_expr(predicate));

        // Perform the prune
        let mut required_cols = FixedBitSet::with_capacity(3);
        required_cols.insert(2);
        let plan = filter.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 1);
        assert_eq_input_ref!(&project.exprs()[0], 1);

        let filter = project.input();
        let filter = filter.as_logical_filter().unwrap();
        assert_eq!(filter.schema().fields().len(), 2);
        assert_eq!(filter.schema().fields()[0], fields[1]);
        assert_eq!(filter.schema().fields()[1], fields[2]);
        match filter.predicate.clone().to_expr() {
            ExprImpl::FunctionCall(call) => assert_eq_input_ref!(&call.inputs()[0], 0),
            _ => panic!("Expected function call"),
        }

        let scan = filter.input();
        let scan = scan.as_logical_scan().unwrap();
        assert_eq!(scan.schema().fields().len(), 2);
        assert_eq!(scan.schema().fields()[0], fields[1]);
        assert_eq!(scan.schema().fields()[1], fields[2]);
    }

    #[test]
    /// Pruning
    /// ```text
    /// Filter(cond: input_ref(1)<5)
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1, 2] will result in
    /// ```text
    ///   Filter(cond: input_ref(0)<5)
    ///     TableScan(v2, v3)
    /// ```
    fn test_prune_filter_no_project() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field {
                data_type: ty.clone(),
                name: "v1".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v2".to_string(),
            },
            Field {
                data_type: ty.clone(),
                name: "v3".to_string(),
            },
        ];
        let table_scan = LogicalScan::new(
            "test".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields.clone(),
            },
        );
        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::Literal(Box::new(Literal::new(None, ty))),
                ],
            )
            .unwrap(),
        ));
        let filter = LogicalFilter::new(table_scan.into(), Condition::with_expr(predicate));

        // Perform the prune
        let mut required_cols = FixedBitSet::with_capacity(3);
        required_cols.insert(1);
        required_cols.insert(2);
        let plan = filter.prune_col(&required_cols);

        // Check the result
        let filter = plan.as_logical_filter().unwrap();
        assert_eq!(filter.schema().fields().len(), 2);
        assert_eq!(filter.schema().fields()[0], fields[1]);
        assert_eq!(filter.schema().fields()[1], fields[2]);
        match filter.predicate.clone().to_expr() {
            ExprImpl::FunctionCall(call) => assert_eq_input_ref!(&call.inputs()[0], 0),
            _ => panic!("Expected function call"),
        }

        let scan = filter.input();
        let scan = scan.as_logical_scan().unwrap();
        assert_eq!(scan.schema().fields().len(), 2);
        assert_eq!(scan.schema().fields()[0], fields[1]);
        assert_eq!(scan.schema().fields()[1], fields[2]);
    }
}
