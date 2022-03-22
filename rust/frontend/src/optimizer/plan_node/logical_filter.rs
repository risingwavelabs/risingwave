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
//
use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::error::Result;

use super::{
    ColPrunable, CollectInputRef, LogicalProject, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatch,
    ToStream,
};
use crate::expr::{assert_input_ref, ExprImpl};
use crate::optimizer::plan_node::{BatchFilter, StreamFilter};
use crate::optimizer::property::WithSchema;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalFilter` iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub base: PlanBase,
    predicate: Condition,
    input: PlanRef,
}

impl LogicalFilter {
    pub fn new(input: PlanRef, predicate: Condition) -> Self {
        let ctx = input.ctx();
        for cond in &predicate.conjunctions {
            assert_input_ref(cond, input.schema().fields().len());
        }
        let schema = input.schema().clone();
        let pk_indices = input.pk_indices().to_vec();
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalFilter {
            input,
            base,
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
        write!(f, "LogicalFilter {{ predicate: {} }}", self.predicate)
    }
}

impl ColPrunable for LogicalFilter {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

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
            let mut remaining_columns = FixedBitSet::with_capacity(filter.schema().fields().len());
            remaining_columns.extend(required_cols.ones().map(|i| mapping.map(i)));
            LogicalProject::with_mapping(
                filter.into(),
                ColIndexMapping::with_remaining_columns(&remaining_columns),
            )
            .into()
        }
    }
}

impl ToBatch for LogicalFilter {
    fn to_batch(&self) -> PlanRef {
        let new_input = self.input().to_batch();
        let new_logical = self.clone_with_input(new_input);
        BatchFilter::new(new_logical).into()
    }
}

impl ToStream for LogicalFilter {
    fn to_stream(&self) -> PlanRef {
        let new_input = self.input().to_stream();
        let new_logical = self.clone_with_input(new_input);
        StreamFilter::new(new_logical).into()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::LogicalScan;
    use crate::optimizer::property::ctx::WithId;
    use crate::session::QueryContext;

    #[tokio::test]
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
    async fn test_prune_filter() {
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ];
        let table_scan = LogicalScan::new(
            "test".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let predicate: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::LessThan,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(None, DataType::Int32))),
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
        assert_eq!(filter.id().0, 3);

        match filter.predicate.clone().to_expr() {
            ExprImpl::FunctionCall(call) => assert_eq_input_ref!(&call.inputs()[0], 0),
            _ => panic!("Expected function call"),
        }

        let scan = filter.input();
        let scan = scan.as_logical_scan().unwrap();
        assert_eq!(scan.schema().fields().len(), 2);
        assert_eq!(scan.schema().fields()[0], fields[1]);
        assert_eq!(scan.schema().fields()[1], fields[2]);
        assert_eq!(scan.id().0, 2);
    }

    #[tokio::test]
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
    async fn test_prune_filter_no_project() {
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
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
            ctx,
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
