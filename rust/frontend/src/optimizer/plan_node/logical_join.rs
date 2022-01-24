use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::types::DataTypeKind;
use risingwave_pb::plan::JoinType;

use super::{ColPrunable, IntoPlanRef, JoinPredicate, PlanRef, PlanTreeNodeBinary};
use crate::expr::{assert_input_ref, BoundExpr, BoundExprImpl};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalJoin {
    left: PlanRef,
    right: PlanRef,
    predicate: JoinPredicate,
    join_type: JoinType,
    schema: Schema,
}
impl fmt::Display for LogicalJoin {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl LogicalJoin {
    fn new(left: PlanRef, right: PlanRef, join_type: JoinType, predicate: JoinPredicate) -> Self {
        let schema = Self::derive_schema(left.schema(), right.schema(), join_type);

        let left_cols_num = left.schema().fields.len();
        let input_col_num = left_cols_num + right.schema().fields.len();
        for cond in predicate.other_conds() {
            assert_eq!(cond.return_type(), DataTypeKind::Boolean);
            assert_input_ref(cond, input_col_num);
        }
        for (k1, k2) in predicate.equal_keys() {
            assert!(k1 < left_cols_num);
            assert!(k2 >= left_cols_num);
            assert!(k2 < input_col_num);
        }

        LogicalJoin {
            left,
            right,
            schema,
            join_type,
            predicate,
        }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on_clause: BoundExprImpl,
    ) -> PlanRef {
        let left_cols_num = left.schema().fields.len();
        let predicate = JoinPredicate::create(left_cols_num, on_clause);
        Self::new(left, right, join_type, predicate).into_plan_ref()
    }
    fn derive_schema(_left: &Schema, _right: &Schema, _join_type: JoinType) -> Schema {
        todo!()
    }
    pub fn predicate(&self) -> &JoinPredicate {
        &self.predicate
    }
}
impl PlanTreeNodeBinary for LogicalJoin {
    fn left(&self) -> PlanRef {
        self.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(left, right, self.join_type, self.predicate.clone())
    }
}
impl_plan_tree_node_for_binary! {LogicalJoin}
impl WithOrder for LogicalJoin {}
impl WithDistribution for LogicalJoin {}
impl WithSchema for LogicalJoin {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
impl ColPrunable for LogicalJoin {}
