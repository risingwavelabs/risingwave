use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Field, FieldDisplay, Schema, TableDesc};
use risingwave_common::types::{DataType, IntervalUnit};
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_pb::expr::agg_call::OrderByField as ProstAggOrderByField;
use risingwave_pb::expr::AggCall as ProstAggCall;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::{agg_call_state, AggCallState as AggCallStateProst};

use super::super::utils::{IndicesDisplay, TableCatalogBuilder};
use super::{stream, EqJoinPredicate, GenericPlanNode, GenericPlanRef};
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::{ColumnId, IndexCatalog};
use crate::expr::{Expr, ExprDisplay, ExprImpl, InputRef, InputRefDisplay};
use crate::optimizer::property::{Direction, Order};
use crate::session::OptimizerContextRef;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};
use crate::TableCatalog;

/// [`ProjectSet`] projects one row multiple times according to `select_list`.
///
/// Different from `Project`, it supports [`TableFunction`](crate::expr::TableFunction)s.
/// See also [`ProjectSetSelectItem`](risingwave_pb::expr::ProjectSetSelectItem) for examples.
///
/// To have a pk, it has a hidden column `projected_row_id` at the beginning. The implementation of
/// `LogicalProjectSet` is highly similar to [`LogicalProject`], except for the additional hidden
/// column.
#[derive(Debug, Clone)]
pub struct ProjectSet<PlanRef> {
    pub select_list: Vec<ExprImpl>,
    pub input: PlanRef,
}
impl<PlanRef: GenericPlanRef> GenericPlanNode for ProjectSet<PlanRef> {
    fn schema(&self) -> Schema {
        let input_schema = self.input.schema();
        let o2i = self.o2i_col_mapping();
        let mut fields = vec![Field::with_name(DataType::Int64, "projected_row_id")];
        fields.extend(self.select_list.iter().enumerate().map(|(idx, expr)| {
            let idx = idx + 1;
            // Get field info from o2i.
            let (name, sub_fields, type_name) = match o2i.try_map(idx) {
                Some(input_idx) => {
                    let field = input_schema.fields()[input_idx].clone();
                    (field.name, field.sub_fields, field.type_name)
                }
                None => (
                    format!("{:?}", ExprDisplay { expr, input_schema }),
                    vec![],
                    String::new(),
                ),
            };
            Field::with_struct(expr.return_type(), name, sub_fields, type_name)
        }));

        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let i2o = self.i2o_col_mapping();
        let mut pk = self
            .input
            .logical_pk()
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        // add `projected_row_id` to pk
        pk.push(0);
        Some(pk)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> ProjectSet<PlanRef> {
    /// Gets the Mapping of columnIndex from output column index to input column index
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let mut map = vec![None; 1 + self.select_list.len()];
        for (i, item) in self.select_list.iter().enumerate() {
            map[1 + i] = match item {
                ExprImpl::InputRef(input) => Some(input.index()),
                _ => None,
            }
        }
        ColIndexMapping::with_target_size(map, input_len)
    }

    /// Gets the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.o2i_col_mapping().inverse()
    }
}
