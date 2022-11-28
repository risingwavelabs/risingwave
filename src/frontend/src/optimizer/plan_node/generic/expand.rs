use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::rc::Rc;

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

/// [`Expand`] expand one row multiple times according to `column_subsets` and also keep
/// original columns of input. It can be used to implement distinct aggregation and group set.
///
/// This is the schema of `Expand`:
/// | expanded columns(i.e. some columns are set to null) | original columns of input | flag |.
///
/// Aggregates use expanded columns as their arguments and original columns for their filter. `flag`
/// is used to distinguish between different `subset`s in `column_subsets`.
#[derive(Debug, Clone)]
pub struct Expand<PlanRef> {
    // `column_subsets` has many `subset`s which specifies the columns that need to be
    // reserved and other columns will be filled with NULL.
    pub column_subsets: Vec<Vec<usize>>,
    pub input: PlanRef,
}
impl<PlanRef: GenericPlanRef> GenericPlanNode for Expand<PlanRef> {
    fn schema(&self) -> Schema {
        let mut fields = self.input.schema().clone().into_fields();
        fields.extend(fields.clone());
        fields.push(Field::with_name(DataType::Int64, "flag"));
        Schema::new(fields)
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let input_schema_len = self.input.schema().len();
        let mut pk_indices = self
            .input
            .logical_pk()
            .iter()
            .map(|&pk| pk + input_schema_len)
            .collect_vec();
        // The last column should be the flag.
        pk_indices.push(input_schema_len * 2);
        Some(pk_indices)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> Expand<PlanRef> {
    pub fn column_subsets_display(&self) -> Vec<Vec<FieldDisplay<'_>>> {
        self.column_subsets
            .iter()
            .map(|subset| {
                subset
                    .iter()
                    .map(|&i| FieldDisplay(self.input.schema().fields.get(i).unwrap()))
                    .collect_vec()
            })
            .collect_vec()
    }
}
