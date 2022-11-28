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
/// [`Filter`] iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone)]
pub struct Filter<PlanRef> {
    pub predicate: Condition,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Filter<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}
