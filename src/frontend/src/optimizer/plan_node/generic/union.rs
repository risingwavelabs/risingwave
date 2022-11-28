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
/// `Union` returns the union of the rows of its inputs.
/// If `all` is false, it needs to eliminate duplicates.
#[derive(Debug, Clone)]
pub struct Union<PlanRef> {
    pub all: bool,
    pub inputs: Vec<PlanRef>,
    /// It is used by streaming processing. We need to use `source_col` to identify the record came
    /// from which source input.
    /// We add it as a logical property, because we need to derive the logical pk based on it.
    pub source_col: Option<usize>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Union<PlanRef> {
    fn schema(&self) -> Schema {
        self.inputs[0].schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        // Union all its inputs pks + source_col if exists
        let mut pk_indices = vec![];
        for input in &self.inputs {
            for pk in input.logical_pk() {
                if !pk_indices.contains(pk) {
                    pk_indices.push(*pk);
                }
            }
        }
        if let Some(source_col) = self.source_col {
            pk_indices.push(source_col)
        }
        Some(pk_indices)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.inputs[0].ctx()
    }
}
