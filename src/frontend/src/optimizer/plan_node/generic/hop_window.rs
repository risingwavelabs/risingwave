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
/// [`HopWindow`] implements Hop Table Function.
#[derive(Debug, Clone)]
pub struct HopWindow<PlanRef> {
    pub input: PlanRef,
    pub time_col: InputRef,
    pub window_slide: IntervalUnit,
    pub window_size: IntervalUnit,
    pub output_indices: Vec<usize>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for HopWindow<PlanRef> {
    fn schema(&self) -> Schema {
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        let original_schema: Schema = self
            .input
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(output_type.clone(), "window_start"),
                Field::with_name(output_type, "window_end"),
            ])
            .collect();
        self.output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let window_start_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == self.input.schema().len());
        let window_end_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == self.input.schema().len() + 1);
        if window_start_index.is_none() && window_end_index.is_none() {
            None
        } else {
            let mut pk = self
                .input
                .logical_pk()
                .iter()
                .filter_map(|&pk_idx| self.output_indices.iter().position(|&idx| idx == pk_idx))
                .collect_vec();
            if let Some(start_idx) = window_start_index {
                pk.push(start_idx);
            };
            if let Some(end_idx) = window_end_index {
                pk.push(end_idx);
            };
            Some(pk)
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> HopWindow<PlanRef> {
    pub fn into_parts(self) -> (PlanRef, InputRef, IntervalUnit, IntervalUnit, Vec<usize>) {
        (
            self.input,
            self.time_col,
            self.window_slide,
            self.window_size,
            self.output_indices,
        )
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        write!(
            f,
            "{} {{ time_col: {}, slide: {}, size: {}, output: {} }}",
            name,
            format_args!(
                "{}",
                InputRefDisplay {
                    input_ref: &self.time_col,
                    input_schema: self.input.schema()
                }
            ),
            self.window_slide,
            self.window_size,
            if self
                .output_indices
                .iter()
                .copied()
                // Behavior is the same as `LogicalHopWindow::internal_column_num`
                .eq(0..(self.input.schema().len() + 2))
            {
                "all".to_string()
            } else {
                let original_schema: Schema = self
                    .input
                    .schema()
                    .clone()
                    .into_fields()
                    .into_iter()
                    .chain([
                        Field::with_name(output_type.clone(), "window_start"),
                        Field::with_name(output_type, "window_end"),
                    ])
                    .collect();
                format!(
                    "{:?}",
                    &IndicesDisplay {
                        indices: &self.output_indices,
                        input_schema: &original_schema,
                    }
                )
            },
        )
    }
}
