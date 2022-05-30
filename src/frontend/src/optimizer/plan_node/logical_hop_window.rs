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

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::Field;
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, IntervalUnit};

use super::{
    gen_filter_and_pushdown, BatchHopWindow, ColPrunable, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamHopWindow, ToBatch, ToStream,
};
use crate::expr::{InputRef, InputRefDisplay};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalHopWindow` implements Hop Table Function.
#[derive(Debug, Clone)]
pub struct LogicalHopWindow {
    pub base: PlanBase,
    input: PlanRef,
    pub(super) time_col: InputRef,
    pub(super) window_slide: IntervalUnit,
    pub(super) window_size: IntervalUnit,
}

impl LogicalHopWindow {
    fn new(
        input: PlanRef,
        time_col: InputRef,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
    ) -> Self {
        let ctx = input.ctx();
        let schema = input
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(DataType::Timestamp, "window_start"),
                Field::with_name(DataType::Timestamp, "window_end"),
            ])
            .collect();
        let mut pk_indices = input.pk_indices().to_vec();
        pk_indices.push(input.schema().len());
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalHopWindow {
            base,
            input,
            time_col,
            window_slide,
            window_size,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(
        input: PlanRef,
        time_col: InputRef,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
    ) -> PlanRef {
        Self::new(input, time_col, window_slide, window_size).into()
    }

    pub fn window_start_col_idx(&self) -> usize {
        self.schema().len() - 2
    }

    pub fn window_end_col_idx(&self) -> usize {
        self.schema().len() - 1
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::identity_or_none(self.schema().len(), self.input.schema().len())
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::identity_or_none(self.input.schema().len(), self.schema().len())
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ time_col: {} slide: {} size: {} }}",
            name,
            InputRefDisplay(self.time_col.index),
            self.window_slide,
            self.window_size
        )
    }
}

impl PlanTreeNodeUnary for LogicalHopWindow {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.time_col.clone(),
            self.window_slide,
            self.window_size,
        )
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let mut time_col = self.time_col.clone();
        time_col.index = input_col_change.map(time_col.index);
        let new_hop = Self::new(input.clone(), time_col, self.window_slide, self.window_size);

        let (mut mapping, new_input_col_num) = input_col_change.into_parts();
        assert_eq!(new_input_col_num, input.schema().len());
        assert_eq!(new_input_col_num + 2, new_hop.schema().len());
        mapping.push(Some(new_input_col_num));
        mapping.push(Some(new_input_col_num + 1));

        (new_hop, ColIndexMapping::new(mapping))
    }
}

impl_plan_tree_node_for_unary! {LogicalHopWindow}

impl fmt::Display for LogicalHopWindow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_name(f, "LogicalHopWindow")
    }
}

impl ColPrunable for LogicalHopWindow {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let o2i = self.o2i_col_mapping();
        let input_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(self.schema().len());
            tmp.extend(required_cols.iter().copied());
            tmp = o2i.rewrite_bitset(&tmp);
            // LogicalHopWindow should keep all required cols from upstream,
            // as well as its own time_col.
            tmp.put(self.time_col.index());
            tmp.ones().collect_vec()
        };
        let input = self.input.prune_col(&input_required_cols);
        let input_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let (new_hop, _) = self.rewrite_with_input(input, input_change.clone());
        let output_cols = {
            // output cols = { cols required by upstream from input node } ∪ { additional window
            // cols }
            let win_start_index = new_hop.schema().len() - 2;
            let win_end_index = new_hop.schema().len() - 1;
            #[derive(Copy, Clone, Debug)]
            enum IndexType {
                Input(usize),
                WindowStart,
                WindowEnd,
            }
            // map the indices from output to input
            let input_required_cols = required_cols
                .iter()
                .map(|&idx| {
                    if let Some(idx) = o2i.try_map(idx) {
                        IndexType::Input(idx)
                    } else if idx == self.window_start_col_idx() {
                        IndexType::WindowStart
                    } else {
                        IndexType::WindowEnd
                    }
                })
                .collect_vec();
            // this mapping will only keeps required columns
            let mapping = input_change.composite(&new_hop.i2o_col_mapping());
            input_required_cols
                .iter()
                .filter_map(|&idx| match idx {
                    IndexType::Input(x) => mapping.try_map(x),
                    IndexType::WindowStart => Some(win_start_index),
                    IndexType::WindowEnd => Some(win_end_index),
                })
                .collect_vec()
        };
        LogicalProject::with_mapping(
            new_hop.into(),
            ColIndexMapping::with_remaining_columns(&output_cols, self.schema().len()),
        )
        .into()
    }
}

impl PredicatePushdown for LogicalHopWindow {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond())
    }
}

impl ToBatch for LogicalHopWindow {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchHopWindow::new(new_logical).into())
    }
}

impl ToStream for LogicalHopWindow {
    fn to_stream(&self) -> Result<PlanRef> {
        let new_input = self.input().to_stream()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(StreamHopWindow::new(new_logical).into())
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream()?;
        let (hop, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((hop.into(), out_col_change))
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::{assert_eq_input_ref, ExprImpl, InputRef};
    use crate::optimizer::plan_node::LogicalValues;
    use crate::session::OptimizerContext;
    #[tokio::test]
    /// Pruning
    /// ```text
    /// HopWindow(time_col: $0 slide: 1 day 00:00:00 size: 3 days 00:00:00)
    ///   TableScan(date, v1, v2)
    /// ```
    /// with required columns [4, 2, 3] will result in
    /// ```text
    /// Project($3, $1, $2)
    ///   HopWindow(time_col: $0 slide: 1 day 00:00:00 size: 3 days 00:00:00)
    ///     TableScan(date, v3)
    /// ```
    async fn test_prune_hop_window_with_order_required() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Date, "date"),
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let hop_window: PlanRef = LogicalHopWindow::new(
            values.into(),
            InputRef::new(0, DataType::Date),
            IntervalUnit::new(0, 1, 0),
            IntervalUnit::new(0, 3, 0),
        )
        .into();
        // Perform the prune
        let required_cols = vec![4, 2, 3];
        let plan = hop_window.prune_col(&required_cols);
        println!(
            "{}\n{}",
            hop_window.explain_to_string().unwrap(),
            plan.explain_to_string().unwrap()
        );
        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 3);
        assert_eq_input_ref!(&project.exprs()[0], 3);
        assert_eq_input_ref!(&project.exprs()[1], 1);
        assert_eq_input_ref!(&project.exprs()[2], 2);

        let hop_window = project.input();
        let hop_window = hop_window.as_logical_hop_window().unwrap();
        assert_eq!(hop_window.schema().fields().len(), 4);

        let values = hop_window.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[0]);
        assert_eq!(values.schema().fields()[1], fields[2]);
    }
}
