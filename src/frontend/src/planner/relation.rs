// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, IntervalUnit, ScalarImpl};

use crate::binder::{
    BoundBaseTable, BoundJoin, BoundShare, BoundSource, BoundSystemTable, BoundWatermark,
    BoundWindowTableFunction, Relation, WindowTableFunctionKind,
};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, TableFunction};
use crate::optimizer::plan_node::{
    LogicalHopWindow, LogicalJoin, LogicalProject, LogicalScan, LogicalShare, LogicalSource,
    LogicalTableFunction, PlanRef,
};
use crate::planner::Planner;

const ERROR_WINDOW_SIZE_ARG: &str =
    "The size arg of window table function should be an interval literal.";

impl Planner {
    pub fn plan_relation(&mut self, relation: Relation) -> Result<PlanRef> {
        match relation {
            Relation::BaseTable(t) => self.plan_base_table(*t),
            Relation::SystemTable(st) => self.plan_sys_table(*st),
            // TODO: order is ignored in the subquery
            Relation::Subquery(q) => Ok(self.plan_query(q.query)?.into_subplan()),
            Relation::Join(join) => self.plan_join(*join),
            Relation::WindowTableFunction(tf) => self.plan_window_table_function(*tf),
            Relation::Source(s) => self.plan_source(*s),
            Relation::TableFunction(tf) => self.plan_table_function(*tf),
            Relation::Watermark(tf) => self.plan_watermark(*tf),
            Relation::Share(share) => self.plan_share(*share),
        }
    }

    pub(crate) fn plan_sys_table(&mut self, sys_table: BoundSystemTable) -> Result<PlanRef> {
        Ok(LogicalScan::create(
            sys_table.sys_table_catalog.name().to_string(),
            true,
            Rc::new(sys_table.sys_table_catalog.table_desc()),
            vec![],
            self.ctx(),
        )
        .into())
    }

    pub(super) fn plan_base_table(&mut self, base_table: BoundBaseTable) -> Result<PlanRef> {
        Ok(LogicalScan::create(
            base_table.table_catalog.name().to_string(),
            false,
            Rc::new(base_table.table_catalog.table_desc()),
            base_table
                .table_indexes
                .iter()
                .map(|x| x.as_ref().clone().into())
                .collect(),
            self.ctx(),
        )
        .into())
    }

    pub(super) fn plan_source(&mut self, source: BoundSource) -> Result<PlanRef> {
        let column_descs = source
            .catalog
            .columns
            .iter()
            .map(|column| column.column_desc.clone())
            .collect_vec();
        let pk_col_ids = source.catalog.pk_col_ids.clone();
        let row_id_index = source.catalog.row_id_index;
        let gen_row_id = source.catalog.append_only;
        Ok(LogicalSource::new(
            Some(Rc::new(source.catalog)),
            column_descs,
            pk_col_ids,
            row_id_index,
            gen_row_id,
            false,
            self.ctx(),
        )
        .into())
    }

    pub(super) fn plan_join(&mut self, join: BoundJoin) -> Result<PlanRef> {
        let left = self.plan_relation(join.left)?;
        let right = self.plan_relation(join.right)?;
        let join_type = join.join_type;
        let on_clause = join.cond;
        if on_clause.has_subquery() {
            Err(ErrorCode::NotImplemented(
                "Subquery in join on condition is unsupported".into(),
                None.into(),
            )
            .into())
        } else {
            Ok(LogicalJoin::create(left, right, join_type, on_clause))
        }
    }

    pub(super) fn plan_window_table_function(
        &mut self,
        table_function: BoundWindowTableFunction,
    ) -> Result<PlanRef> {
        use WindowTableFunctionKind::*;
        match table_function.kind {
            Tumble => self.plan_tumble_window(
                table_function.input,
                table_function.time_col,
                table_function.args,
            ),
            Hop => self.plan_hop_window(
                table_function.input,
                table_function.time_col,
                table_function.args,
            ),
        }
    }

    pub(super) fn plan_table_function(&mut self, table_function: TableFunction) -> Result<PlanRef> {
        Ok(LogicalTableFunction::new(table_function, self.ctx()).into())
    }

    pub(super) fn plan_share(&mut self, share: BoundShare) -> Result<PlanRef> {
        match self.share_cache.get(&share.share_id) {
            None => {
                let result = self.plan_relation(share.input)?;
                let logical_share = LogicalShare::create(result);
                self.share_cache
                    .insert(share.share_id, logical_share.clone());
                Ok(logical_share)
            }
            Some(result) => Ok(result.clone()),
        }
    }

    pub(super) fn plan_watermark(&mut self, _watermark: BoundWatermark) -> Result<PlanRef> {
        todo!("plan watermark");
    }

    fn collect_col_data_types_for_tumble_window(relation: &Relation) -> Result<Vec<DataType>> {
        let col_data_types = match relation {
            Relation::Source(s) => s
                .catalog
                .columns
                .iter()
                .map(|col| col.data_type().clone())
                .collect(),
            Relation::BaseTable(t) => t
                .table_catalog
                .columns
                .iter()
                .map(|col| col.data_type().clone())
                .collect(),
            Relation::Subquery(q) => q
                .query
                .schema()
                .fields
                .iter()
                .map(|f| f.data_type())
                .collect(),
            Relation::Share(share) => Self::collect_col_data_types_for_tumble_window(&share.input)?,
            r => {
                return Err(ErrorCode::BindError(format!(
                    "Invalid input relation to tumble: {r:?}"
                ))
                .into())
            }
        };
        Ok(col_data_types)
    }

    fn plan_tumble_window(
        &mut self,
        input: Relation,
        time_col: InputRef,
        args: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        let mut args = args.into_iter();
        let col_data_types: Vec<_> = Self::collect_col_data_types_for_tumble_window(&input)?;

        match (args.next(), args.next()) {
            (Some(window_size @ ExprImpl::Literal(_)), None) => {
                let mut exprs = Vec::with_capacity(col_data_types.len() + 2);
                for (idx, col_dt) in col_data_types.iter().enumerate() {
                    exprs.push(InputRef::new(idx, col_dt.clone()).into());
                }
                let window_start: ExprImpl = FunctionCall::new(
                    ExprType::TumbleStart,
                    vec![ExprImpl::InputRef(Box::new(time_col)), window_size.clone()],
                )?
                .into();
                // TODO: `window_end` may be optimized to avoid double calculation of
                // `tumble_start`, or we can depends on common expression
                // optimization.
                let window_end =
                    FunctionCall::new(ExprType::Add, vec![window_start.clone(), window_size])?
                        .into();
                exprs.push(window_start);
                // timestamp - (timestamp - offset) % windowSize

                // 现在的
                // time_sub_offset = sub(time - offset)
                // window_offset  = window_offset(time_sub_offset , window_size)
                //  if time_sub_offset < 0:  window_offset % window_size + window_size
                //  else : window_offset % window_size
                // window_start = sub(time , window_offset)

                // tumble_offset (time , offset) =  abs(timestamp - offset)
                // tumble_start (time , window_offset)
                exprs.push(window_end);
                let base = self.plan_relation(input)?;
                let project = LogicalProject::create(base, exprs);
                Ok(project)
            }
            _ => Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_string()).into()),
        }
    }

    fn plan_hop_window(
        &mut self,
        input: Relation,
        time_col: InputRef,
        args: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        let input = self.plan_relation(input)?;
        let mut args = args.into_iter();
        let Some((ExprImpl::Literal(window_slide), ExprImpl::Literal(window_size))) = args.next_tuple() else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_string()).into());
        };

        let Some(ScalarImpl::Interval(window_slide)) = *window_slide.get_data() else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_string()).into());
        };
        let Some(ScalarImpl::Interval(window_size)) = *window_size.get_data() else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_string()).into());
        };

        let window_offset = match (args.next(), args.next()) {
            (Some(ExprImpl::Literal(window_offset)), None) => match *window_offset.get_data() {
                Some(ScalarImpl::Interval(window_offset)) => window_offset,
                _ => return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_string()).into()),
            },
            (None, None) => IntervalUnit::new(0, 0, 0),
            _ => return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_string()).into()),
        };

        if !window_size.is_positive() || !window_slide.is_positive() {
            return Err(ErrorCode::BindError(format!(
                "window_size {} and window_slide {} must be positive",
                window_size, window_slide
            ))
            .into());
        }

        if window_size.exact_div(&window_slide).is_none() {
            return Err(ErrorCode::BindError(format!("Invalid arguments for HOP window function: window_size {} cannot be divided by window_slide {}",window_size, window_slide)).into());
        }

        Ok(LogicalHopWindow::create(
            input,
            time_col,
            window_slide,
            window_size,
            window_offset,
        ))
    }
}
