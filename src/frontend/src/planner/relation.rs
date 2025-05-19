// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;

use either::Either;
use itertools::Itertools;
use risingwave_common::catalog::{
    ColumnCatalog, Engine, Field, RISINGWAVE_ICEBERG_ROW_ID, ROW_ID_COLUMN_NAME, Schema,
};
use risingwave_common::types::{DataType, Interval, ScalarImpl};
use risingwave_common::{bail, bail_not_implemented};
use risingwave_sqlparser::ast::AsOf;

use crate::binder::{
    BoundBackCteRef, BoundBaseTable, BoundJoin, BoundShare, BoundShareInput, BoundSource,
    BoundSystemTable, BoundWatermark, BoundWindowTableFunction, Relation, WindowTableFunctionKind,
};
use crate::error::{ErrorCode, Result};
use crate::expr::{CastContext, Expr, ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::generic::SourceNodeKind;
use crate::optimizer::plan_node::{
    LogicalApply, LogicalCteRef, LogicalHopWindow, LogicalJoin, LogicalProject, LogicalScan,
    LogicalShare, LogicalSource, LogicalSysScan, LogicalTableFunction, LogicalValues, PlanRef,
};
use crate::optimizer::property::Cardinality;
use crate::planner::{PlanFor, Planner};
use crate::utils::Condition;

const ERROR_WINDOW_SIZE_ARG: &str =
    "The size arg of window table function should be an interval literal.";

impl Planner {
    pub fn plan_relation(&mut self, relation: Relation) -> Result<PlanRef> {
        match relation {
            Relation::BaseTable(t) => self.plan_base_table(&t),
            Relation::SystemTable(st) => self.plan_sys_table(*st),
            // TODO: order is ignored in the subquery
            Relation::Subquery(q) => Ok(self.plan_query(q.query)?.into_unordered_subplan()),
            Relation::Join(join) => self.plan_join(*join),
            Relation::Apply(join) => self.plan_apply(*join),
            Relation::WindowTableFunction(tf) => self.plan_window_table_function(*tf),
            Relation::Source(s) => self.plan_source(*s),
            Relation::TableFunction {
                expr: tf,
                with_ordinality,
            } => self.plan_table_function(tf, with_ordinality),
            Relation::Watermark(tf) => self.plan_watermark(*tf),
            // note that rcte (i.e., RecursiveUnion) is included *implicitly* in share.
            Relation::Share(share) => self.plan_share(*share),
            Relation::BackCteRef(cte_ref) => self.plan_cte_ref(*cte_ref),
        }
    }

    pub(crate) fn plan_sys_table(&mut self, sys_table: BoundSystemTable) -> Result<PlanRef> {
        Ok(LogicalSysScan::create(
            sys_table.sys_table_catalog.name().to_owned(),
            Rc::new(sys_table.sys_table_catalog.table_desc()),
            self.ctx(),
            Cardinality::unknown(), // TODO(card): cardinality of system table
        )
        .into())
    }

    pub(super) fn plan_base_table(&mut self, base_table: &BoundBaseTable) -> Result<PlanRef> {
        let as_of = base_table.as_of.clone();
        let table_cardinality = base_table.table_catalog.cardinality;
        let scan = LogicalScan::create(
            base_table.table_catalog.name().to_owned(),
            base_table.table_catalog.clone(),
            base_table
                .table_indexes
                .iter()
                .map(|x| x.as_ref().clone().into())
                .collect(),
            self.ctx(),
            as_of.clone(),
            table_cardinality,
        );

        match (base_table.table_catalog.engine, self.plan_for()) {
            (Engine::Hummock, PlanFor::Stream)
            | (Engine::Hummock, PlanFor::Batch)
            | (Engine::Hummock, PlanFor::BatchDql) => {
                match as_of {
                    None
                    | Some(AsOf::ProcessTime)
                    | Some(AsOf::TimestampNum(_))
                    | Some(AsOf::TimestampString(_))
                    | Some(AsOf::ProcessTimeWithInterval(_)) => {}
                    Some(AsOf::VersionNum(_)) | Some(AsOf::VersionString(_)) => {
                        bail_not_implemented!("As Of Version is not supported yet.")
                    }
                };
                Ok(scan.into())
            }
            (Engine::Iceberg, PlanFor::Stream) | (Engine::Iceberg, PlanFor::Batch) => {
                match as_of {
                    None
                    | Some(AsOf::VersionNum(_))
                    | Some(AsOf::TimestampString(_))
                    | Some(AsOf::TimestampNum(_)) => {}
                    Some(AsOf::ProcessTime) | Some(AsOf::ProcessTimeWithInterval(_)) => {
                        bail_not_implemented!("As Of ProcessTime() is not supported yet.")
                    }
                    Some(AsOf::VersionString(_)) => {
                        bail_not_implemented!("As Of Version is not supported yet.")
                    }
                }
                Ok(scan.into())
            }
            (Engine::Iceberg, PlanFor::BatchDql) => {
                match as_of {
                    None
                    | Some(AsOf::VersionNum(_))
                    | Some(AsOf::TimestampString(_))
                    | Some(AsOf::TimestampNum(_)) => {}
                    Some(AsOf::ProcessTime) | Some(AsOf::ProcessTimeWithInterval(_)) => {
                        bail_not_implemented!("As Of ProcessTime() is not supported yet.")
                    }
                    Some(AsOf::VersionString(_)) => {
                        bail_not_implemented!("As Of Version is not supported yet.")
                    }
                }
                let opt_ctx = self.ctx();
                let session = opt_ctx.session_ctx();
                let db_name = &session.database();
                let catalog_reader = session.env().catalog_reader().read_guard();
                let mut source_catalog = None;
                for schema in catalog_reader.iter_schemas(db_name).unwrap() {
                    if schema
                        .get_table_by_id(&base_table.table_catalog.id)
                        .is_some()
                    {
                        source_catalog = schema.get_source_by_name(
                            &base_table.table_catalog.iceberg_source_name().unwrap(),
                        );
                        break;
                    }
                }
                if let Some(source_catalog) = source_catalog {
                    let column_map: HashMap<String, (usize, ColumnCatalog)> = source_catalog
                        .columns
                        .clone()
                        .into_iter()
                        .enumerate()
                        .map(|(i, column)| (column.name().to_owned(), (i, column)))
                        .collect();
                    let exprs = scan
                        .table_catalog()
                        .column_schema()
                        .fields()
                        .iter()
                        .map(|field| {
                            let source_filed_name = if field.name == ROW_ID_COLUMN_NAME {
                                RISINGWAVE_ICEBERG_ROW_ID
                            } else {
                                &field.name
                            };
                            if let Some((i, source_column)) = column_map.get(source_filed_name) {
                                if source_column.column_desc.data_type == field.data_type {
                                    ExprImpl::InputRef(
                                        InputRef::new(*i, field.data_type.clone()).into(),
                                    )
                                } else {
                                    let mut input_ref = ExprImpl::InputRef(
                                        InputRef::new(
                                            *i,
                                            source_column.column_desc.data_type.clone(),
                                        )
                                        .into(),
                                    );
                                    FunctionCall::cast_mut(
                                        &mut input_ref,
                                        field.data_type().clone(),
                                        CastContext::Explicit,
                                    )
                                    .unwrap();
                                    input_ref
                                }
                            } else {
                                // fields like `_rw_timestamp`, would not be found in source.
                                ExprImpl::Literal(
                                    Literal::new(None, field.data_type.clone()).into(),
                                )
                            }
                        })
                        .collect_vec();
                    let logical_source = LogicalSource::with_catalog(
                        Rc::new(source_catalog.deref().clone()),
                        SourceNodeKind::CreateMViewOrBatch,
                        self.ctx(),
                        as_of,
                    )?;
                    Ok(LogicalProject::new(logical_source.into(), exprs).into())
                } else {
                    bail!(
                        "failed to plan a iceberg engine table: {}. Can't find the corresponding iceberg source. Maybe you need to recreate the table",
                        base_table.table_catalog.name()
                    );
                }
            }
        }
    }

    pub(super) fn plan_source(&mut self, source: BoundSource) -> Result<PlanRef> {
        if source.is_shareable_cdc_connector() {
            Err(ErrorCode::InternalError(
                "Should not create MATERIALIZED VIEW or SELECT directly on shared CDC source. HINT: create TABLE from the source instead.".to_owned(),
            )
            .into())
        } else {
            let as_of = source.as_of.clone();
            match as_of {
                None
                | Some(AsOf::VersionNum(_))
                | Some(AsOf::TimestampString(_))
                | Some(AsOf::TimestampNum(_)) => {}
                Some(AsOf::ProcessTime) | Some(AsOf::ProcessTimeWithInterval(_)) => {
                    bail_not_implemented!("As Of ProcessTime() is not supported yet.")
                }
                Some(AsOf::VersionString(_)) => {
                    bail_not_implemented!("As Of Version is not supported yet.")
                }
            }

            // validate the source has pk. We raise an error here to avoid panic in expect_stream_key later
            // for a nicer error message.
            if matches!(self.plan_for(), PlanFor::Stream) {
                let has_pk =
                    source.catalog.row_id_index.is_some() || !source.catalog.pk_col_ids.is_empty();
                if !has_pk {
                    // in older version, iceberg source doesn't have row_id, thus may hit this
                    let is_iceberg = source.catalog.is_iceberg_connector();
                    // only iceberg should hit this.
                    debug_assert!(is_iceberg);
                    if is_iceberg {
                        return Err(ErrorCode::BindError(format!(
                        "Cannot create a stream job from an iceberg source without a primary key.\nThe iceberg source might be created in an older version of RisingWave. Please try recreating the source.\nSource: {:?}",
                        source.catalog
                    ))
                    .into());
                    } else {
                        return Err(ErrorCode::BindError(format!(
                            "Cannot create a stream job from a source without a primary key.
This is a bug. We would appreciate a bug report at:
https://github.com/risingwavelabs/risingwave/issues/new?labels=type%2Fbug&template=bug_report.yml

source: {:?}",
                            source.catalog
                        ))
                        .into());
                    }
                }
            }
            Ok(LogicalSource::with_catalog(
                Rc::new(source.catalog),
                SourceNodeKind::CreateMViewOrBatch,
                self.ctx(),
                as_of,
            )?
            .into())
        }
    }

    pub(super) fn plan_join(&mut self, join: BoundJoin) -> Result<PlanRef> {
        let left = self.plan_relation(join.left)?;
        let right = self.plan_relation(join.right)?;
        let join_type = join.join_type;
        let on_clause = join.cond;
        if on_clause.has_subquery() {
            bail_not_implemented!("Subquery in join on condition");
        } else {
            Ok(LogicalJoin::create(left, right, join_type, on_clause))
        }
    }

    pub(super) fn plan_apply(&mut self, mut join: BoundJoin) -> Result<PlanRef> {
        let join_type = join.join_type;
        let on_clause = join.cond;
        if on_clause.has_subquery() {
            bail_not_implemented!("Subquery in join on condition");
        }

        let correlated_id = self.ctx.next_correlated_id();
        let correlated_indices = join
            .right
            .collect_correlated_indices_by_depth_and_assign_id(0, correlated_id);
        let left = self.plan_relation(join.left)?;
        let right = self.plan_relation(join.right)?;

        Ok(LogicalApply::create(
            left,
            right,
            join_type,
            Condition::with_expr(on_clause),
            correlated_id,
            correlated_indices,
            false,
        ))
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

    pub(super) fn plan_table_function(
        &mut self,
        table_function: ExprImpl,
        with_ordinality: bool,
    ) -> Result<PlanRef> {
        // TODO: maybe we can unify LogicalTableFunction with LogicalValues
        match table_function {
            ExprImpl::TableFunction(tf) => {
                Ok(LogicalTableFunction::new(*tf, with_ordinality, self.ctx()).into())
            }
            expr => {
                let schema = Schema {
                    // TODO: should be named
                    fields: vec![Field::unnamed(expr.return_type())],
                };
                let expr_return_type = expr.return_type();
                let root = LogicalValues::create(vec![vec![expr]], schema, self.ctx());
                let input_ref = ExprImpl::from(InputRef::new(0, expr_return_type.clone()));
                let mut exprs = if let DataType::Struct(st) = expr_return_type {
                    st.iter()
                        .enumerate()
                        .map(|(i, (_, ty))| {
                            let idx = ExprImpl::literal_int(i.try_into().unwrap());
                            let args = vec![input_ref.clone(), idx];
                            FunctionCall::new_unchecked(ExprType::Field, args, ty.clone()).into()
                        })
                        .collect()
                } else {
                    vec![input_ref]
                };
                if with_ordinality {
                    exprs.push(ExprImpl::literal_bigint(1));
                }
                Ok(LogicalProject::create(root, exprs))
            }
        }
    }

    pub(super) fn plan_share(&mut self, share: BoundShare) -> Result<PlanRef> {
        match share.input {
            BoundShareInput::Query(Either::Left(nonrecursive_query)) => {
                let id = share.share_id;
                match self.share_cache.get(&id) {
                    None => {
                        let result = self
                            .plan_query(nonrecursive_query)?
                            .into_unordered_subplan();
                        let logical_share = LogicalShare::create(result);
                        self.share_cache.insert(id, logical_share.clone());
                        Ok(logical_share)
                    }
                    Some(result) => Ok(result.clone()),
                }
            }
            // for the recursive union in rcte
            BoundShareInput::Query(Either::Right(recursive_union)) => self.plan_recursive_union(
                *recursive_union.base,
                *recursive_union.recursive,
                share.share_id,
            ),
            BoundShareInput::ChangeLog(relation) => {
                let id = share.share_id;
                let result = self.plan_changelog(relation)?;
                let logical_share = LogicalShare::create(result);
                self.share_cache.insert(id, logical_share.clone());
                Ok(logical_share)
            }
        }
    }

    pub(super) fn plan_watermark(&mut self, _watermark: BoundWatermark) -> Result<PlanRef> {
        todo!("plan watermark");
    }

    pub(super) fn plan_cte_ref(&mut self, cte_ref: BoundBackCteRef) -> Result<PlanRef> {
        // TODO: this is actually duplicated from `plan_recursive_union`, refactor?
        let base = self.plan_set_expr(cte_ref.base, vec![], &[])?;
        Ok(LogicalCteRef::create(cte_ref.share_id, base))
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
            Relation::Subquery(q) => q.query.schema().data_types(),
            Relation::Share(share) => share
                .input
                .fields()?
                .into_iter()
                .map(|(_, f)| f.data_type)
                .collect(),
            r => {
                return Err(ErrorCode::BindError(format!(
                    "Invalid input relation to tumble: {r:?}"
                ))
                .into());
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

        match (args.next(), args.next(), args.next()) {
            (Some(window_size @ ExprImpl::Literal(_)), None, None) => {
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
                exprs.push(window_end);
                let base = self.plan_relation(input)?;
                let project = LogicalProject::create(base, exprs);
                Ok(project)
            }
            (
                Some(window_size @ ExprImpl::Literal(_)),
                Some(window_offset @ ExprImpl::Literal(_)),
                None,
            ) => {
                let mut exprs = Vec::with_capacity(col_data_types.len() + 2);
                for (idx, col_dt) in col_data_types.iter().enumerate() {
                    exprs.push(InputRef::new(idx, col_dt.clone()).into());
                }
                let window_start: ExprImpl = FunctionCall::new(
                    ExprType::TumbleStart,
                    vec![
                        ExprImpl::InputRef(Box::new(time_col)),
                        window_size.clone(),
                        window_offset,
                    ],
                )?
                .into();
                // TODO: `window_end` may be optimized to avoid double calculation of
                // `tumble_start`, or we can depends on common expression
                // optimization.
                let window_end =
                    FunctionCall::new(ExprType::Add, vec![window_start.clone(), window_size])?
                        .into();
                exprs.push(window_start);
                exprs.push(window_end);
                let base = self.plan_relation(input)?;
                let project = LogicalProject::create(base, exprs);
                Ok(project)
            }
            _ => Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into()),
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
        let Some((ExprImpl::Literal(window_slide), ExprImpl::Literal(window_size))) =
            args.next_tuple()
        else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into());
        };

        let Some(ScalarImpl::Interval(window_slide)) = *window_slide.get_data() else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into());
        };
        let Some(ScalarImpl::Interval(window_size)) = *window_size.get_data() else {
            return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into());
        };

        let window_offset = match (args.next(), args.next()) {
            (Some(ExprImpl::Literal(window_offset)), None) => match *window_offset.get_data() {
                Some(ScalarImpl::Interval(window_offset)) => window_offset,
                _ => return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into()),
            },
            (None, None) => Interval::from_month_day_usec(0, 0, 0),
            _ => return Err(ErrorCode::BindError(ERROR_WINDOW_SIZE_ARG.to_owned()).into()),
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
