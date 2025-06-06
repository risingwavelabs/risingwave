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
use std::rc::Rc;

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{CdcTableDesc, ColumnCatalog, ColumnDesc, TableId};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::plan_common::GeneratedColumnDesc;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;

use super::generic::{GenericPlanRef, SourceNodeKind};
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef, PredicatePushdown,
    StreamProject, ToBatch, ToStream, generic,
};
use crate::WithOptions;
use crate::catalog::ColumnId;
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::CdcScanOptions;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamCdcTableScan,
    ToStreamContext,
};
use crate::optimizer::property::Order;
use crate::utils::{ColIndexMapping, Condition, IndexRewriter};

/// `LogicalCdcScan` reads rows of a table from an external upstream database
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalCdcScan {
    pub base: PlanBase<Logical>,
    core: generic::CdcScan,

    /// Expressions to output. This field presents and will be turned to a `Project` when
    /// converting to a physical plan, only if there are generated columns.
    pub(crate) output_exprs: Option<Vec<ExprImpl>>,
    /// When there are generated columns, the stream key is different from
    /// the one in `core`. So we store the one in `output_exprs` here.
    pub(crate) output_stream_key: Option<Vec<usize>>,
}

impl From<generic::CdcScan> for LogicalCdcScan {
    fn from(core: generic::CdcScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self {
            base,
            core,
            output_exprs: None,
            output_stream_key: None,
        }
    }
}

impl From<generic::CdcScan> for PlanRef {
    fn from(core: generic::CdcScan) -> Self {
        LogicalCdcScan::from(core).into()
    }
}

impl LogicalCdcScan {
    pub fn create(
        table_name: String, // explain-only
        cdc_table_desc: Rc<CdcTableDesc>,
        ctx: OptimizerContextRef,
        options: CdcScanOptions,
        kind: SourceNodeKind,
    ) -> Self {
        let core = generic::CdcScan::new(
            table_name,
            (0..cdc_table_desc.columns.len()).collect(),
            cdc_table_desc,
            ctx,
            options,
            kind, // This is a placeholder; the actual kind should be set later
        );
        let base = PlanBase::new_logical_with_core(&core);
        Self {
            base,
            core,
            output_exprs: None,
            output_stream_key: None,
        }
    }

    pub fn with_catalog(
        source_catalog: Rc<SourceCatalog>,
        kind: SourceNodeKind,
        ctx: OptimizerContextRef,
    ) -> Result<Self> {
        let column_catalogs = source_catalog.columns.clone();

        let non_generated_columns: Vec<ColumnDesc> = column_catalogs
            .iter()
            .filter(|c| !c.is_generated())
            .map(|c| c.column_desc.clone())
            .collect();

        let pk_column_indices = {
            let mut id_to_idx: HashMap<risingwave_common::catalog::ColumnId, usize> =
                HashMap::new();
            non_generated_columns
                .iter()
                .enumerate()
                .for_each(|(idx, c)| {
                    id_to_idx.insert(c.column_id, idx);
                });
            // pk column id must exist in table columns.
            source_catalog
                .pk_col_ids
                .iter()
                .map(|c| id_to_idx.get(c).copied().unwrap())
                .collect_vec()
        };

        let cdc_etl_info = source_catalog.cdc_etl_info.clone().unwrap();
        let table_name = cdc_etl_info.external_table_name.clone();

        let (connect_properties, connect_secret_refs) =
            source_catalog.with_properties.clone().into_parts();

        let table_pk = pk_column_indices
            .iter()
            .map(|idx| ColumnOrder::new(*idx, OrderType::ascending()))
            .collect();

        let cdc_table_desc = Rc::new(CdcTableDesc {
            table_id: TableId::placeholder(), // Should be filled in `fill_job`
            source_id: cdc_etl_info.shared_source_id.into(),
            external_table_name: table_name.clone(),
            pk: table_pk,
            columns: non_generated_columns,
            stream_key: pk_column_indices.clone(),
            connect_properties,
            secret_refs: connect_secret_refs,
        });

        let etl_source_properties = WithOptions::new_with_options(cdc_etl_info.properties);

        let options = CdcScanOptions::from_with_options(&etl_source_properties)?;

        let output_exprs = Self::derive_output_exprs_from_generated_columns(&column_catalogs)?;

        let (core, output_stream_key) = Self::exclude_generated_columns_from_core(
            table_name,
            cdc_table_desc,
            column_catalogs,
            ctx,
            options,
            kind,
        );

        let base = PlanBase::new_logical_with_core(&core);

        Ok(LogicalCdcScan {
            base,
            core,
            output_exprs,
            output_stream_key,
        })
    }

    /// If there are no generated columns, returns `None`.
    ///
    /// Otherwise, the returned expressions correspond to all columns.
    /// Non-generated columns are represented by `InputRef`.
    pub fn derive_output_exprs_from_generated_columns(
        columns: &[ColumnCatalog],
    ) -> Result<Option<Vec<ExprImpl>>> {
        if !columns.iter().any(|c| c.is_generated()) {
            return Ok(None);
        }

        let col_mapping = {
            let mut mapping = vec![None; columns.len()];
            let mut cur = 0;
            for (idx, column) in columns.iter().enumerate() {
                if !column.is_generated() {
                    mapping[idx] = Some(cur);
                    cur += 1;
                } else {
                    mapping[idx] = None;
                }
            }
            ColIndexMapping::new(mapping, columns.len())
        };

        let mut rewriter = IndexRewriter::new(col_mapping);
        let mut exprs = Vec::with_capacity(columns.len());
        let mut cur = 0;
        for column in columns {
            let column_desc = &column.column_desc;
            let ret_data_type = column_desc.data_type.clone();

            if let Some(GeneratedOrDefaultColumn::GeneratedColumn(generated_column)) =
                &column_desc.generated_or_default_column
            {
                let GeneratedColumnDesc { expr } = generated_column;
                // TODO(yuhao): avoid this `from_expr_proto`.
                let proj_expr =
                    rewriter.rewrite_expr(ExprImpl::from_expr_proto(expr.as_ref().unwrap())?);
                let casted_expr = proj_expr.cast_assign(ret_data_type)?;
                exprs.push(casted_expr);
            } else {
                let input_ref = InputRef {
                    data_type: ret_data_type,
                    index: cur,
                };
                cur += 1;
                exprs.push(ExprImpl::InputRef(Box::new(input_ref)));
            }
        }

        Ok(Some(exprs))
    }

    fn exclude_generated_columns_from_core(
        table_name: String,
        cdc_table_desc: Rc<CdcTableDesc>,
        mut column_catalogs: Vec<ColumnCatalog>,
        ctx: OptimizerContextRef,
        options: CdcScanOptions,
        kind: SourceNodeKind,
    ) -> (generic::CdcScan, Option<Vec<usize>>) {
        let original_stream_key = cdc_table_desc.stream_key.clone();

        // Calculate new stream key indices after excluding generated columns
        let output_stream_key = if column_catalogs.iter().any(|c| c.is_generated()) {
            let new_stream_key = original_stream_key
                .iter()
                .map(|&idx| {
                    let mut cnt = 0;
                    for (i, col) in column_catalogs.iter().enumerate() {
                        if i == idx {
                            break;
                        }
                        if col.is_generated() {
                            cnt += 1;
                        }
                    }
                    idx - cnt
                })
                .collect();
            Some(new_stream_key)
        } else {
            None
        };

        // Filter out generated columns
        column_catalogs.retain(|c| !c.is_generated());
        let output_col_idx = (0..column_catalogs.len()).collect();

        let filtered_cdc_table_desc = Rc::new(CdcTableDesc {
            table_id: cdc_table_desc.table_id,
            source_id: cdc_table_desc.source_id,
            external_table_name: cdc_table_desc.external_table_name.clone(),
            pk: cdc_table_desc.pk.clone(),
            columns: cdc_table_desc.columns.clone(),
            stream_key: output_stream_key.clone().unwrap_or(original_stream_key),
            connect_properties: cdc_table_desc.connect_properties.clone(),
            secret_refs: cdc_table_desc.secret_refs.clone(),
        });

        let core = generic::CdcScan::new(
            table_name,
            output_col_idx,
            filtered_cdc_table_desc,
            ctx,
            options,
            kind,
        );

        (core, output_stream_key)
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn cdc_table_desc(&self) -> &CdcTableDesc {
        self.core.cdc_table_desc.as_ref()
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.core.column_descs()
    }

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.core.output_column_ids()
    }

    pub fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        let new_core = generic::CdcScan::new(
            self.table_name().to_owned(),
            output_col_idx,
            self.core.cdc_table_desc.clone(),
            self.base.ctx().clone(),
            self.core.options.clone(),
            self.core.kind.clone(),
        );

        Self {
            base: PlanBase::new_logical_with_core(&new_core),
            core: new_core,
            output_exprs: self.output_exprs.clone(),
            output_stream_key: self.output_stream_key.clone(),
        }
    }

    pub fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }
}

impl_plan_tree_node_for_leaf! {LogicalCdcScan}

impl Distill for LogicalCdcScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(5);
        vec.push(("table", Pretty::from(self.table_name().to_owned())));
        let key_is_columns = true;
        let key = if key_is_columns {
            "columns"
        } else {
            "output_columns"
        };
        vec.push((key, self.core.columns_pretty(verbose)));
        if !key_is_columns {
            vec.push((
                "required_columns",
                Pretty::Array(
                    self.output_col_idx()
                        .iter()
                        .map(|i| {
                            let col_name = &self.cdc_table_desc().columns[*i].name;
                            Pretty::from(if verbose {
                                format!("{}.{}", self.table_name(), col_name)
                            } else {
                                col_name.to_string()
                            })
                        })
                        .collect(),
                ),
            ));
        }

        childless_record("LogicalCdcScan", vec)
    }
}

impl ColPrunable for LogicalCdcScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        // TODO: prune the columns by pushing project to cdc backfill executor.
        // This is tricky because we need to ensure pk is not pruned.
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl ExprRewritable for LogicalCdcScan {
    fn has_rewritable_expr(&self) -> bool {
        self.output_exprs.is_some()
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let core = self.core.clone();
        core.rewrite_exprs(r);

        let mut output_exprs = self.output_exprs.clone();
        for expr in output_exprs.iter_mut().flatten() {
            *expr = r.rewrite_expr(expr.clone());
        }

        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
            output_exprs,
            output_stream_key: self.output_stream_key.clone(),
        }
        .into()
    }
}

impl ExprVisitable for LogicalCdcScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
        self.output_exprs
            .iter()
            .flatten()
            .for_each(|e| v.visit_expr(e));
    }
}

impl PredicatePushdown for LogicalCdcScan {
    fn predicate_pushdown(
        &self,
        _predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        self.clone().into()
    }
}

impl ToBatch for LogicalCdcScan {
    fn to_batch(&self) -> Result<PlanRef> {
        unreachable!()
    }

    fn to_batch_with_order_required(&self, _required_order: &Order) -> Result<PlanRef> {
        unreachable!()
    }
}

impl ToStream for LogicalCdcScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let mut plan: PlanRef;
        match self.core.kind {
            SourceNodeKind::CreateTable => {
                // Note: for create table, generated columns is created in plan_root.gen_table_plan.
                plan = StreamCdcTableScan::new(self.core.clone()).into()
            }
            SourceNodeKind::CreateMViewOrBatch => {
                plan = StreamCdcTableScan::new(self.core.clone()).into();
                if let Some(exprs) = &self.output_exprs {
                    let logical_project = generic::Project::new(exprs.to_vec(), plan);
                    plan = StreamProject::new(logical_project).into();
                }
            }
            SourceNodeKind::CreateSharedSource => {
                unreachable!("LogicalCdcScan should not be used for shared source");
            }
        }
        Ok(plan)
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }
}
