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

use std::rc::Rc;
use std::sync::Arc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_connector::source::iceberg::IcebergFileScanTask;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, LogicalProject, PlanBase,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    BatchIcebergScan, ColumnPruningContext, LogicalFilter, LogicalSource, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalIcebergScan` is only used by batch queries. At the beginning of the batch query optimization, `LogicalSource` with a iceberg property would be converted into a `LogicalIcebergScan`.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct LogicalIcebergScan {
    pub base: PlanBase<Logical>,
    pub logical_source: LogicalSource,
    pub predicate: Condition,
    pub iceberg_file_scan_task: Option<Arc<IcebergFileScanTask>>,
    pub count: u64,
    pub snapshot_id: Option<i64>,
}

impl Eq for LogicalIcebergScan {}

impl LogicalIcebergScan {
    pub fn new(logical_source: &LogicalSource, snapshot_id: Option<i64>) -> Self {
        assert!(logical_source.core.is_iceberg_connector());
        let base = PlanBase::new_logical_with_core(&logical_source.core);
        assert!(logical_source.output_exprs.is_none());
        LogicalIcebergScan {
            base,
            logical_source: logical_source.clone(),
            predicate: Condition::true_cond(),
            iceberg_file_scan_task: None,
            count: 0,
            snapshot_id,
        }
    }

    pub fn new_with_iceberg_file_scan_task(
        logical_source: &LogicalSource,
        iceberg_file_scan_task: IcebergFileScanTask,
        count: u64,
        snapshot_id: Option<i64>,
    ) -> Self {
        assert!(logical_source.core.is_iceberg_connector());
        let base = PlanBase::new_logical_with_core(&logical_source.core);
        assert!(logical_source.output_exprs.is_none());

        LogicalIcebergScan {
            base,
            logical_source: logical_source.clone(),
            predicate: Condition::true_cond(),
            iceberg_file_scan_task: Some(Arc::new(iceberg_file_scan_task)),
            count,
            snapshot_id,
        }
    }

    pub fn predicate(&self) -> Condition {
        self.predicate.clone()
    }

    pub fn clone_with_predicate(&self, predicate: Condition) -> Self {
        Self {
            base: self.base.clone(),
            logical_source: self.logical_source.clone(),
            predicate,
            iceberg_file_scan_task: self.iceberg_file_scan_task.clone(),
            count: self.count,
            snapshot_id: self.snapshot_id,
        }
    }

    pub fn clone_with_iceberg_file_scan_task(
        &self,
        iceberg_file_scan_task: IcebergFileScanTask,
        count: u64,
    ) -> Self {
        Self {
            base: self.base.clone(),
            logical_source: self.logical_source.clone(),
            predicate: self.predicate.clone(),
            iceberg_file_scan_task: Some(Arc::new(iceberg_file_scan_task)),
            count,
            snapshot_id: self.snapshot_id,
        }
    }

    pub fn iceberg_scan_type(&self) -> Result<IcebergScanType> {
        match self.iceberg_file_scan_task.as_ref() {
            Some(task) => Ok(task.get_iceberg_scan_type()),
            None => Err(crate::error::ErrorCode::BindError(
                "Iceberg file scan task is missing in LogicalIcebergScan".to_string(),
            )
            .into()),
        }
    }

    pub fn new_count_star_with_logical_iceberg_scan(
        logical_iceberg_scan: &LogicalIcebergScan,
    ) -> Self {
        let mut logical_source = logical_iceberg_scan.logical_source.clone();
        logical_source.core.column_catalog = vec![ColumnCatalog::visible(ColumnDesc::named(
            "count",
            ColumnId::first_user_column(),
            DataType::Int64,
        ))];
        let base = PlanBase::new_logical_with_core(&logical_source.core);

        LogicalIcebergScan {
            base,
            logical_source,
            predicate: Condition::true_cond(),
            iceberg_file_scan_task: Some(Arc::new(IcebergFileScanTask::CountStar(
                logical_iceberg_scan.count,
            ))),
            count: 0,
            snapshot_id: logical_iceberg_scan.snapshot_id,
        }
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.logical_source.core.catalog.clone()
    }

    pub fn clone_with_required_cols(&self, required_cols: &[usize]) -> Self {
        assert!(!required_cols.is_empty());
        let mut core = self.logical_source.core.clone();
        let mut has_row_id = false;
        core.column_catalog = required_cols
            .iter()
            .map(|idx| {
                if Some(*idx) == core.row_id_index {
                    has_row_id = true;
                }
                core.column_catalog[*idx].clone()
            })
            .collect();
        if !has_row_id {
            core.row_id_index = None;
        }
        let base = PlanBase::new_logical_with_core(&core);

        LogicalIcebergScan {
            base,
            logical_source: LogicalSource {
                core,
                ..self.logical_source.clone()
            },
            predicate: self.predicate.clone(),
            iceberg_file_scan_task: self.iceberg_file_scan_task.clone(),
            count: self.count,
            snapshot_id: self.snapshot_id,
        }
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalIcebergScan}
impl Distill for LogicalIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
                (
                    "iceberg_scan_type",
                    match self.iceberg_file_scan_task.as_deref() {
                        Some(task) => Pretty::debug(&task.get_iceberg_scan_type()),
                        None => Pretty::debug(&"NoType"),
                    },
                ),
            ]
        } else {
            vec![]
        };
        childless_record("LogicalIcebergScan", fields)
    }
}

impl ColPrunable for LogicalIcebergScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        if required_cols.is_empty() {
            let mapping =
                ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
            // If reuqiured_cols is empty, we use the first column of iceberg to avoid the empty schema.
            LogicalProject::with_mapping(self.clone_with_required_cols(&[0]).into(), mapping).into()
        } else {
            self.clone_with_required_cols(required_cols).into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalIcebergScan {}

impl ExprVisitable for LogicalIcebergScan {}

impl PredicatePushdown for LogicalIcebergScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalIcebergScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        let plan = BatchIcebergScan::new(
            self.logical_source.core.clone(),
            self.iceberg_file_scan_task.as_ref().ok_or_else(|| {
                crate::error::ErrorCode::BindError(
                    "Iceberg file scan task is missing in LogicalIcebergScan when converting to BatchIcebergScan".to_string(),
                )
            })?.clone(),
            self.snapshot_id
        )
        .into();
        Ok(plan)
    }
}

impl ToStream for LogicalIcebergScan {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        unreachable!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!()
    }
}
