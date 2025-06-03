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
use std::str::FromStr;

use anyhow::anyhow;
use educe::Educe;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::{CdcTableDesc, ColumnDesc, Field, Schema};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_connector::source::cdc::{
    CDC_BACKFILL_ENABLE_KEY, CDC_BACKFILL_SNAPSHOT_BATCH_SIZE_KEY,
    CDC_BACKFILL_SNAPSHOT_INTERVAL_KEY,
};
use risingwave_pb::stream_plan::StreamCdcScanOptions;

use super::{GenericPlanNode, SourceNodeKind};
use crate::WithOptions;
use crate::catalog::ColumnId;
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{FunctionalDependencySet, MonotonicityMap, WatermarkColumns};

/// [`CdcScan`] reads rows of a table from an external upstream database
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct CdcScan {
    pub table_name: String,
    /// Include `output_col_idx` and columns required in `predicate`
    /// Currently not in use, but needed when we push the column pruning to the source node.
    pub output_col_idx: Vec<usize>,
    /// Descriptor of the external table for CDC
    pub cdc_table_desc: Rc<CdcTableDesc>,
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,

    pub options: CdcScanOptions,
    pub kind: SourceNodeKind,
}

#[derive(Debug, Clone, Hash, PartialEq)]
pub struct CdcScanOptions {
    pub disable_backfill: bool,
    pub snapshot_barrier_interval: u32,
    pub snapshot_batch_size: u32,
}

impl Default for CdcScanOptions {
    fn default() -> Self {
        Self {
            disable_backfill: false,
            snapshot_barrier_interval: 1,
            snapshot_batch_size: 1000,
        }
    }
}

impl CdcScanOptions {
    pub fn from_with_options(with_options: &WithOptions) -> Result<Self> {
        // unspecified option will use default values
        let mut scan_options = Self::default();

        // disable backfill if 'snapshot=false'
        if let Some(snapshot) = with_options.get(CDC_BACKFILL_ENABLE_KEY) {
            scan_options.disable_backfill = !(bool::from_str(snapshot)
                .map_err(|_| anyhow!("Invalid value for {}", CDC_BACKFILL_ENABLE_KEY))?);
        };

        if let Some(snapshot_interval) = with_options.get(CDC_BACKFILL_SNAPSHOT_INTERVAL_KEY) {
            scan_options.snapshot_barrier_interval = u32::from_str(snapshot_interval)
                .map_err(|_| anyhow!("Invalid value for {}", CDC_BACKFILL_SNAPSHOT_INTERVAL_KEY))?;
        };

        if let Some(snapshot_batch_size) = with_options.get(CDC_BACKFILL_SNAPSHOT_BATCH_SIZE_KEY) {
            scan_options.snapshot_batch_size =
                u32::from_str(snapshot_batch_size).map_err(|_| {
                    anyhow!("Invalid value for {}", CDC_BACKFILL_SNAPSHOT_BATCH_SIZE_KEY)
                })?;
        };

        Ok(scan_options)
    }

    pub fn to_proto(&self) -> StreamCdcScanOptions {
        StreamCdcScanOptions {
            disable_backfill: self.disable_backfill,
            snapshot_barrier_interval: self.snapshot_barrier_interval,
            snapshot_batch_size: self.snapshot_batch_size,
        }
    }
}

impl CdcScan {
    pub fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) {}

    pub fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {}

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.output_col_idx
            .iter()
            .map(|i| self.get_table_columns()[*i].column_id)
            .collect()
    }

    pub fn primary_key(&self) -> &[ColumnOrder] {
        &self.cdc_table_desc.pk
    }

    pub fn watermark_columns(&self) -> WatermarkColumns {
        WatermarkColumns::new()
    }

    pub fn columns_monotonicity(&self) -> MonotonicityMap {
        MonotonicityMap::new()
    }

    pub(crate) fn column_names_with_table_prefix(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| format!("{}.{}", self.table_name, self.get_table_columns()[i].name))
            .collect()
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].name.clone())
            .collect()
    }

    /// get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(
            &self.output_col_idx,
            self.get_table_columns().len(),
        )
    }

    /// Get the ids of the output columns and primary key columns.
    pub fn output_and_pk_column_ids(&self) -> Vec<ColumnId> {
        let mut ids = self.output_column_ids();
        for column_order in self.primary_key() {
            let id = self.get_table_columns()[column_order.column_index].column_id;
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }

    /// Create a logical scan node for CDC backfill
    pub(crate) fn new(
        table_name: String,
        output_col_idx: Vec<usize>, // the column index in the table
        cdc_table_desc: Rc<CdcTableDesc>,
        ctx: OptimizerContextRef,
        options: CdcScanOptions,
        kind: SourceNodeKind,
    ) -> Self {
        Self {
            table_name,
            output_col_idx,
            cdc_table_desc,
            ctx,
            options,
            kind,
        }
    }

    pub(crate) fn columns_pretty<'a>(&self, verbose: bool) -> Pretty<'a> {
        Pretty::Array(
            match verbose {
                true => self.column_names_with_table_prefix(),
                false => self.column_names(),
            }
            .into_iter()
            .map(Pretty::from)
            .collect(),
        )
    }
}

// TODO: extend for cdc table
impl GenericPlanNode for CdcScan {
    fn schema(&self) -> Schema {
        let fields = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.get_table_columns()[*tb_idx];
                col.into()
            })
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.cdc_table_desc.stream_key.clone())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.stream_key();
        let col_num = self.output_col_idx.len();
        match &pk_indices {
            Some(pk_indices) => FunctionalDependencySet::with_key(col_num, pk_indices),
            None => FunctionalDependencySet::new(col_num),
        }
    }
}

impl CdcScan {
    pub fn get_table_columns(&self) -> &[ColumnDesc] {
        &self.cdc_table_desc.columns
    }

    pub fn append_only(&self) -> bool {
        false
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].clone())
            .collect()
    }
}
