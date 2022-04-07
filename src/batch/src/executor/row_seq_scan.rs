use std::sync::Arc;

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
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnDesc, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_storage::table::cell_based_table::{CellBasedTable, CellBasedTableRowIter};
// use risingwave_storage::table::mview::{MViewTable, MViewTableIter};
use risingwave_storage::{dispatch_state_store, Keyspace, StateStore, StateStoreImpl};

use super::monitor::BatchMetrics;
use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

/// Executor that scans data from row table
pub struct RowSeqScanExecutor<S: StateStore> {
    table: CellBasedTable<S>,
    /// An iterator to scan StateStore.
    iter: Option<CellBasedTableRowIter<S>>,
    primary: bool,

    chunk_size: usize,
    schema: Schema,
    identity: String,

    epoch: u64,

    stats: Arc<BatchMetrics>,
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    pub fn new(
        table: CellBasedTable<S>,
        chunk_size: usize,
        primary: bool,
        identity: String,
        epoch: u64,
        stats: Arc<BatchMetrics>,
    ) -> Self {
        let schema = table.schema().clone();

        Self {
            table,
            iter: None,
            primary,
            chunk_size,
            schema,
            identity,
            epoch,
            stats,
        }
    }

    // TODO: Remove this when we support real partition-scan.
    // For shared storage like Hummock, we are using a fake partition-scan now. If `self.primary` is
    // false, we'll ignore this scanning and yield no chunk.
    fn should_ignore(&self) -> bool {
        !self.primary
    }
}

pub struct RowSeqScanExecutorBuilder {}

impl RowSeqScanExecutorBuilder {
    // TODO: decide the chunk size for row seq scan
    pub const DEFAULT_CHUNK_SIZE: usize = 1024;
}

impl BoxedExecutorBuilder for RowSeqScanExecutorBuilder {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let seq_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::RowSeqScan
        )?;

        let table_id = TableId {
            table_id: seq_scan_node.table_desc.as_ref().unwrap().table_id,
        };
        let column_descs = seq_scan_node
            .column_descs
            .iter()
            .map(|column_desc| ColumnDesc::from(column_desc.clone()))
            .collect_vec();
        dispatch_state_store!(source.global_batch_env().state_store(), state_store, {
            let keyspace = Keyspace::table_root(state_store.clone(), &table_id);
            let storage_stats = state_store.stats();
            let batch_stats = source.global_batch_env().stats();
            let table = CellBasedTable::new_adhoc(keyspace, column_descs, storage_stats);
            Ok(Box::new(
                RowSeqScanExecutor::new(
                    table,
                    RowSeqScanExecutorBuilder::DEFAULT_CHUNK_SIZE,
                    source.task_id.task_id == 0,
                    source.plan_node().get_identity().clone(),
                    source.epoch,
                    batch_stats,
                )
                .fuse(),
            ))
        })
    }
}

#[async_trait::async_trait]
impl<S: StateStore> Executor for RowSeqScanExecutor<S> {
    async fn open(&mut self) -> Result<()> {
        if self.should_ignore() {
            info!("non-primary row seq scan, ignored");
            return Ok(());
        }

        self.iter = Some(self.table.iter(self.epoch).await?);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        let timer = self.stats.row_seq_scan_next_duration.start_timer();
        if self.should_ignore() {
            return Ok(None);
        }

        let iter = self.iter.as_mut().expect("executor not open");
        let result = iter
            .collect_data_chunk(&self.table, Some(self.chunk_size))
            .await;
        timer.observe_duration();

        if let Ok(Some(r)) = result {
            debug!("Cardinality of chunk: {}, data: {:?}", r.cardinality(), &r);
            Ok(Some(r))
        } else {
            result
        }
    }

    async fn close(&mut self) -> Result<()> {
        info!("Table scan closed.");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}
