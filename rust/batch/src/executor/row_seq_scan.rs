use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_storage::table::mview::{new_adhoc_mview_table, MViewTable};
use risingwave_storage::table::{ScannableTable, TableIterRef};
use risingwave_storage::{dispatch_state_store, Keyspace, StateStoreImpl};

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

/// Executor that scans data from row table
pub struct RowSeqScanExecutor {
    table: Arc<dyn ScannableTable>,
    /// An iterator to scan StateStore.
    iter: Option<TableIterRef>,
    primary: bool,

    chunk_size: usize,
    schema: Schema,
    identity: String,

    epoch: u64,
}

impl RowSeqScanExecutor {
    // TODO: decide the chunk size for row seq scan
    pub const DEFAULT_CHUNK_SIZE: usize = 1024;

    pub fn new(
        table: Arc<dyn ScannableTable>,
        chunk_size: usize,
        primary: bool,
        identity: String,
        epoch: u64,
    ) -> Self {
        let schema = table.schema().into_owned();

        Self {
            table,
            iter: None,
            primary,
            chunk_size,
            schema,
            identity,
            epoch,
        }
    }

    // TODO: Remove this when we support real partition-scan.
    // For shared storage like Hummock, we are using a fake partition-scan now. If `self.primary` is
    // false, we'll ignore this scanning and yield no chunk.
    fn should_ignore(&self) -> bool {
        if self.table.is_shared_storage() {
            !self.primary
        } else {
            false
        }
    }
}

impl BoxedExecutorBuilder for RowSeqScanExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let seq_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::RowSeqScan
        )?;

        let table_id = TableId::from(&seq_scan_node.table_ref_id);
        let column_ids = seq_scan_node
            .column_ids
            .iter()
            .map(|column_id| ColumnId::new(*column_id))
            .collect_vec();
        let fields = seq_scan_node
            .fields
            .iter()
            .map(|field| Field::from(field))
            .collect_vec();

        let table = new_adhoc_mview_table(
            source.global_batch_env().state_store(),
            &table_id,
            &column_ids,
            &fields,
        );

        Ok(Box::new(Self::new(
            table,
            Self::DEFAULT_CHUNK_SIZE,
            source.task_id.task_id == 0,
            source.plan_node().get_identity().clone(),
            source.epoch,
        )))
    }
}

#[async_trait::async_trait]
impl Executor for RowSeqScanExecutor {
    async fn open(&mut self) -> Result<()> {
        if self.should_ignore() {
            info!("non-primary row seq scan, ignored");
            return Ok(());
        }

        self.iter = Some(self.table.iter(self.epoch).await?);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.should_ignore() {
            return Ok(None);
        }

        let iter = self.iter.as_mut().expect("executor not open");

        let column_indices = (0..self.table.schema().len()).collect_vec();
        self.table
            .collect_from_iter(iter, &column_indices, Some(self.chunk_size))
            .await
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
