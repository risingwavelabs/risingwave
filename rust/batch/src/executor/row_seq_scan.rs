use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_storage::table::mview::{MViewTable, MViewTableIter};
use risingwave_storage::{dispatch_state_store, Keyspace, StateStore, StateStoreImpl};

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

/// Executor that scans data from row table
pub struct RowSeqScanExecutor<S: StateStore> {
    table: MViewTable<S>,
    /// An iterator to scan StateStore.
    iter: Option<MViewTableIter<S>>,
    primary: bool,

    chunk_size: usize,
    schema: Schema,
    identity: String,

    epoch: u64,
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    pub fn new(
        table: MViewTable<S>,
        chunk_size: usize,
        primary: bool,
        identity: String,
        epoch: u64,
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

        let table_id = TableId::from(&seq_scan_node.table_ref_id);
        // TODO(lmatz): Send ColumnDesc directly.
        let column_ids = seq_scan_node
            .column_ids
            .iter()
            .map(|column_id| ColumnId::new(*column_id))
            .collect_vec();
        let fields = seq_scan_node.fields.iter().map(Field::from).collect_vec();
        let column_descs = column_ids
            .into_iter()
            .zip_eq(fields.into_iter())
            .map(|(column_id, field)| ColumnDesc {
                data_type: field.data_type,
                column_id,
                name: field.name,
            })
            .collect_vec();

        dispatch_state_store!(source.global_batch_env().state_store(), state_store, {
            let keyspace = Keyspace::table_root(state_store, &table_id);
            let table = MViewTable::new_adhoc(keyspace, column_descs);
            Ok(Box::new(RowSeqScanExecutor::new(
                table,
                RowSeqScanExecutorBuilder::DEFAULT_CHUNK_SIZE,
                source.task_id.task_id == 0,
                source.plan_node().get_identity().clone(),
                source.epoch,
            )))
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
        if self.should_ignore() {
            return Ok(None);
        }

        let iter = self.iter.as_mut().expect("executor not open");
        iter.collect_data_chunk(&self.table, Some(self.chunk_size))
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
