use crate::storage::PartitionedRowGroupRef;
use crate::storage::StagedRowGroupRef;
use crate::storage::Table;
use crate::storage::{MemRowGroup, MemRowGroupRef};
use crate::stream_op::{Op, StreamChunk};
use futures::channel::mpsc;
use futures::SinkExt;
use risingwave_common::array::InternalError;
use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::catalog::TableId;
use risingwave_common::error::{Result, RwError};
use risingwave_proto::plan::ColumnDesc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug)]
pub struct BummockTable {
    /// Table identifier.
    table_id: TableId,

    /// Collections of column descriptors.
    column_descs: Vec<ColumnDesc>,

    /// Current tuple id.
    current_tuple_id: AtomicU64,

    /// Stream sender.
    stream_sender: RwLock<Option<mpsc::UnboundedSender<StreamChunk>>>,

    /// Represents the cached in-memory parts of the table.
    /// Note: Remotely partitioned (merged) segments will be segmented here.
    mem_clean_segs: Vec<MemRowGroupRef>,

    /// Represents the unused in-memory parts.
    /// Note: This should be empty before buffer pool exists.
    mem_free_segs: Vec<MemRowGroupRef>,

    /// Represents the in-memory parts of the table yet flushed.
    mem_dirty_segs: RwLock<Vec<MemRowGroup>>,

    /// Represents the handles of staged parts of the table.
    staged_segs: Vec<StagedRowGroupRef>,

    /// Represents the handles of non overlapped parts of the table.
    partitioned_segs: Vec<PartitionedRowGroupRef>,

    /// synchronization protection
    rwlock: Arc<RwLock<i32>>,
}

#[async_trait::async_trait]
impl Table for BummockTable {
    async fn append(&self, data: DataChunk) -> Result<usize> {
        let _write_guard = self.rwlock.write().unwrap();
        let mut is_send_messages_success = true;

        // TODO, this looks like not necessary when we have table source stream.
        if let Some(ref mut sender) = *(self.stream_sender.write().unwrap()) {
            use crate::stream_op::Op;
            let chunk = StreamChunk::new(
                vec![Op::Insert; data.cardinality()],
                Vec::from(data.columns()),
                data.visibility().clone(),
            );

            futures::executor::block_on(async move { sender.send(chunk).await })
                .or_else(|x| {
                    // Disconnection means the receiver is dropped. So the sender shouble be dropped
                    // here too.
                    if x.is_disconnected() {
                        is_send_messages_success = false;
                        return Ok(());
                    }
                    Err(x)
                })
                .expect("send changes failed");
        }

        if !is_send_messages_success {
            *(self.stream_sender.write().unwrap()) = None;
        }

        let mut appender = self.mem_dirty_segs.write().unwrap();

        // only one row group before having disk swapping
        if appender.is_empty() {
            (*appender).push(MemRowGroup::new(self.get_column_ids().unwrap().len()));
        }

        let (ret_tuple_id, ret_cardinality) = (*appender)
            .last_mut()
            .unwrap()
            .append_data(self.current_tuple_id.load(Ordering::SeqCst), data)
            .unwrap();
        self.current_tuple_id.store(ret_tuple_id, Ordering::SeqCst);
        Ok(ret_cardinality)
    }

    fn write(&self, chunk: &StreamChunk) -> Result<usize> {
        let _write_guard = self.rwlock.write().unwrap();

        let (data_chunk, ops) = chunk.clone().into_parts();

        for op in ops.iter() {
            assert_eq!(*op, Op::Insert);
        }

        let mut appender = self.mem_dirty_segs.write().unwrap();
        // only one row group before having disk swapping
        if (appender).is_empty() {
            (*appender).push(MemRowGroup::new(self.get_column_ids().unwrap().len()));
        }

        let (ret_tuple_id, ret_cardinality) = (*appender)
            .last_mut()
            .unwrap()
            .append_data(self.current_tuple_id.load(Ordering::SeqCst), data_chunk)
            .unwrap();
        self.current_tuple_id.store(ret_tuple_id, Ordering::SeqCst);
        Ok(ret_cardinality)
    }

    fn create_stream(&self) -> Result<mpsc::UnboundedReceiver<StreamChunk>> {
        let _write_guard = self.rwlock.write().unwrap();
        ensure!(
            self.stream_sender.read().unwrap().is_none(),
            "stream of table {:?} exists",
            self.table_id
        );
        let (tx, rx) = mpsc::unbounded();
        let mut s = self.stream_sender.write().unwrap();
        *s = Some(tx);
        Ok(rx)
    }

    async fn get_data(&self) -> Result<Vec<DataChunkRef>> {
        Ok(self
            .mem_dirty_segs
            .read()
            .unwrap()
            .last()
            .unwrap()
            .get_data()
            .unwrap())
    }

    fn get_column_ids(&self) -> Result<Arc<Vec<i32>>> {
        Ok(Arc::new((0..self.column_descs.len() as i32).collect()))
    }

    fn index_of_column_id(&self, column_id: i32) -> Result<usize> {
        let column_ids = self.get_column_ids().unwrap();
        if let Some(p) = column_ids.iter().position(|c| *c == column_id) {
            Ok(p)
        } else {
            Err(RwError::from(InternalError(format!(
                "column id {:?} not found in table {:?}",
                column_id, self.table_id
            ))))
        }
    }

    fn is_stream_connected(&self) -> bool {
        self.stream_sender.read().unwrap().is_some()
    }
}

impl BummockTable {
    pub fn new(table_id: &TableId, column_descs: &[ColumnDesc]) -> Self {
        Self {
            table_id: table_id.clone(),
            column_descs: column_descs.to_vec(),
            stream_sender: RwLock::new(None),
            mem_clean_segs: Vec::new(),
            mem_free_segs: Vec::with_capacity(0), // empty before we have memory pool
            mem_dirty_segs: RwLock::new(Vec::new()),
            staged_segs: Vec::with_capacity(0), // empty before introducing IO next time
            partitioned_segs: Vec::with_capacity(0), /* emptry before introducing compaction next
                                                      * time */
            current_tuple_id: AtomicU64::new(0),
            rwlock: Arc::new(RwLock::new(1)),
        }
    }

    pub fn columns(&self) -> &Vec<ColumnDesc> {
        &self.column_descs
    }

    /// Get a tuple with `tuple_id`. This is a basic operation to fetch a tuple.
    async fn get_tuple_by_id(&self, _tupleid: u64) -> Result<DataChunk> {
        // first scan `mem_dirty_segs`

        // then scan `mem_clean_segs`

        // then request on-disk segments

        todo!();
    }

    /// Given a list of `column id`s, return the data chunks.
    /// Projection pushdown: use `projections` for interested `column_id`s.
    /// Predicate pushdown: use `predicates` for interested filtering conditions.
    async fn get_data_by_columns(
        &self,
        _projections: Option<Vec<i32>>,
        _predicates: Option<Vec<(u64, u64)>>,
    ) -> Result<Vec<DataChunkRef>> {
        // first scan `mem_dirty_segs`

        // then scan `mem_clean_seg`

        // then request on-disk segments

        // if predicates exist, process them

        todo!();
    }

    /// A `delete` simply marks the deletion of the key in the bitmap.
    /// Since we are not key-value based stores, we cannot assume the
    /// deletion is specified with specific keys. So this operation has
    /// to fetch the specific rows first and mark them deleted.
    /// That's the reason we shall have identifiers of rows and so that we
    /// can mark them explicitly in the delete bitmap.
    /// If the tuples are already in `mem_clean_segs` or `mem_dirty_segs`, locate its
    /// tuple id and mark it deleted.
    /// If the tuples are in disk, fetch them to `mem_clean_segs`, locate its
    /// tuple id and mark it deleted.
    async fn delete(&self, _predicates: Option<Vec<(u64, u64)>>) -> Result<u64> {
        // get tuple ids with the predicates

        // mark them deleted in the dbmp

        // stamp the row group if transaction boundry is hit

        todo!();
    }

    /// An `update` marks the deletion of a key and appends a new one.
    /// DN: Alternatively, deletion of a key can be ommitted and just appending
    /// a new row. But that would require more efforts scanning and merging the results
    async fn update(
        &self,
        _predicates: Option<Vec<(u64, u64)>>,
        _datachunk: Arc<Vec<DataChunkRef>>,
    ) -> Result<()> {
        // fetch and delete the tuples with delete()

        // append new data chunks

        // stamp the row group if transaction boundry is hit

        todo!();
    }

    // TODO: [xiangyhu] flush using persist::fs module
    async fn flush(&self) -> Result<()> {
        todo!();
    }
}
