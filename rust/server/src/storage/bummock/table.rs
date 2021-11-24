use crate::storage::StagedRowGroupRef;
use crate::storage::Table;
use crate::storage::{MemRowGroup, MemRowGroupRef};
use crate::storage::{PartitionedRowGroupRef, TableColumnDesc};
use crate::stream_op::{Op, StreamChunk};
use futures::channel::mpsc;
use futures::SinkExt;
use risingwave_common::array::InternalError;
use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::{Result, RwError};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::sync::RwLock;

use super::BummockResult;

#[derive(Debug)]
pub struct BummockTable {
    /// Table identifier.
    table_id: TableId,

    /// Table Column Definitions
    table_columns: Vec<TableColumnDesc>,

    /// Current tuple id.
    current_tuple_id: AtomicU64,

    /// Stream sender.
    stream_sender: RwLock<Vec<Option<mpsc::UnboundedSender<StreamChunk>>>>,

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

    /// current implicit row id
    next_row_id: AtomicUsize,
}

#[async_trait::async_trait]
impl Table for BummockTable {
    async fn append(&self, data: DataChunk) -> Result<usize> {
        let _write_guard = self.rwlock.write().unwrap();
        let mut is_send_messages_success = true;

        // TODO, this looks like not necessary when we have table source stream.
        self.stream_sender.write().unwrap().iter().for_each(|ch| {
            let sender = &mut ch.as_ref().unwrap();

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
        });

        if !is_send_messages_success {
            self.stream_sender.write().unwrap().clear();
        }

        let mut appender = self.mem_dirty_segs.write().unwrap();

        // only one row group before having disk swapping
        if appender.is_empty() {
            appender.push(MemRowGroup::new(self.get_column_ids().len()));
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
            appender.push(MemRowGroup::new(self.get_column_ids().len()));
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
        let (tx, rx) = mpsc::unbounded();
        let mut s = self.stream_sender.write().unwrap();
        s.push(Some(tx));
        Ok(rx)
    }

    async fn get_data(&self) -> Result<BummockResult> {
        // TODO, traverse other segs as well
        let segs = self.mem_dirty_segs.read().unwrap();
        match segs.is_empty() {
            true => Ok(BummockResult::DataEof),
            false => Ok(BummockResult::Data(
                segs.last().unwrap().get_data().unwrap(),
            )),
        }
    }

    fn get_column_ids(&self) -> Vec<i32> {
        self.table_columns.iter().map(|c| c.column_id).collect()
    }

    fn index_of_column_id(&self, column_id: i32) -> Result<usize> {
        let column_ids = self.get_column_ids();
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
        self.stream_sender.read().unwrap().len() > 0
    }
}

impl BummockTable {
    pub fn new(table_id: &TableId, table_columns: Vec<TableColumnDesc>) -> Self {
        Self {
            table_id: table_id.clone(),
            table_columns,
            stream_sender: RwLock::new(vec![]),
            mem_clean_segs: Vec::new(),
            mem_free_segs: Vec::with_capacity(0), // empty before we have memory pool
            mem_dirty_segs: RwLock::new(Vec::new()),
            staged_segs: Vec::with_capacity(0), // empty before introducing IO next time
            partitioned_segs: Vec::with_capacity(0), /* empty before introducing compaction next
                                                      * time */
            current_tuple_id: AtomicU64::new(0),
            rwlock: Arc::new(RwLock::new(1)),
            next_row_id: AtomicUsize::new(0),
        }
    }

    pub fn next_row_id(&self) -> usize {
        self.next_row_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn schema(&self) -> Schema {
        Schema::new(
            self.table_columns
                .iter()
                .map(|c| Field::new(c.data_type.clone()))
                .collect(),
        )
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

        // stamp the row group if transaction boundary is hit

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

#[cfg(test)]
mod tests {
    use crate::storage::{BummockResult, BummockTable, Table, TableColumnDesc};

    use risingwave_common::array::{Array, DataChunk, I64Array};
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::error::Result;
    use risingwave_common::types::{DecimalType, Int64Type};

    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_table_basic_read_write() -> Result<()> {
        let table_id = TableId::default();

        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Arc::new(DecimalType::new(false, 10, 5)?),
                },
                Field {
                    data_type: Arc::new(DecimalType::new(false, 10, 5)?),
                },
            ],
        };

        let table_columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| TableColumnDesc {
                data_type: f.data_type.clone(),
                column_id: i as i32, // use column index as column id
            })
            .collect();

        let col1 = column_nonnull! { I64Array, Int64Type, [1, 3, 5, 7, 9] };
        let col2 = column_nonnull! { I64Array, Int64Type, [2, 4, 6, 8, 10] };
        let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();

        let bummock_table = Arc::new(BummockTable::new(&table_id, table_columns));

        assert!(matches!(
            bummock_table.get_data().await?,
            BummockResult::DataEof
        ));

        let _ = bummock_table.append(data_chunk).await;
        assert_eq!(bummock_table.table_columns.len(), 2);

        assert_eq!(bummock_table.current_tuple_id.load(Ordering::Relaxed), 5);

        match bummock_table.get_data().await? {
            BummockResult::Data(v) => {
                assert_eq!(
                    v[0].column_at(0)
                        .unwrap()
                        .array()
                        .as_int64()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
                );
                assert_eq!(
                    v[0].column_at(1)
                        .unwrap()
                        .array()
                        .as_int64()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
                );
            }
            BummockResult::DataEof => {
                panic!("Empty data returned.")
            }
        }
        Ok(())
    }
}
