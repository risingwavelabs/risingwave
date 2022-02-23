use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use async_trait::async_trait;
use rand::prelude::SliceRandom;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_storage::table::ScannableTableRef;
use risingwave_storage::TableColumnDesc;
use tokio::sync::mpsc;

use crate::{BatchSourceReader, Source, StreamSourceReader};

#[derive(Debug)]
struct TableSourceV2Core {
    _table: ScannableTableRef,

    /// The senders of the changes channel.
    ///
    /// When a `StreamReader` is created, a channel will be created and the sender will be
    /// saved here. The insert statement will take one channel randomly.
    changes_txs: Vec<mpsc::UnboundedSender<StreamChunk>>,
}

/// [`TableSourceV2`] is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows will be send to the associated "materialize" streaming task, then be written to the
/// state store. Therefore, [`TableSourceV2`] can be simply be treated as a channel without side
/// effects.
#[derive(Debug)]
pub struct TableSourceV2 {
    core: RwLock<TableSourceV2Core>,

    /// All columns in this table.
    column_descs: Vec<TableColumnDesc>,

    /// Curren allocated row id.
    next_row_id: AtomicUsize,
}

impl TableSourceV2 {
    pub fn new(table: ScannableTableRef) -> Self {
        let column_descs = table.column_descs().into_owned();

        let core = TableSourceV2Core {
            _table: table,
            changes_txs: vec![],
        };

        Self {
            core: RwLock::new(core),
            column_descs,
            next_row_id: 0.into(),
        }
    }

    /// Generate a global-unique row id with given `worker_id`.
    pub fn next_row_id(&self, worker_id: u32) -> i64 {
        let local_row_id = self.next_row_id.fetch_add(1, Ordering::SeqCst) as u32;

        // Concatenate worker_id and local_row_id to produce a global-unique row_id
        (((worker_id as u64) << 32) + (local_row_id as u64)) as i64
    }

    /// Write stream chunk into table. Changes writen here will be simply passed to the associated
    /// streaming task via channel, and then be materialized to storage there.
    pub async fn write_chunk(&self, chunk: StreamChunk) -> Result<()> {
        let core = self.core.read().unwrap();

        let tx = core
            .changes_txs
            .choose(&mut rand::thread_rng())
            .expect("no table reader exists");
        tx.send(chunk).expect("write chunk to table reader failed");

        Ok(())
    }
}

#[derive(Debug)]
pub struct TableV2ReaderContext;

// TODO: Currently batch read directly calls api from `ScannableTable` instead of using
// `BatchReader`.
#[derive(Debug)]
pub struct TableV2BatchReader;

#[async_trait]
impl BatchSourceReader for TableV2BatchReader {
    async fn open(&mut self) -> Result<()> {
        unimplemented!()
    }

    async fn next(&mut self) -> Result<Option<risingwave_common::array::DataChunk>> {
        unimplemented!()
    }

    async fn close(&mut self) -> Result<()> {
        unimplemented!()
    }
}

/// [`TableV2StreamReader`] reads changes from a certain table continuously.
/// This struct should be only used for associated materialize task, thus the reader should be
/// created only once. Further streaming task relying on this table source should follow the
/// structure of "`MView` on `MView`".
#[derive(Debug)]
pub struct TableV2StreamReader {
    /// The receiver of the changes channel. This field is `Some` only if the reader has been
    /// `open`ed.
    rx: mpsc::UnboundedReceiver<StreamChunk>,

    /// Mappings from the source column to the column to be read.
    column_indices: Vec<usize>,
}

#[async_trait]
impl StreamSourceReader for TableV2StreamReader {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        let chunk = match self.rx.recv().await {
            Some(chunk) => chunk,
            None => panic!("TableSourceV2 dropped before associated streaming task terminated"),
        };

        let (ops, columns, bitmap) = chunk.into_inner();

        let selected_columns = self
            .column_indices
            .iter()
            .map(|i| columns[*i].clone())
            .collect();

        Ok(StreamChunk::new(ops, selected_columns, bitmap))
    }
}

#[async_trait]
impl Source for TableSourceV2 {
    type ReaderContext = TableV2ReaderContext;
    type BatchReader = TableV2BatchReader;
    type StreamReader = TableV2StreamReader;

    fn batch_reader(
        &self,
        _context: Self::ReaderContext,
        _column_ids: Vec<ColumnId>,
    ) -> Result<Self::BatchReader> {
        unreachable!("should use table_scan instead of stream_scan to read the table source")
    }

    fn stream_reader(
        &self,
        _context: Self::ReaderContext,
        column_ids: Vec<ColumnId>,
    ) -> Result<Self::StreamReader> {
        let column_indices = column_ids
            .into_iter()
            .map(|id| {
                self.column_descs
                    .iter()
                    .position(|c| c.column_id == id)
                    .expect("column id not exists")
            })
            .collect();

        let mut core = self.core.write().unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        core.changes_txs.push(tx);

        Ok(TableV2StreamReader { rx, column_indices })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array, Op};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::mview::MViewTable;
    use risingwave_storage::Keyspace;

    use super::*;

    fn new_source() -> TableSourceV2 {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::table_root(store, &Default::default());
        let table = Arc::new(MViewTable::new_batch(
            keyspace,
            vec![TableColumnDesc::unnamed(ColumnId::from(0), DataType::Int64)],
        ));

        TableSourceV2::new(table)
    }

    #[tokio::test]
    async fn test_table_source_v2() -> Result<()> {
        let source = new_source();
        let mut reader = source.stream_reader(TableV2ReaderContext, vec![ColumnId::from(0)])?;

        macro_rules! write_chunk {
            ($i:expr) => {{
                let chunk = StreamChunk::new(
                    vec![Op::Insert],
                    vec![column_nonnull!(I64Array, [$i])],
                    None,
                );
                source.write_chunk(chunk).await?;
            }};
        }

        write_chunk!(0);

        reader.open().await?;

        macro_rules! check_next_chunk {
            ($i: expr) => {
                assert_matches!(reader.next().await?, chunk => {
                    assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some($i)]);
                });
            }
        }

        check_next_chunk!(0);

        write_chunk!(1);
        check_next_chunk!(1);

        Ok(())
    }
}
