use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::Result;
use risingwave_storage::table::ScannableTableRef;
use risingwave_storage::TableColumnDesc;
use tokio::sync::{mpsc, RwLock};

use crate::{BatchSourceReader, Source, SourceWriter, StreamSourceReader};

#[derive(Debug)]
struct TableSourceV2Core {
    _table: ScannableTableRef,

    /// Send changes to the associated materialize task.
    changes_tx: mpsc::UnboundedSender<StreamChunk>,

    /// The receiver of the above channel.
    /// This field will be taken once a `StreamReader` is created, i.e., the associated materialize
    /// task starts.
    changes_rx: Option<mpsc::UnboundedReceiver<StreamChunk>>,
}

/// [`TableSourceV2`] is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows will be send to the associated "materialize" streaming task, then be written to the
/// state store. Therefore, [`TableSourceV2`] can be simply be treated as a channel without side
/// effects.
#[derive(Debug)]
pub struct TableSourceV2 {
    core: Arc<RwLock<TableSourceV2Core>>,

    /// All columns in this table.
    column_descs: Vec<TableColumnDesc>,

    /// Curren allocated row id.
    next_row_id: AtomicUsize,
}

impl TableSourceV2 {
    pub fn new(table: ScannableTableRef) -> Self {
        let column_descs = table.column_descs().into_owned();

        let (tx, rx) = mpsc::unbounded_channel();
        let core = TableSourceV2Core {
            _table: table,
            changes_tx: tx,
            changes_rx: Some(rx),
        };

        Self {
            core: Arc::new(RwLock::new(core)),
            column_descs,
            next_row_id: 0.into(),
        }
    }

    pub fn next_row_id(&self) -> usize {
        self.next_row_id.fetch_add(1, Ordering::SeqCst)
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
        todo!()
    }

    async fn next(&mut self) -> Result<Option<risingwave_common::array::DataChunk>> {
        todo!()
    }

    async fn close(&mut self) -> Result<()> {
        todo!()
    }
}

/// [`TableV2StreamReader`] reads changes from a certain table continuously.
/// This struct should be only used for associated materialize task, thus the reader should be
/// created only once. Further streaming task relying on this table source should follow the
/// structure of "MView on MView".
#[derive(Debug)]
pub struct TableV2StreamReader {
    core: Arc<RwLock<TableSourceV2Core>>,

    /// The receiver of the changes channel. This field is `Some` only if the reader has been
    /// `open`ed.
    rx: Option<mpsc::UnboundedReceiver<StreamChunk>>,

    /// Mappings from the source column to the column to be read.
    column_indices: Vec<usize>,
}

#[async_trait]
impl StreamSourceReader for TableV2StreamReader {
    async fn open(&mut self) -> Result<()> {
        let mut core = self.core.write().await;
        let rx = core.changes_rx.take().expect("can only open reader once");
        self.rx = Some(rx);
        Ok(())
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        let chunk = match self.rx.as_mut().expect("reader not open").recv().await {
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

/// [`TableV2Writer`] is for writing data into table. Changes writen here will be simply passed to
/// the associated streaming task via channel, and then be materialized to storage there.
#[derive(Debug)]
pub struct TableV2Writer {
    core: Arc<RwLock<TableSourceV2Core>>,
}

#[async_trait]
impl SourceWriter for TableV2Writer {
    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let core = self.core.read().await;
        core.changes_tx
            .send(chunk)
            .expect("write to table v2 failed");
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Source for TableSourceV2 {
    type ReaderContext = TableV2ReaderContext;
    type BatchReader = TableV2BatchReader;
    type StreamReader = TableV2StreamReader;
    type Writer = TableV2Writer;

    fn batch_reader(
        &self,
        _context: Self::ReaderContext,
        _column_ids: Vec<i32>,
    ) -> Result<Self::BatchReader> {
        unimplemented!("currently no one use this")
    }

    fn stream_reader(
        &self,
        _context: Self::ReaderContext,
        column_ids: Vec<i32>,
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

        Ok(TableV2StreamReader {
            core: self.core.clone(),
            rx: None,
            column_indices,
        })
    }

    fn create_writer(&self) -> Result<Self::Writer> {
        Ok(TableV2Writer {
            core: self.core.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array, Op};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataTypeKind;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::mview::MViewTable;
    use risingwave_storage::Keyspace;

    use super::*;

    fn new_source() -> TableSourceV2 {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::table_root(store, &Default::default());
        let table = Arc::new(MViewTable::new_batch(
            keyspace,
            vec![TableColumnDesc::new_without_name(0, DataTypeKind::Int64)],
        ));

        TableSourceV2::new(table)
    }

    #[tokio::test]
    async fn test_table_source_v2() -> Result<()> {
        let source = new_source();
        let mut writer = source.create_writer()?;

        macro_rules! write_chunk {
            ($i:expr) => {{
                let chunk = StreamChunk::new(
                    vec![Op::Insert],
                    vec![column_nonnull!(I64Array, [$i])],
                    None,
                );
                writer.write(chunk).await?;
            }};
        }

        write_chunk!(0);

        let mut reader = source.stream_reader(TableV2ReaderContext, vec![0])?;
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

    #[should_panic]
    #[tokio::test]
    async fn test_only_one_reader() {
        let source = new_source();
        let mut reader_1 = source.stream_reader(TableV2ReaderContext, vec![0]).unwrap();
        reader_1.open().await.unwrap();
        let mut reader_2 = source.stream_reader(TableV2ReaderContext, vec![0]).unwrap();
        reader_2.open().await.unwrap();
    }
}
