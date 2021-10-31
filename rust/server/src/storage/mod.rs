mod mem;
pub use mem::*;
mod row_table;
pub use row_table::*;

use crate::stream_op::StreamChunk;
use futures::channel::mpsc;
use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_proto::plan::ColumnDesc;
use std::sync::Arc;

/// Table is an abstraction of the collection of columns and rows.
pub trait Table: Sync + Send {
    /// Append an entry to the table.
    fn append(&self, data: DataChunk) -> Result<usize>;

    /// Create a stream from the table.
    /// The stream includes all existing rows in tables and changed rows in future.
    /// Note that only one stream is allowed for a table.
    /// Existing stream should be reused if the user create
    /// more than one materialized view from a table.
    fn create_stream(&self) -> Result<mpsc::UnboundedReceiver<StreamChunk>>;

    /// Get data from the table.
    fn get_data(&self) -> Result<Vec<DataChunkRef>>;

    /// Get the column ids of the table.
    fn get_column_ids(&self) -> Result<Arc<Vec<i32>>>;

    /// Get the indices of the specific column.
    fn index_of_column_id(&self, column_id: i32) -> Result<usize>;
}

/// `TableManager` is an abstraction of managing a collection of tables.
/// The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
pub trait TableManager: Sync + Send {
    /// Create a specific table.
    fn create_table(&self, table_id: &TableId, columns: &[ColumnDesc]) -> Result<()>;

    /// Get a specific table.
    fn get_table(&self, table_id: &TableId) -> Result<SimpleTableRef>;

    /// Drop a specific table.
    fn drop_table(&self, table_id: &TableId) -> Result<()>;

    /// Create materialized view.
    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: Vec<ColumnDesc>,
        pk_columns: Vec<usize>,
    ) -> Result<()>;
}

/// Reference of a `TableManager`.
pub type TableManagerRef = Arc<dyn TableManager>;
