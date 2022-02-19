pub mod mview;
mod simple_manager;
pub mod test;

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, DataChunkRef, Row};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan::ColumnDesc;
pub use simple_manager::*;

use crate::TableColumnDesc;

// TODO: should not be ref.
pub type DataChunks = Vec<DataChunkRef>;

#[async_trait::async_trait]
/// `TableManager` is an abstraction of managing a collection of tables.
/// The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
pub trait TableManager: Sync + Send + AsRef<dyn Any> {
    /// Create a specific table.
    async fn create_table_v2(
        &self,
        table_id: &TableId,
        table_columns: Vec<TableColumnDesc>,
    ) -> Result<ScannableTableRef>;

    /// Get a specific table.
    fn get_table(&self, table_id: &TableId) -> Result<ScannableTableRef>;

    /// Drop a specific table.
    async fn drop_table(&self, table_id: &TableId) -> Result<()>;

    /// Create materialized view.
    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: &[ColumnDesc],
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
    ) -> Result<()>;

    /// Create materialized view associated to table v2
    fn register_associated_materialized_view(
        &self,
        associated_table_id: &TableId,
        mview_id: &TableId,
    ) -> Result<ScannableTableRef>;

    /// Drop materialized view.
    async fn drop_materialized_view(&self, table_id: &TableId) -> Result<()>;
}

#[async_trait::async_trait]
pub trait TableIter: Send {
    async fn next(&mut self) -> Result<Option<Row>>;
}

#[async_trait::async_trait]
pub trait ScannableTable: Sync + Send + Any + core::fmt::Debug {
    /// Open and return an iterator.
    async fn iter(&self, epoch: u64) -> Result<TableIterRef>;

    /// Collect data chunk with the target `chunk_size` from the given `iter`, projected on
    /// `indices`. If there's no more data, return `None`.
    async fn collect_from_iter(
        &self,
        iter: &mut TableIterRef,
        indices: &[usize],
        chunk_size: Option<usize>,
    ) -> Result<Option<DataChunk>> {
        let schema = self.schema();
        let mut builders = indices
            .iter()
            .map(|i| {
                schema.fields()[*i]
                    .data_type()
                    .create_array_builder(chunk_size.unwrap_or(0))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut row_count = 0;
        for _ in 0..chunk_size.unwrap_or(usize::MAX) {
            match iter.next().await? {
                Some(row) => {
                    for (&index, builder) in indices.iter().zip_eq(builders.iter_mut()) {
                        builder.append_datum(&row.0[index])?;
                    }
                    row_count += 1;
                }
                None => break,
            }
        }

        let chunk = if indices.is_empty() {
            // Generate some dummy data to ensure a correct cardinality, which might be used by
            // count(*).
            DataChunk::new_dummy(row_count)
        } else {
            let columns: Vec<Column> = builders
                .into_iter()
                .map(|builder| builder.finish().map(|a| Column::new(Arc::new(a))))
                .try_collect()?;
            DataChunk::builder().columns(columns).build()
        };

        if chunk.cardinality() == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }

    /// Scan data of specified column ids
    ///
    /// In future, it will accept `predicates` for interested filtering conditions.
    ///
    /// This default implementation using iterator is not efficient, which project `column_ids` row
    /// by row. If the underlying storage is a column store, we may implement this function
    /// specifically.
    async fn get_data_by_columns(&self, column_ids: &[i32]) -> Result<Option<DataChunks>> {
        let indices = self.column_indices(column_ids);
        let mut iter = self.iter(u64::MAX).await?;
        let chunks = self
            .collect_from_iter(&mut iter, &indices, None)
            .await?
            .map(|chunk| vec![chunk.into()]);

        Ok(chunks)
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send>;

    fn schema(&self) -> Cow<Schema>;

    fn column_descs(&self) -> Cow<[TableColumnDesc]>;

    /// Get column indices for given `column_ids`.
    fn column_indices(&self, column_ids: &[i32]) -> Vec<usize> {
        column_ids
            .iter()
            .map(|id| {
                self.column_descs()
                    .iter()
                    .position(|c| c.column_id == *id)
                    .expect("column id not exists")
            })
            .collect()
    }

    /// Indicates whether this table is backed with a shared storage. The behavior of distributed
    /// scanning differs according to this property.
    fn is_shared_storage(&self) -> bool;
}

/// Reference of a `TableManager`.
pub type TableManagerRef = Arc<dyn TableManager>;
pub type ScannableTableRef = Arc<dyn ScannableTable>;
pub type TableIterRef = Box<dyn TableIter>;
