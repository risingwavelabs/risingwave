pub mod mview;
mod simple_manager;
use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan::ColumnDesc;
pub use simple_manager::*;

use crate::bummock::BummockResult;
use crate::TableColumnDesc;

#[async_trait::async_trait]
/// `TableManager` is an abstraction of managing a collection of tables.
/// The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
pub trait TableManager: Sync + Send + AsRef<dyn Any> {
    /// Create a specific table.
    async fn create_table(
        &self,
        table_id: &TableId,
        table_columns: Vec<TableColumnDesc>,
    ) -> Result<ScannableTableRef>;

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
    async fn iter(&self) -> Result<TableIterRef>;

    /// Scan data of specified column ids
    ///
    /// In future, it will accept `predicates` for interested filtering conditions.
    ///
    /// This default implementation using iterator is not efficient, which project `column_ids` row
    /// by row. If the underlying storage is a column store, we may implement this function
    /// specifically.
    async fn get_data_by_columns(&self, column_ids: &[i32]) -> Result<BummockResult> {
        let indices = column_ids
            .iter()
            .map(|id| {
                self.column_descs()
                    .iter()
                    .position(|c| c.column_id == *id)
                    .expect("column id not exists")
            })
            .collect_vec();

        let mut iter = self.iter().await?;

        let schema = self.schema();
        let mut builders = indices
            .iter()
            .map(|i| schema.fields()[*i].data_type().create_array_builder(0))
            .collect::<Result<Vec<_>>>()?;

        while let Some(row) = iter.next().await? {
            for (&index, builder) in indices.iter().zip_eq(builders.iter_mut()) {
                builder.append_datum(&row.0[index])?;
            }
        }

        let columns: Vec<Column> = builders
            .into_iter()
            .map(|builder| builder.finish().map(|a| Column::new(Arc::new(a))))
            .try_collect()?;
        let chunk = DataChunk::builder().columns(columns).build();

        Ok(BummockResult::Data(vec![Arc::new(chunk)]))
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send>;

    fn schema(&self) -> Cow<Schema>;

    fn column_descs(&self) -> Cow<[TableColumnDesc]>;
}

/// Reference of a `TableManager`.
pub type TableManagerRef = Arc<dyn TableManager>;
pub type ScannableTableRef = Arc<dyn ScannableTable>;
pub type TableIterRef = Box<dyn TableIter>;
