pub mod mview;
pub mod row_table;
mod simple_manager;

use std::any::Any;
use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan::ColumnDesc;
pub use simple_manager::*;

use crate::TableColumnDesc;

// deprecated and to be removed
// deprecated and to be removed
/// deprecated and to be removed
/// `TableManager` is an abstraction of managing a collection of
/// tables. The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
#[async_trait::async_trait]
pub trait TableManager: Debug + Sync + Send + AsRef<dyn Any> {}

#[async_trait::async_trait]
pub trait TableIter: Send {
    async fn next(&mut self) -> Result<Option<Row>>;
}

#[async_trait::async_trait]
// deprecated and to be removed
// deprecated and to be removed
/// deprecated and to be removed
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

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send>;

    fn schema(&self) -> Cow<Schema>;

    fn column_descs(&self) -> Cow<[TableColumnDesc]>;

    /// Indicates whether this table is backed with a shared storage. The behavior of distributed
    /// scanning differs according to this property.
    fn is_shared_storage(&self) -> bool;
}
/// Reference of a `TableManager`.
pub type TableManagerRef = Arc<dyn TableManager>;
pub type ScannableTableRef = Arc<dyn ScannableTable>;
pub type TableIterRef = Box<dyn TableIter>;
