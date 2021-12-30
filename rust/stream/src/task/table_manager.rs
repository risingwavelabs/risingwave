use std::sync::Arc;

use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{ensure, gen_error};
use risingwave_pb::plan::ColumnDesc;
use risingwave_storage::table::{ScannableTableRef, SimpleTableManager, TableManager};
use risingwave_storage::Keyspace;

use crate::dispatch_state_store;
use crate::executor::MViewTable;
use crate::task::StateStoreImpl;

#[async_trait::async_trait]
/// `TableManager` is an abstraction of managing a collection of tables.
/// The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
pub trait StreamTableManager: TableManager {
    /// Create materialized view.
    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: &[ColumnDesc],
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
        state_store: StateStoreImpl,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl StreamTableManager for SimpleTableManager {
    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: &[ColumnDesc],
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
        state_store: StateStoreImpl,
    ) -> Result<()> {
        let mut tables = self.get_tables();
        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );
        let column_count = columns.len();
        ensure!(column_count > 0, "There must be more than one column in MV");
        let schema = Schema::try_from(columns)?;

        let table: ScannableTableRef = dispatch_state_store!(state_store, store, {
            Arc::new(MViewTable::new(
                Keyspace::table_root(store, table_id),
                schema,
                pk_columns,
                orderings,
            ))
        });

        tables.insert(table_id.clone(), table);
        Ok(())
    }
}

pub type StreamTableManagerRef = Arc<dyn StreamTableManager>;
