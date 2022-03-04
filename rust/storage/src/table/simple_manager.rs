use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use risingwave_common::array::InternalError;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{ensure, gen_error};
use risingwave_pb::plan::ColumnDesc;

use super::{ScannableTableRef, TableManager};
use crate::table::mview::MViewTable;
use crate::{dispatch_state_store, Keyspace, StateStoreImpl, TableColumnDesc};

/// Manages all tables in the storage backend.
#[derive(Debug)]
pub struct SimpleTableManager {
    // TODO: should not use `std::sync::Mutex` in async context.
    tables: Mutex<HashMap<TableId, ScannableTableRef>>,

    /// Used for `TableV2`.
    state_store: StateStoreImpl,
}

impl AsRef<dyn Any> for SimpleTableManager {
    fn as_ref(&self) -> &dyn Any {
        self as &dyn Any
    }
}

#[async_trait::async_trait]
impl TableManager for SimpleTableManager {
    // TODO: the data in StateStore should also be dropped directly/through unpin or some other way.
    async fn drop_table(&self, _table_id: &TableId) -> Result<()> {
        // let mut tables = self.lock_tables();
        // ensure!(
        //     tables.contains_key(table_id),
        //     "Table does not exist: {:?}",
        //     table_id
        // );
        // tables.remove(table_id);
        Ok(())
    }

    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: &[ColumnDesc],
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
    ) -> Result<()> {
        tracing::debug!("create materialized view: {:?}", table_id);

        let mut tables = self.lock_tables();
        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );
        let column_count = columns.len();
        ensure!(column_count > 0, "There must be more than one column in MV");
        let schema = Schema::try_from(columns)?;

        let table: ScannableTableRef = dispatch_state_store!(self.state_store(), store, {
            Arc::new(MViewTable::new(
                Keyspace::table_root(store, table_id),
                schema,
                pk_columns,
                orderings,
            ))
        });

        tables.insert(*table_id, table);
        Ok(())
    }

    fn register_associated_materialized_view(
        &self,
        associated_table_id: &TableId,
        mview_id: &TableId,
    ) -> Result<ScannableTableRef> {
        tracing::debug!(
            "register associated materialized view: associated_table_id={:?}, mview_id={:?}",
            associated_table_id,
            mview_id
        );

        let mut tables = self.lock_tables();
        let table = tables
            .get(associated_table_id)
            .ok_or_else(|| {
                // TODO: make this "panic"
                ErrorCode::CatalogError(
                    anyhow::anyhow!(
                        "associated table {:?} for table_v2 {:?} not exist",
                        associated_table_id,
                        mview_id
                    )
                    .into(),
                )
            })?
            .clone();

        // Simply associate the mview id to the table
        tables.insert(*mview_id, table.clone());
        Ok(table)
    }

    async fn drop_materialized_view(&self, table_id: &TableId) -> Result<()> {
        self.drop_table(table_id).await
    }
}

impl SimpleTableManager {
    pub fn new(state_store: StateStoreImpl) -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
            state_store,
        }
    }

    pub fn with_in_memory_store() -> Self {
        Self::new(StateStoreImpl::shared_in_memory_store())
    }

    pub fn lock_tables(&self) -> MutexGuard<HashMap<TableId, ScannableTableRef>> {
        self.tables.lock().unwrap()
    }

    pub fn state_store(&self) -> StateStoreImpl {
        self.state_store.clone()
    }
}
