use std::any::Any;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use risingwave_common::catalog::TableId;

use super::{ScannableTableRef, TableManager};
use crate::StateStoreImpl;

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
impl TableManager for SimpleTableManager {}

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
