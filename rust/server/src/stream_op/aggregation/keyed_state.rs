use risingwave_common::array::Row;
use risingwave_common::error::Result;
use std::collections::HashMap;

/// A trait for keyed state store
#[async_trait::async_trait]
pub trait KeyedState: Send + Sync + 'static {
    async fn get(&self, key: &Row) -> Result<Option<&Row>>;

    async fn put(&mut self, key: Row, value: Row) -> Result<()>;

    async fn remove(&mut self, key: &Row) -> Result<()>;
}

#[derive(Default)]
pub struct KeyedStateHeap {
    state_entries: HashMap<Row, Row>,
}

impl KeyedStateHeap {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl KeyedState for KeyedStateHeap {
    async fn get(&self, key: &Row) -> Result<Option<&Row>> {
        Ok(self.state_entries.get(key))
    }

    async fn put(&mut self, key: Row, value: Row) -> Result<()> {
        self.state_entries.insert(key, value);
        Ok(())
    }

    async fn remove(&mut self, key: &Row) -> Result<()> {
        self.state_entries.remove(key);
        Ok(())
    }
}
