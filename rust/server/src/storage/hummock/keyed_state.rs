use std::collections::HashMap;

use async_trait::async_trait;
use risingwave_common::array::Row;
use risingwave_common::error::{ErrorCode, Result};

use super::{HummockStorage, HummockValue};
use crate::stream_op::{KeyedState, SchemaedSerializable};

/// [`KeyedState`] for `Hummock` storage engine.
pub struct HummockKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: SchemaedSerializable,
{
    key_schema: K,
    value_schema: V,
    mem_table: HashMap<K::Output, HummockValue<V::Output>>,
    storage: HummockStorage,
}

impl<K, V> HummockKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: SchemaedSerializable,
{
    pub fn new(key_schema: K, value_schema: V, storage: HummockStorage) -> HummockKeyedState<K, V> {
        Self {
            storage,
            mem_table: HashMap::new(),
            key_schema,
            value_schema,
        }
    }
}

#[async_trait]
impl<K, V> KeyedState<K, V> for HummockKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: SchemaedSerializable,
{
    async fn get(&self, key: &Row) -> Result<Option<V::Output>> {
        match self.mem_table.get(key) {
            Some(HummockValue::Put(value)) => Ok(Some(value.clone())),
            Some(HummockValue::Delete) => Ok(None),
            // fetch value from the storage engine
            _ => todo!(),
        }
    }

    fn put(&mut self, key: Row, value: V::Output) {
        self.mem_table.insert(key, HummockValue::Put(value));
    }

    fn delete(&mut self, key: &Row) {
        self.mem_table.insert(key.clone(), HummockValue::Delete);
    }

    async fn flush(&mut self) -> Result<()> {
        self.storage
            .ingest_kv_pairs(self.mem_table.drain().map(|(key, value)| {
                (
                    self.key_schema.schemaed_serialize(&key),
                    match value {
                        HummockValue::Put(value) => {
                            HummockValue::Put(self.value_schema.schemaed_serialize(&value))
                        }
                        HummockValue::Delete => HummockValue::Delete,
                    },
                )
            }))
            .await
            .map_err(|err| ErrorCode::InternalError(err.to_string()))?;
        Ok(())
    }
}
