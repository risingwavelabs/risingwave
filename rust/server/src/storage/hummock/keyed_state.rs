use std::collections::HashMap;

use async_trait::async_trait;
use risingwave_common::array::Row;
use risingwave_common::error::{ErrorCode, Result};

use super::{HummockStorage, HummockValue};
use crate::stream_op::{CellBasedSchemaedSerializable, KeyedState, SchemaedSerializable};

/// [`KeyedState`] for `Hummock` storage engine.
pub struct HummockKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: CellBasedSchemaedSerializable,
{
    key_schema: K,
    value_schema: V,
    mem_table: HashMap<K::Output, HummockValue<V::Output>>,
    storage: HummockStorage,
}

impl<K, V> HummockKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: CellBasedSchemaedSerializable,
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
    V: CellBasedSchemaedSerializable,
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

    // TODO(MrCroxx): value now support cell-based serialize/deserialize.
    // Only dirty cell-based state needs to be flushed.
    async fn flush(&mut self) -> Result<()> {
        self.storage
            .write_batch(self.mem_table.drain().map(|(key, value)| {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use crate::storage::hummock::HummockOptions;
    use crate::storage::InMemObjectStore;
    use crate::stream_op::RowSerializer;
    use risingwave_common::catalog::Schema;
    use risingwave_common::types::ScalarImpl::Int64;

    fn generate_hummock_keyed_state() -> HummockKeyedState<RowSerializer, RowSerializer> {
        let schema_new = || RowSerializer::new(Schema::default());
        HummockKeyedState::new(
            schema_new(),
            schema_new(),
            HummockStorage::new(Arc::new(InMemObjectStore::new()), HummockOptions::default()),
        )
    }

    #[tokio::test]
    #[should_panic]
    async fn test_hummock_keyed_state_absent_val() {
        let hummock_keyed_state = generate_hummock_keyed_state();
        hummock_keyed_state.get(&Row(vec![])).await.unwrap();
    }

    #[tokio::test]
    async fn test_hummock_keyed_state_present_val() {
        let mut hummock_keyed_state = generate_hummock_keyed_state();
        let row_val = |x| Row(vec![Some(Int64(x))]);
        let empty_row_key = Row(vec![]);
        let some_row_key = Row(vec![Some(Int64(101))]);
        hummock_keyed_state.put(empty_row_key.clone(), row_val(-1));
        hummock_keyed_state.put(empty_row_key.clone(), row_val(1));
        hummock_keyed_state.put(some_row_key.clone(), row_val(2));
        assert_eq!(
            hummock_keyed_state.get(&empty_row_key).await.unwrap(),
            Some(row_val(1))
        );
        hummock_keyed_state.delete(&empty_row_key);
        assert_eq!(hummock_keyed_state.get(&empty_row_key).await.unwrap(), None);
        assert_eq!(
            hummock_keyed_state.get(&some_row_key).await.unwrap(),
            Some(row_val(2))
        );
        hummock_keyed_state.put(empty_row_key.clone(), row_val(3));
        assert_eq!(
            hummock_keyed_state.get(&empty_row_key).await.unwrap(),
            Some(row_val(3))
        );
    }
}
