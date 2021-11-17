use async_trait::async_trait;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use std::collections::HashMap;

/// Transform a value to and from a `Vec<u8>`.
pub trait SchemaedSerializable: Send + Sync + 'static {
    type Output: Clone + Send + Sync + 'static;

    /// Serialize a value to `Vec<u8>`.
    fn schemaed_serialize(data: &Self::Output) -> Vec<u8>;

    /// Deserialize a value from `&[u8]`.
    fn schemaed_deserialize(data: &[u8]) -> Self::Output;
}

/// A trait for keyed state store
#[async_trait]
pub trait KeyedState<K, V>: Send + 'static
where
    K: SchemaedSerializable<Output = Row>,
    V: SchemaedSerializable,
{
    /// Put a key-value pair to the keyed states. This operation is in-memory and should always
    /// succeed.
    fn put(&mut self, key: Row, value: V::Output);

    /// Delete a key from the states. This operation is in-memory and should always succeed.
    fn delete(&mut self, key: &Row);

    /// Get a corresponding row from the keyed states.
    ///
    /// TODO: optimize this function later. If a value is bufferred in memory, we do not need
    /// to call an async function to get it. We could directly fetch it without an async function.
    /// There are two ways to do this: use `async_trait_static` that avoids the `Box<dyn Future>`
    /// overhead, or add a new function called `try_get_in_memory`.
    async fn get(&self, key: &Row) -> Result<Option<V::Output>>;

    /// Flush all in-memory operations to persistent storage, e.g., RocksDB, S3.
    async fn flush(&mut self) -> Result<()>;
}

#[derive(Default)]
pub struct InMemoryKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: SchemaedSerializable,
{
    _key_schema: K,
    _value_schema: V,
    state_entries: HashMap<K::Output, V::Output>,
}

impl<K, V> InMemoryKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: SchemaedSerializable,
{
    pub fn new(key_schema: K, value_schema: V) -> InMemoryKeyedState<K, V> {
        Self {
            state_entries: HashMap::new(),
            // In-memory state won't need to serialize / deserialize the keys and values, so the
            // schemas are simply not used.
            _key_schema: key_schema,
            _value_schema: value_schema,
        }
    }
}

#[async_trait]
impl<K, V> KeyedState<K, V> for InMemoryKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: SchemaedSerializable,
{
    async fn get(&self, key: &Row) -> Result<Option<V::Output>> {
        Ok(self.state_entries.get(key).cloned())
    }

    fn put(&mut self, key: Row, value: V::Output) {
        self.state_entries.insert(key, value);
    }

    fn delete(&mut self, key: &Row) {
        self.state_entries.remove(key);
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
