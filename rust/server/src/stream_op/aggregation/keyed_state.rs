use async_trait::async_trait;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use std::collections::HashMap;

/// Transform a value to and from a `Vec<u8>`.
pub trait SchemaedSerializable: Send + Sync + 'static {
    type Output: Clone + Send + Sync + 'static;

    /// Serialize a value to `Vec<u8>`.
    fn schemaed_serialize(&self, data: &Self::Output) -> Vec<u8>;

    /// Deserialize a value from `&[u8]`.
    fn schemaed_deserialize(&self, data: &[u8]) -> Self::Output;
}

/// Transform a cell-based value to and from a `Vec<u8>`
/// based on the schema of each cell.
pub trait CellBasedSchemaedSerializable: Send + Sync + 'static {
    type Output: Clone + Send + Sync + std::ops::Index<usize> + 'static;

    /// Count of cells.
    fn cells(&self) -> usize;

    /// Serialize a cell of value to `Vec<u8>`.
    fn cell_based_schemaed_serialize(&self, data: &Self::Output, cell_idx: usize) -> Vec<u8>;

    /// Deserialize a cell of value from `Vec<u8>`.
    fn cell_based_schemaed_deserialize(
        &self,
        data: &[u8],
        cell_idx: usize,
    ) -> <Self::Output as std::ops::Index<usize>>::Output;

    /// Serialize the full value to `Vec<u8>`.
    fn schemaed_serialize(&self, data: &Self::Output) -> Vec<u8>;

    /// Deserialize the full value from `&[u8]`.
    fn schemaed_deserialize(&self, data: &[u8]) -> Self::Output;
}

/// A trait for keyed state store
#[async_trait]
pub trait KeyedState<K, V>: Send + Sync + 'static
where
    K: SchemaedSerializable<Output = Row>,
    V: CellBasedSchemaedSerializable,
{
    /// Put a key-value pair to the keyed states. This operation is in-memory and should always
    /// succeed.
    fn put(&mut self, key: Row, value: V::Output);

    /// Delete a key from the states. This operation is in-memory and should always succeed.
    fn delete(&mut self, key: &Row);

    /// Get a corresponding row from the keyed states.
    ///
    /// TODO: optimize this function later. If a value is buffered in memory, we do not need
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
    V: CellBasedSchemaedSerializable,
{
    _key_schema: K,
    _value_schema: V,
    state_entries: HashMap<K::Output, V::Output>,
}

impl<K, V> InMemoryKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: CellBasedSchemaedSerializable,
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
    V: CellBasedSchemaedSerializable,
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

#[derive(Default)]
pub struct SerializedKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: CellBasedSchemaedSerializable,
{
    key_schema: K,
    value_schema: V,
    state_entries: HashMap<Vec<u8>, Vec<u8>>,
}

impl<K, V> SerializedKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: CellBasedSchemaedSerializable,
{
    pub fn new(key_schema: K, value_schema: V) -> SerializedKeyedState<K, V> {
        Self {
            state_entries: HashMap::new(),
            key_schema,
            value_schema,
        }
    }
}

#[async_trait]
impl<K, V> KeyedState<K, V> for SerializedKeyedState<K, V>
where
    K: SchemaedSerializable<Output = Row>,
    V: CellBasedSchemaedSerializable,
{
    async fn get(&self, key: &Row) -> Result<Option<V::Output>> {
        Ok(self
            .state_entries
            .get(&self.key_schema.schemaed_serialize(key))
            .map(|x| self.value_schema.schemaed_deserialize(x)))
    }

    fn put(&mut self, key: Row, value: V::Output) {
        self.state_entries.insert(
            self.key_schema.schemaed_serialize(&key),
            self.value_schema.schemaed_serialize(&value),
        );
    }

    fn delete(&mut self, key: &Row) {
        self.state_entries
            .remove(&self.key_schema.schemaed_serialize(key));
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
