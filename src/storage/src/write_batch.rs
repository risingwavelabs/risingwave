// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;

use crate::error::StorageResult;
use crate::hummock::HummockError;
use crate::storage_value::StorageValue;
use crate::{Keyspace, StateStore};

/// [`WriteBatch`] wraps a list of key-value pairs and an associated [`StateStore`].
pub struct WriteBatch<S: StateStore> {
    store: S,

    batch: Vec<(Bytes, StorageValue)>,
}

impl<S> WriteBatch<S>
where
    S: StateStore,
{
    /// Constructs a new, empty [`WriteBatch`] with the given `store`.
    pub fn new(store: S) -> Self {
        Self {
            store,
            batch: Vec::new(),
        }
    }

    /// Constructs a new, empty [`WriteBatch`] with the given `store` and specified capacity.
    pub fn with_capacity(store: S, capacity: usize) -> Self {
        Self {
            store,
            batch: Vec::with_capacity(capacity),
        }
    }

    /// Reserves capacity for at least `additional` more key-value pairs to be inserted in the
    /// batch.
    pub fn reserve(&mut self, additional: usize) {
        self.batch.reserve(additional);
    }

    /// Returns the number of key-value pairs in the batch.
    pub fn len(&self) -> usize {
        self.batch.len()
    }

    /// Preprocesses the batch to make it sorted. It returns `false` if duplicate keys are found.
    pub fn preprocess(&mut self) -> StorageResult<()> {
        if self.is_empty() {
            return Ok(());
        }

        let original_length = self.batch.len();
        self.batch.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        self.batch.dedup_by(|(k1, _), (k2, _)| k1 == k2);

        if original_length == self.batch.len() {
            Ok(())
        } else {
            Err(HummockError::invalid_write_batch().into())
        }
    }

    /// Returns `true` if the batch contains no key-value pairs.
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Ingests this batch into the associated state store.
    pub async fn ingest(mut self, epoch: u64) -> StorageResult<()> {
        self.preprocess()?;
        self.store.ingest_batch(self.batch, epoch).await?;
        Ok(())
    }

    /// Ingests this batch into the associated state store, without being persisted.
    pub async fn replicate_remote(mut self, epoch: u64) -> StorageResult<()> {
        self.preprocess()?;
        self.store.replicate_batch(self.batch, epoch).await?;
        Ok(())
    }

    /// Creates a [`KeySpaceWriteBatch`] with the given `prefix`, which automatically prepends the
    /// prefix when writing.
    pub fn prefixify<'a>(&'a mut self, keyspace: &'a Keyspace<S>) -> KeySpaceWriteBatch<'a, S> {
        KeySpaceWriteBatch {
            keyspace,
            global: self,
        }
    }
}

/// [`KeySpaceWriteBatch`] attaches a [`Keyspace`] to a mutable reference of global [`WriteBatch`],
/// which automatically prepends the keyspace prefix when writing.
pub struct KeySpaceWriteBatch<'a, S: StateStore> {
    keyspace: &'a Keyspace<S>,

    global: &'a mut WriteBatch<S>,
}

impl<'a, S: StateStore> KeySpaceWriteBatch<'a, S> {
    /// Pushes `key` and `value` into the `WriteBatch`.
    /// If `key` is valid, it will be prefixed with `keyspace` key.
    /// Otherwise, only `keyspace` key is pushed.
    fn do_push(&mut self, key: Option<&[u8]>, value: StorageValue) {
        let key = match key {
            Some(key) => self.keyspace.prefixed_key(key),
            None => self.keyspace.key().to_vec(),
        }
        .into();
        self.global.batch.push((key, value));
    }

    /// Treats the keyspace as a single key, and put a value.
    pub fn put_single(&mut self, value: StorageValue) {
        self.do_push(None, value);
    }

    /// Treats the keyspace as a single key, and delete a value.
    pub fn delete_single(&mut self) {
        self.do_push(None, StorageValue::new_default_delete());
    }

    /// Puts a value, with the key prepended by the prefix of `keyspace`, like `[prefix | given
    /// key]`.
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: StorageValue) {
        self.do_push(Some(key.as_ref()), value);
    }

    /// Deletes a value, with the key prepended by the prefix of `keyspace`, like `[prefix | given
    /// key]`.
    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        self.do_push(Some(key.as_ref()), StorageValue::new_default_delete());
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::WriteBatch;
    use crate::memory::MemoryStateStore;
    use crate::storage_value::StorageValue;
    use crate::Keyspace;

    #[tokio::test]
    async fn test_invalid_write_batch() {
        let state_store = MemoryStateStore::new();
        let mut write_batch = WriteBatch::new(state_store.clone());
        let key_space = Keyspace::executor_root(state_store, 0x118);

        assert!(write_batch.is_empty());
        let mut key_space_batch = write_batch.prefixify(&key_space);
        key_space_batch.put(Bytes::from("aa"), StorageValue::new_default_put("444"));
        key_space_batch.put(Bytes::from("cc"), StorageValue::new_default_put("444"));
        key_space_batch.put(Bytes::from("bb"), StorageValue::new_default_put("444"));
        key_space_batch.delete(Bytes::from("aa"));

        write_batch
            .ingest(1)
            .await
            .expect_err("Should panic here because of duplicate key.");
    }
}
