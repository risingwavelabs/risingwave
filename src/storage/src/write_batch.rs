// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;
use risingwave_hummock_sdk::key::next_key;
use risingwave_hummock_sdk::opts::WriteOptions;

use crate::error::StorageResult;
use crate::hummock::HummockError;
use crate::storage_value::StorageValue;
use crate::store::StateStoreWrite;

/// [`WriteBatch`] wraps a list of key-value pairs and an associated [`StateStore`].
pub struct WriteBatch<'a, S: StateStoreWrite> {
    store: &'a S,

    batch: Vec<(Bytes, StorageValue)>,

    delete_ranges: Vec<(Bytes, Bytes)>,

    write_options: WriteOptions,
}

impl<'a, S: StateStoreWrite> WriteBatch<'a, S> {
    /// Constructs a new, empty [`WriteBatch`] with the given `store`.
    pub fn new(store: &'a S, write_options: WriteOptions) -> Self {
        Self {
            store,
            batch: vec![],
            delete_ranges: vec![],
            write_options,
        }
    }

    /// Constructs a new, empty [`WriteBatch`] with the given `store` and specified capacity.
    pub fn with_capacity(store: &'a S, capacity: usize, write_options: WriteOptions) -> Self {
        Self {
            store,
            batch: Vec::with_capacity(capacity),
            delete_ranges: vec![],
            write_options,
        }
    }

    /// Puts a value.
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: StorageValue) {
        self.do_push(key.as_ref(), value);
    }

    /// Deletes a value.
    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        self.do_push(key.as_ref(), StorageValue::new_delete());
    }

    /// Delete all keys starting with `prefix`.
    pub fn delete_prefix(&mut self, prefix: impl AsRef<[u8]>) {
        let start_key = Bytes::from(prefix.as_ref().to_owned());
        let end_key = Bytes::from(next_key(&start_key));
        self.delete_ranges.push((start_key, end_key));
    }

    /// Delete all keys in this range.
    pub fn delete_range(&mut self, start: impl AsRef<[u8]>, end: impl AsRef<[u8]>) {
        let start_key = Bytes::from(start.as_ref().to_owned());
        let end_key = Bytes::from(end.as_ref().to_owned());
        self.delete_ranges.push((start_key, end_key));
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
    fn preprocess(&mut self) -> StorageResult<()> {
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
        self.batch.is_empty() && self.delete_ranges.is_empty()
    }

    /// Ingests this batch into the associated state store.
    pub async fn ingest(mut self) -> StorageResult<()> {
        if !self.is_empty() {
            self.preprocess()?;
            self.store
                .ingest_batch(self.batch, self.delete_ranges, self.write_options)
                .await?;
        }
        Ok(())
    }

    /// Pushes `key` and `value` into the `WriteBatch`.
    fn do_push(&mut self, key: &[u8], value: StorageValue) {
        let key = Bytes::from(key.to_vec());
        self.batch.push((key, value));
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_hummock_sdk::opts::WriteOptions;

    use crate::memory::MemoryStateStore;
    use crate::storage_value::StorageValue;
    use crate::store::StateStoreWrite;

    #[tokio::test]
    async fn test_invalid_write_batch() {
        let state_store = MemoryStateStore::new();
        let mut batch = state_store.start_write_batch(WriteOptions {
            epoch: 1,
            table_id: Default::default(),
        });

        batch.put(Bytes::from("aa"), StorageValue::new_put("444"));
        batch.put(Bytes::from("cc"), StorageValue::new_put("444"));
        batch.put(Bytes::from("bb"), StorageValue::new_put("444"));
        batch.delete(Bytes::from("aa"));

        batch
            .ingest()
            .await
            .expect_err("Should panic here because of duplicate key.");
    }
}
