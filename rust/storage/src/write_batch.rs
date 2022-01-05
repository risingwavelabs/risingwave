use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::error::Result;

use crate::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};
use crate::{Keyspace, StateStore};

/// [`WriteBatch`] wrap a list of key-value pairs and an associated [`StateStore`].
pub struct WriteBatch<S: StateStore> {
    store: S,

    batch: Vec<(Bytes, Option<Bytes>)>,

    state_store_stats: Arc<StateStoreStats>,
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
            state_store_stats: DEFAULT_STATE_STORE_STATS.clone(),
        }
    }

    /// Constructs a new, empty [`WriteBatch`] with the given `store` and specified capacity.
    pub fn with_capacity(store: S, capacity: usize) -> Self {
        Self {
            store,
            batch: Vec::with_capacity(capacity),
            state_store_stats: DEFAULT_STATE_STORE_STATS.clone(),
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

    /// Returns `true` if the batch contains no key-value pairs.
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Ingest this batch into the associated state store.
    pub async fn ingest(self, epoch: u64) -> Result<()> {
        self.state_store_stats.batched_write_counts.inc();
        // TODO: is it necessary to check or preprocess these pairs?
        self.store.ingest_batch(self.batch, epoch).await
    }

    /// Create a [`LocalWriteBatch`] with the given `keyspace`, which automatically prepends the
    /// keyspace prefix when writing.
    pub fn local<'a>(&'a mut self, keyspace: &'a Keyspace<S>) -> LocalWriteBatch<'a, S> {
        LocalWriteBatch {
            keyspace,
            global: self,
        }
    }
}

/// [`LocalWriteBatch`] attaches a [`Keyspace`] to a mutable reference of global [`WriteBatch`],
/// which automatically prepends the keyspace prefix when writing.
pub struct LocalWriteBatch<'a, S: StateStore> {
    keyspace: &'a Keyspace<S>,

    global: &'a mut WriteBatch<S>,
}

impl<'a, S: StateStore> LocalWriteBatch<'a, S> {
    fn do_push(&mut self, key: Option<&[u8]>, value: Option<Bytes>) {
        let key = match key {
            Some(key) => self.keyspace.prefixed_key(key),
            None => self.keyspace.key().to_vec(),
        }
        .into();
        self.global.batch.push((key, value));
    }

    /// Treat the keyspace as a single key, and put a value.
    pub fn put_single(&mut self, value: impl Into<Bytes>) {
        self.do_push(None, Some(value.into()));
    }

    /// Treat the keyspace as a single key, and delete a value.
    pub fn delete_single(&mut self) {
        self.do_push(None, None);
    }

    /// Put a value, with the key prepended by the prefix of `keyspace`, like `[prefix | given
    /// key]`.
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl Into<Bytes>) {
        self.do_push(Some(key.as_ref()), Some(value.into()));
    }

    /// Delete a value, with the key prepended by the prefix of `keyspace`, like `[prefix | given
    /// key]`.
    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        self.do_push(Some(key.as_ref()), None);
    }
}
