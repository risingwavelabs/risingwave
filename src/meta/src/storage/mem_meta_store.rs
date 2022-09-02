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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use super::{
    ColumnFamily, Key, MetaStore, MetaStoreError, MetaStoreResult, Snapshot, Transaction, Value,
};

pub struct MemSnapshot(OwnedRwLockReadGuard<MemStoreInner>);

/// [`MetaStore`] implemented in memory.
///
/// Note: Don't use in production.
#[derive(Clone, Debug, Default)]
pub struct MemStore {
    inner: Arc<RwLock<MemStoreInner>>,
}

/// The first level is the cf name, the second level is the key.
#[derive(Clone, Debug, Default)]
struct MemStoreInner(HashMap<ColumnFamily, BTreeMap<Key, Value>>);

impl MemStoreInner {
    #[inline(always)]
    fn cf_ref(&self, cf: &str) -> Option<&BTreeMap<Key, Value>> {
        self.0.get(cf)
    }

    #[inline(always)]
    fn cf_mut(&mut self, cf: &str) -> &mut BTreeMap<Key, Value> {
        self.0.entry(cf.to_string()).or_default()
    }
}

#[async_trait]
impl Snapshot for MemSnapshot {
    #[inline(always)]
    async fn list_cf(&self, cf: &str) -> MetaStoreResult<Vec<Value>> {
        Ok(match self.0.cf_ref(cf) {
            Some(cf) => cf.values().cloned().collect(),
            None => vec![],
        })
    }

    #[inline(always)]
    async fn get_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<Value> {
        self.0
            .cf_ref(cf)
            .and_then(|cf| cf.get(key).cloned())
            .ok_or_else(|| MetaStoreError::ItemNotFound(hex::encode(key)))
    }
}

impl MemStore {
    /// Get a global shared in-memory store.
    pub fn shared() -> Self {
        lazy_static::lazy_static! {
            static ref STORE: MemStore = MemStore::default();
        }
        STORE.clone()
    }
}

#[async_trait]
impl MetaStore for MemStore {
    type Snapshot = MemSnapshot;

    async fn snapshot(&self) -> Self::Snapshot {
        let guard = self.inner.clone().read_owned().await;
        MemSnapshot(guard)
    }

    async fn put_cf(&self, cf: &str, key: Key, value: Value) -> MetaStoreResult<()> {
        let mut inner = self.inner.write().await;
        inner.cf_mut(cf).insert(key, value);
        Ok(())
    }

    async fn delete_cf(&self, cf: &str, key: &[u8]) -> MetaStoreResult<()> {
        let mut inner = self.inner.write().await;
        inner.cf_mut(cf).remove(key);
        Ok(())
    }

    async fn txn(&self, txn: Transaction) -> MetaStoreResult<()> {
        use super::Operation::*;
        use super::Precondition::*;

        let mut inner = self.inner.write().await;
        let (conds, ops) = txn.into_parts();

        for cond in conds {
            match cond {
                KeyExists { cf, key } => {
                    if !inner
                        .cf_ref(cf.as_str())
                        .map(|cf| cf.contains_key(&key[..]))
                        .unwrap_or(false)
                    {
                        return Err(MetaStoreError::TransactionAbort());
                    }
                }
                KeyEqual { cf, key, value } => {
                    let v = inner
                        .cf_ref(cf.as_str())
                        .map(|cf| cf.get(key.as_slice()))
                        .unwrap_or(None);
                    if !v.map_or(false, |v| v.eq(&value)) {
                        return Err(MetaStoreError::TransactionAbort());
                    }
                }
            }
        }

        for op in ops {
            match op {
                Put { cf, key, value } => {
                    inner.cf_mut(cf.as_str()).insert(key, value);
                }
                Delete { cf, key } => {
                    inner.cf_mut(cf.as_str()).remove(&key);
                }
            }
        }
        Ok(())
    }
}
