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

mod catalog;
mod catalog_v2;
mod cluster;
mod hash_mapping;
mod stream;

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use async_trait::async_trait;
pub use catalog::*;
pub use catalog_v2::*;
pub use cluster::*;
pub use hash_mapping::*;
use prost::Message;
use risingwave_common::error::Result;
pub use stream::*;

use crate::storage::{self, MetaStore, Transaction};

pub type ActorId = u32;
pub type FragmentId = u32;

pub trait Transactional {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> risingwave_common::error::Result<()>;
    fn delete_in_transaction(&self, trx: &mut Transaction) -> risingwave_common::error::Result<()>;
}

/// `MetadataModel` defines basic model operations in CRUD.
#[async_trait]
pub trait MetadataModel: std::fmt::Debug + Sized {
    /// Serialized prost message type.
    type ProstType: Message + Default;
    /// Serialized key type.
    type KeyType: Message;

    /// Column family for this model.
    fn cf_name() -> String;

    /// Serialize to protobuf.
    fn to_protobuf(&self) -> Self::ProstType;

    /// Serialize to protobuf encoded byte vector.
    fn to_protobuf_encoded_vec(&self) -> Vec<u8> {
        self.to_protobuf().encode_to_vec()
    }

    /// Deserialize from protobuf.
    fn from_protobuf(prost: Self::ProstType) -> Self;

    /// Current record key.
    fn key(&self) -> Result<Self::KeyType>;

    /// `list` returns all records in this model.
    async fn list<S>(store: &S) -> Result<Vec<Self>>
    where
        S: MetaStore,
    {
        let bytes_vec = store.list_cf(&Self::cf_name()).await?;
        Ok(bytes_vec
            .iter()
            .map(|bytes| Self::from_protobuf(Self::ProstType::decode(bytes.as_slice()).unwrap()))
            .collect::<Vec<_>>())
    }

    /// `insert` insert a new record in meta store, replaced it if the record already exist.
    async fn insert<S>(&self, store: &S) -> Result<()>
    where
        S: MetaStore,
    {
        store
            .put_cf(
                &Self::cf_name(),
                self.key()?.encode_to_vec(),
                self.to_protobuf().encode_to_vec(),
            )
            .await
            .map_err(Into::into)
    }

    /// `delete` drop records from meta store with associated key.
    async fn delete<S>(store: &S, key: &Self::KeyType) -> Result<()>
    where
        S: MetaStore,
    {
        store
            .delete_cf(&Self::cf_name(), &key.encode_to_vec())
            .await
            .map_err(Into::into)
    }

    /// `select` query a record with associated key and version.
    async fn select<S>(store: &S, key: &Self::KeyType) -> Result<Option<Self>>
    where
        S: MetaStore,
    {
        let byte_vec = match store.get_cf(&Self::cf_name(), &key.encode_to_vec()).await {
            Ok(byte_vec) => byte_vec,
            Err(err) => {
                if !matches!(err, storage::Error::ItemNotFound(_)) {
                    return Err(err.into());
                }
                return Ok(None);
            }
        };
        let model = Self::from_protobuf(Self::ProstType::decode(byte_vec.as_slice())?);
        Ok(Some(model))
    }
}

/// `Transactional` defines operations supported in a transaction.
/// Read operations can be supported if necessary.
impl<T> Transactional for T
where
    T: MetadataModel,
{
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> Result<()> {
        trx.put(
            Self::cf_name(),
            self.key()?.encode_to_vec(),
            self.to_protobuf_encoded_vec(),
        );
        Ok(())
    }
    fn delete_in_transaction(&self, trx: &mut Transaction) -> Result<()> {
        trx.delete(Self::cf_name(), self.key()?.encode_to_vec());
        Ok(())
    }
}

/// Trait that wraps a local memory value and applies the change to the local memory value on
/// `commit` or leaves the local memory value untouched on `abort`.
pub trait ValTransaction: Sized {
    /// Commit the change to local memory value
    fn commit(self);

    /// Apply the change (upsert or delete) to `txn`
    fn apply_to_txn(&self, txn: &mut Transaction) -> Result<()>;

    /// Abort the `VarTransaction` and leave the local memory value untouched
    fn abort(self) {
        drop(self);
    }
}

/// Transaction wrapper for a variable.
/// In first `deref_mut` call, a copy of the original value will be assigned to `new_value`
/// and all subsequent modifications will be applied to the `new_value`.
/// When `commit` is called, the change to `new_value` will be applied to the `orig_value_ref`
/// When `abort` is called, the VarTransaction is dropped and the local memory value is
/// untouched.
pub struct VarTransaction<'a, T> {
    orig_value_ref: &'a mut T,
    new_value: Option<T>,
}

impl<'a, T> VarTransaction<'a, T>
where
    T: Clone,
{
    /// Create a `VarTransaction` that wraps a raw variable
    pub fn new(val_ref: &'a mut T) -> VarTransaction<'a, T> {
        VarTransaction {
            // lazy initialization
            new_value: None,
            orig_value_ref: val_ref,
        }
    }
}

impl<'a, T> Deref for VarTransaction<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match &self.new_value {
            Some(new_value) => new_value,
            None => self.orig_value_ref,
        }
    }
}

impl<'a, T> DerefMut for VarTransaction<'a, T>
where
    T: Clone,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        if let None = self.new_value {
            self.new_value.replace(self.orig_value_ref.clone());
        }
        self.new_value.as_mut().unwrap()
    }
}

impl<'a, T> ValTransaction for VarTransaction<'a, T>
where
    T: Transactional + PartialEq,
{
    fn commit(self) {
        if let Some(new_value) = self.new_value {
            *self.orig_value_ref = new_value;
        }
    }

    fn apply_to_txn(&self, txn: &mut Transaction) -> Result<()> {
        if let Some(new_value) = &self.new_value {
            // Apply the change to `txn` only when the value is modified
            if *self.orig_value_ref != *new_value {
                new_value.upsert_in_transaction(txn)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

impl<'a, K, V> ValTransaction for VarTransaction<'a, BTreeMap<K, V>>
where
    K: Ord,
    V: Transactional + PartialEq,
{
    fn commit(self) {
        if let Some(new_value) = self.new_value {
            *self.orig_value_ref = new_value;
        }
    }

    /// For keys only in `self.orig_value`, call `delete_in_transaction` for the corresponding
    /// value. For keys only in `self.new_value` or in both `self.new_value` and
    /// `self.orig_value` but different in the value, call `upsert_in_transaction` for the
    /// corresponding value.
    fn apply_to_txn(&self, txn: &mut Transaction) -> Result<()> {
        if let Some(new_value) = &self.new_value {
            for (k, v) in self.orig_value_ref.iter() {
                if !new_value.contains_key(k) {
                    v.delete_in_transaction(txn)?;
                }
            }
            for (k, v) in new_value {
                let orig_value = self.orig_value_ref.get(k);
                if orig_value.is_none() || orig_value.unwrap() != v {
                    v.upsert_in_transaction(txn)?;
                }
            }
        }
        Ok(())
    }
}

/// Transaction wrapper for a `BTreeMap` entry value of given `key`
pub struct BTreeMapEntryTransaction<'a, K, V> {
    tree_ref: &'a mut BTreeMap<K, V>,
    key: K,
    new_value: V,
}

impl<'a, K: Ord, V: Clone> BTreeMapEntryTransaction<'a, K, V> {
    /// Create a `ValTransaction` that wraps a `BTreeMap` entry of the given `key`.
    /// If the tree does not contain `key`, the `default_val` will be used as the initial value
    pub fn new_or_default(
        tree_ref: &'a mut BTreeMap<K, V>,
        key: K,
        default_val: V,
    ) -> BTreeMapEntryTransaction<'a, K, V> {
        let init_value = tree_ref.get(&key).cloned().unwrap_or(default_val);
        BTreeMapEntryTransaction {
            new_value: init_value,
            tree_ref,
            key,
        }
    }

    /// Create a `ValTransaction` that wraps a `BTreeMap` entry of the given `key`.
    /// If the `key` exists in the tree, return `Some` of a `VarTransaction` wrapped for the
    /// of the given `key`.
    /// Otherwise return `None`.
    pub fn new(
        tree_ref: &'a mut BTreeMap<K, V>,
        key: K,
    ) -> Option<BTreeMapEntryTransaction<'a, K, V>> {
        tree_ref
            .get(&key)
            .cloned()
            .map(|orig_value| BTreeMapEntryTransaction {
                new_value: orig_value.clone(),
                tree_ref,
                key,
            })
    }
}

impl<'a, K, V> Deref for BTreeMapEntryTransaction<'a, K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.new_value
    }
}

impl<'a, K, V> DerefMut for BTreeMapEntryTransaction<'a, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.new_value
    }
}

impl<'a, K: Ord, V: PartialEq + Transactional> ValTransaction
    for BTreeMapEntryTransaction<'a, K, V>
{
    fn commit(self) {
        self.tree_ref.insert(self.key, self.new_value);
    }

    fn apply_to_txn(&self, txn: &mut Transaction) -> Result<()> {
        if !self.tree_ref.contains_key(&self.key)
            || *self.tree_ref.get(&self.key).unwrap() != self.new_value
        {
            self.new_value.upsert_in_transaction(txn)?
        }
        Ok(())
    }
}

impl<'a, K: Ord, V: Clone> VarTransaction<'a, BTreeMap<K, V>> {
    pub fn new_entry_txn(&mut self, key: K) -> Option<BTreeMapEntryTransaction<K, V>> {
        BTreeMapEntryTransaction::new(self.orig_value_ref, key)
    }

    pub fn new_entry_txn_or_default(
        &mut self,
        key: K,
        default_val: V,
    ) -> BTreeMapEntryTransaction<K, V> {
        BTreeMapEntryTransaction::new_or_default(self.orig_value_ref, key, default_val)
    }
}
