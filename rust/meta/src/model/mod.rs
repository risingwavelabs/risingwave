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
//
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

pub trait ValModifier<'a, T> {
    fn apply_new_val(&mut self, val: T);
    fn get_orig_val(&self) -> Option<&T>;
}

pub struct BTreeMapEntryValModifier<'a, K: Ord, V> {
    tree_ref: &'a mut BTreeMap<K, V>,
    key: K,
}

impl<'a, K: Ord + Clone, V> ValModifier<'a, V> for BTreeMapEntryValModifier<'a, K, V> {
    fn apply_new_val(&mut self, val: V) {
        self.tree_ref.insert(self.key.clone(), val);
    }
    fn get_orig_val(&self) -> Option<&V> {
        None
    }
}

pub struct VarModifier<'a, T> {
    val_ref: &'a mut T,
}

impl<'a, T> ValModifier<'a, T> for VarModifier<'a, T> {
    fn apply_new_val(&mut self, val: T) {
        *self.val_ref = val;
    }
    fn get_orig_val(&self) -> Option<&T> {
        Some(self.val_ref)
    }
}

pub trait VarTransaction {
    fn commit(self);
    fn apply_to_txn(&self, txn: &mut Transaction) -> Result<()>;
}

pub struct VarTransactionImpl<'a, T> {
    modifier: Box<dyn ValModifier<'a, T> + 'a + Send>,
    // TODO: may want to use a lazy value
    orig_value: Option<T>,
    new_value: T,
}

impl<'a, T> VarTransactionImpl<'a, T>
where
    T: Clone + Send,
{
    pub fn from_btreemap_entry_or_default<K: Ord + Clone + Send>(
        tree_ref: &'a mut BTreeMap<K, T>,
        key: K,
        default_val: T,
    ) -> VarTransactionImpl<'a, T> {
        let orig_value = tree_ref.get(&key).cloned();
        VarTransactionImpl {
            orig_value: orig_value.clone(),
            new_value: orig_value.unwrap_or(default_val),
            modifier: Box::new(BTreeMapEntryValModifier { tree_ref, key }),
        }
    }

    pub fn from_btreemap_entry<K: Ord + Clone + Send>(
        tree_ref: &'a mut BTreeMap<K, T>,
        key: K,
    ) -> Option<VarTransactionImpl<'a, T>> {
        tree_ref
            .get(&key)
            .cloned()
            .map(|orig_value| VarTransactionImpl {
                orig_value: Some(orig_value.clone()),
                new_value: orig_value,
                modifier: Box::new(BTreeMapEntryValModifier { tree_ref, key }),
            })
    }

    pub fn from_var(val_ref: &'a mut T) -> VarTransactionImpl<'a, T> {
        VarTransactionImpl {
            new_value: val_ref.clone(),
            orig_value: Some(val_ref.clone()),
            modifier: Box::new(VarModifier { val_ref }),
        }
    }
}

impl<'a, T> Deref for VarTransactionImpl<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.new_value
    }
}

impl<'a, T> DerefMut for VarTransactionImpl<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.new_value
    }
}

impl<'a, T> VarTransaction for VarTransactionImpl<'a, T>
where
    T: Transactional + PartialEq + Clone,
{
    fn commit(mut self) {
        if match self.orig_value {
            None => true,
            Some(orig_value) => orig_value != self.new_value,
        } {
            self.modifier.apply_new_val(self.new_value);
        }
    }
    fn apply_to_txn(&self, txn: &mut Transaction) -> Result<()> {
        if match &self.orig_value {
            None => true,
            Some(orig_value) => *orig_value != self.new_value,
        } {
            self.new_value.upsert_in_transaction(txn)
        } else {
            Ok(())
        }
    }
}

impl<'a, K, V> VarTransaction for VarTransactionImpl<'a, BTreeMap<K, V>>
where
    K: Ord + Clone,
    V: Transactional + PartialEq + Clone,
{
    fn commit(mut self) {
        if match self.orig_value {
            None => true,
            Some(orig_value) => orig_value != self.new_value,
        } {
            self.modifier.apply_new_val(self.new_value);
        }
    }
    fn apply_to_txn(&self, txn: &mut Transaction) -> Result<()> {
        let temp_default = BTreeMap::default();
        let orig_value = self.orig_value.as_ref().unwrap_or(&temp_default);
        if *orig_value != self.new_value {
            for (k, v) in orig_value {
                if !self.new_value.contains_key(k) {
                    v.delete_in_transaction(txn)?;
                }
            }
            for (k, v) in &self.new_value {
                let orig_value = orig_value.get(k);
                if orig_value.is_none() || orig_value.unwrap() != v {
                    v.upsert_in_transaction(txn)?;
                }
            }
            Ok(())
        } else {
            Ok(())
        }
    }
}
