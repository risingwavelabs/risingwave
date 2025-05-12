// Copyright 2025 RisingWave Labs
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

mod error;
mod stream;

use std::collections::BTreeMap;
use std::collections::btree_map::{Entry, VacantEntry};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use async_trait::async_trait;
pub use error::*;
pub use stream::*;
use uuid::Uuid;

/// A global, unique identifier of an actor
pub type ActorId = u32;

/// Should be used together with `ActorId` to uniquely identify a dispatcher
pub type DispatcherId = u64;

/// A global, unique identifier of a fragment
pub type FragmentId = u32;

pub type SubscriptionId = u32;

#[derive(Clone, Debug)]
pub struct ClusterId(String);

impl Default for ClusterId {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl From<ClusterId> for String {
    fn from(value: ClusterId) -> Self {
        value.0
    }
}

impl From<String> for ClusterId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Deref for ClusterId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

#[async_trait]
pub trait Transactional<TXN> {
    async fn upsert_in_transaction(&self, trx: &mut TXN) -> MetadataModelResult<()>;
    async fn delete_in_transaction(&self, trx: &mut TXN) -> MetadataModelResult<()>;
}

pub trait InMemValTransaction: Sized {
    /// Commit the change to local memory value
    fn commit(self);
}

/// Trait that wraps a local memory value and applies the change to the local memory value on
/// `commit` or leaves the local memory value untouched on `abort`.
pub trait ValTransaction<TXN>: InMemValTransaction {
    /// Apply the change (upsert or delete) to `txn`
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()>;
}

/// Transaction wrapper for a variable.
/// In first `deref_mut` call, a copy of the original value will be assigned to `new_value`
/// and all subsequent modifications will be applied to the `new_value`.
/// When `commit` is called, the change to `new_value` will be applied to the `orig_value_ref`
/// When `abort` is called, the `VarTransaction` is dropped and the local memory value is
/// untouched.
pub struct VarTransaction<'a, T> {
    orig_value_ref: &'a mut T,
    new_value: Option<T>,
}

impl<'a, T> VarTransaction<'a, T> {
    /// Create a `VarTransaction` that wraps a raw variable
    pub fn new(val_ref: &'a mut T) -> VarTransaction<'a, T> {
        VarTransaction {
            // lazy initialization
            new_value: None,
            orig_value_ref: val_ref,
        }
    }

    pub fn has_new_value(&self) -> bool {
        self.new_value.is_some()
    }
}

impl<T> Deref for VarTransaction<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match &self.new_value {
            Some(new_value) => new_value,
            None => self.orig_value_ref,
        }
    }
}

impl<T: Clone> DerefMut for VarTransaction<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        if self.new_value.is_none() {
            self.new_value.replace(self.orig_value_ref.clone());
        }
        self.new_value.as_mut().unwrap()
    }
}

impl<T> InMemValTransaction for VarTransaction<'_, T>
where
    T: PartialEq,
{
    fn commit(self) {
        if let Some(new_value) = self.new_value {
            *self.orig_value_ref = new_value;
        }
    }
}

impl<TXN, T> ValTransaction<TXN> for VarTransaction<'_, T>
where
    T: Transactional<TXN> + PartialEq,
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        if let Some(new_value) = &self.new_value {
            // Apply the change to `txn` only when the value is modified
            if *self.orig_value_ref != *new_value {
                new_value.upsert_in_transaction(txn).await
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

/// Represent the entry of the `staging` field of a `BTreeMapTransaction`
enum BTreeMapTransactionStagingEntry<'a, K: Ord, V> {
    /// The entry of a key does not exist in the `staging` field yet.
    Vacant(VacantEntry<'a, K, BTreeMapOp<V>>),
    /// The entry of a key exists in the `staging` field. A mutable reference to the value of the
    /// staging entry is provided for mutable access.
    Occupied(&'a mut V),
}

/// A mutable guard to the value of the corresponding key of a `BTreeMapTransaction`.
/// The staging value is initialized in a lazy manner, that is, the staging value is only cloned
/// from the original value only when it's being mutably deref.
pub struct BTreeMapTransactionValueGuard<'a, K: Ord, V: Clone> {
    // `staging_entry` is always `Some` so it's always safe to unwrap it. We make it `Option` so
    // that we can take a `Vacant` out, take its ownership, insert value into `VacantEntry` and
    // insert an `Occupied` back to the `Option`.
    // If `staging_entry` is `Vacant`, `orig_value` must be Some
    staging_entry: Option<BTreeMapTransactionStagingEntry<'a, K, V>>,
    // If the `orig_value` is None, the `staging_entry` must be `Occupied`
    orig_value: Option<&'a V>,
}

impl<'a, K: Ord, V: Clone> BTreeMapTransactionValueGuard<'a, K, V> {
    fn new(
        staging_entry: BTreeMapTransactionStagingEntry<'a, K, V>,
        orig_value: Option<&'a V>,
    ) -> Self {
        let is_entry_occupied =
            matches!(staging_entry, BTreeMapTransactionStagingEntry::Occupied(_));
        assert!(
            is_entry_occupied || orig_value.is_some(),
            "one of staging_entry and orig_value must be non-empty"
        );
        Self {
            staging_entry: Some(staging_entry),
            orig_value,
        }
    }
}

impl<K: Ord, V: Clone> Deref for BTreeMapTransactionValueGuard<'_, K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        // Read the staging entry first. If the staging entry is vacant, read the original value
        match &self.staging_entry.as_ref().unwrap() {
            BTreeMapTransactionStagingEntry::Vacant(_) => self
                .orig_value
                .expect("staging is vacant, so orig_value must be some"),
            BTreeMapTransactionStagingEntry::Occupied(v) => v,
        }
    }
}

impl<K: Ord, V: Clone> DerefMut for BTreeMapTransactionValueGuard<'_, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let is_occupied = matches!(
            self.staging_entry.as_ref().unwrap(),
            BTreeMapTransactionStagingEntry::Occupied(_)
        );

        // When the staging entry is vacant, take a copy of the original value and insert an entry
        // into the staging.
        if !is_occupied {
            let vacant_entry = match self.staging_entry.take().unwrap() {
                BTreeMapTransactionStagingEntry::Vacant(entry) => entry,
                BTreeMapTransactionStagingEntry::Occupied(_) => {
                    unreachable!("we have previously check that the entry is not occupied")
                }
            };

            // Insert a cloned original value to staging through `vacant_entry`
            let new_value_mut_ref = match vacant_entry.insert(BTreeMapOp::Insert(
                self.orig_value
                    .expect("self.staging_entry was vacant, so orig_value must be some")
                    .clone(),
            )) {
                BTreeMapOp::Insert(v) => v,
                BTreeMapOp::Delete => {
                    unreachable!(
                        "the previous inserted op is `Inserted`, so it's not possible to reach Delete"
                    )
                }
            };
            // Set the staging entry to `Occupied`.
            let _ = self
                .staging_entry
                .insert(BTreeMapTransactionStagingEntry::Occupied(new_value_mut_ref));
        }

        match self.staging_entry.as_mut().unwrap() {
            BTreeMapTransactionStagingEntry::Vacant(_) => {
                unreachable!("we have inserted a cloned original value in case of vacant")
            }
            BTreeMapTransactionStagingEntry::Occupied(v) => v,
        }
    }
}

enum BTreeMapOp<V> {
    Insert(V),
    Delete,
}

/// A `ValTransaction` that wraps a `BTreeMap`. It supports basic `BTreeMap` operations like `get`,
/// `get_mut`, `insert` and `remove`. Incremental modification of `insert`, `remove` and `get_mut`
/// are stored in `staging`. On `commit`, it will apply the changes stored in `staging` to the in
/// memory btree map. When serve `get` and `get_mut`, it merges the value stored in `staging` and
/// `tree_ref`.
pub struct BTreeMapTransactionInner<K: Ord, V, P: DerefMut<Target = BTreeMap<K, V>>> {
    /// A reference to the original `BTreeMap`. All access to this field should be immutable,
    /// except when we commit the staging changes to the original map.
    tree_ref: P,
    /// Store all the staging changes that will be applied to the original map on commit
    staging: BTreeMap<K, BTreeMapOp<V>>,
}

pub type BTreeMapTransaction<'a, K, V> = BTreeMapTransactionInner<K, V, &'a mut BTreeMap<K, V>>;

impl<K: Ord + Debug, V: Clone, P: DerefMut<Target = BTreeMap<K, V>>>
    BTreeMapTransactionInner<K, V, P>
{
    pub fn new(tree_ref: P) -> BTreeMapTransactionInner<K, V, P> {
        Self {
            tree_ref,
            staging: BTreeMap::default(),
        }
    }

    /// Start a `BTreeMapEntryTransaction` when the `key` exists
    #[allow(dead_code)]
    pub fn new_entry_txn(&mut self, key: K) -> Option<BTreeMapEntryTransaction<'_, K, V>> {
        BTreeMapEntryTransaction::new(&mut self.tree_ref, key, None)
    }

    /// Start a `BTreeMapEntryTransaction`. If the `key` does not exist, the the `default_val` will
    /// be taken as the initial value of the transaction and will be applied to the original
    /// `BTreeMap` on commit.
    pub fn new_entry_txn_or_default(
        &mut self,
        key: K,
        default_val: V,
    ) -> BTreeMapEntryTransaction<'_, K, V> {
        BTreeMapEntryTransaction::new(&mut self.tree_ref, key, Some(default_val))
            .expect("default value is provided and should return `Some`")
    }

    /// Start a `BTreeMapEntryTransaction` that inserts the `val` into `key`.
    pub fn new_entry_insert_txn(&mut self, key: K, val: V) -> BTreeMapEntryTransaction<'_, K, V> {
        BTreeMapEntryTransaction::new_insert(&mut self.tree_ref, key, val)
    }

    pub fn tree_ref(&self) -> &BTreeMap<K, V> {
        &self.tree_ref
    }

    /// Get the value of the provided key by merging the staging value and the original value
    pub fn get(&self, key: &K) -> Option<&V> {
        self.staging
            .get(key)
            .and_then(|op| match op {
                BTreeMapOp::Insert(v) => Some(v),
                BTreeMapOp::Delete => None,
            })
            .or_else(|| self.tree_ref.get(key))
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// This method serves the same semantic to the `get_mut` of `BTreeMap`.
    ///
    /// It return a `BTreeMapTransactionValueGuard` of the corresponding key for mutable access to
    /// guarded staging value.
    ///
    /// When the value does not exist in the staging (either key not exist or with a Delete record)
    /// and the value does not exist in the original `BTreeMap`, return None.
    pub fn get_mut(&mut self, key: K) -> Option<BTreeMapTransactionValueGuard<'_, K, V>> {
        let orig_contains_key = self.tree_ref.contains_key(&key);
        let orig_value = self.tree_ref.get(&key);

        let staging_entry = match self.staging.entry(key) {
            Entry::Occupied(entry) => match entry.into_mut() {
                BTreeMapOp::Insert(v) => BTreeMapTransactionStagingEntry::Occupied(v),
                BTreeMapOp::Delete => return None,
            },
            Entry::Vacant(vacant_entry) => {
                if !orig_contains_key {
                    return None;
                } else {
                    BTreeMapTransactionStagingEntry::Vacant(vacant_entry)
                }
            }
        };
        Some(BTreeMapTransactionValueGuard::new(
            staging_entry,
            orig_value,
        ))
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.staging.insert(key, BTreeMapOp::Insert(value));
    }

    pub fn remove(&mut self, key: K) -> Option<V> {
        if let Some(op) = self.staging.get(&key) {
            return match op {
                BTreeMapOp::Delete => None,
                BTreeMapOp::Insert(_) => match self.staging.remove(&key).unwrap() {
                    BTreeMapOp::Insert(v) => {
                        self.staging.insert(key, BTreeMapOp::Delete);
                        Some(v)
                    }
                    BTreeMapOp::Delete => {
                        unreachable!(
                            "we have checked that the op of the key is `Insert`, so it's impossible to be Delete"
                        )
                    }
                },
            };
        }
        match self.tree_ref.get(&key) {
            Some(orig_value) => {
                self.staging.insert(key, BTreeMapOp::Delete);
                Some(orig_value.clone())
            }
            None => None,
        }
    }

    pub fn commit_memory(mut self) {
        // Apply each op stored in the staging to original tree.
        for (k, op) in self.staging {
            match op {
                BTreeMapOp::Insert(v) => {
                    self.tree_ref.insert(k, v);
                }
                BTreeMapOp::Delete => {
                    self.tree_ref.remove(&k);
                }
            }
        }
    }
}

impl<K: Ord + Debug, V: Clone, P: DerefMut<Target = BTreeMap<K, V>>> InMemValTransaction
    for BTreeMapTransactionInner<K, V, P>
{
    fn commit(self) {
        self.commit_memory();
    }
}

impl<K: Ord + Debug, V: Transactional<TXN> + Clone, P: DerefMut<Target = BTreeMap<K, V>>, TXN>
    ValTransaction<TXN> for BTreeMapTransactionInner<K, V, P>
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        // Add the staging operation to txn
        for (k, op) in &self.staging {
            match op {
                BTreeMapOp::Insert(v) => v.upsert_in_transaction(txn).await?,
                BTreeMapOp::Delete => {
                    if let Some(v) = self.tree_ref.get(k) {
                        v.delete_in_transaction(txn).await?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Transaction wrapper for a `BTreeMap` entry value of given `key`
pub struct BTreeMapEntryTransaction<'a, K, V> {
    tree_ref: &'a mut BTreeMap<K, V>,
    pub key: K,
    pub new_value: V,
}

impl<'a, K: Ord + Debug, V: Clone> BTreeMapEntryTransaction<'a, K, V> {
    /// Create a `ValTransaction` that wraps a `BTreeMap` entry of the given `key`.
    /// If the tree does not contain `key`, the `default_val` will be used as the initial value
    pub fn new_insert(
        tree_ref: &'a mut BTreeMap<K, V>,
        key: K,
        value: V,
    ) -> BTreeMapEntryTransaction<'a, K, V> {
        BTreeMapEntryTransaction {
            new_value: value,
            tree_ref,
            key,
        }
    }

    /// Create a `BTreeMapEntryTransaction` that wraps a `BTreeMap` entry of the given `key`.
    /// If the `key` exists in the tree, return `Some` of a `BTreeMapEntryTransaction` wrapped for
    /// the of the given `key`.
    /// If the `key` does not exist in the tree but `default_val` is provided as `Some`, a
    /// `BTreeMapEntryTransaction` that wraps the given `key` and default value is returned
    /// Otherwise return `None`.
    pub fn new(
        tree_ref: &'a mut BTreeMap<K, V>,
        key: K,
        default_val: Option<V>,
    ) -> Option<BTreeMapEntryTransaction<'a, K, V>> {
        tree_ref
            .get(&key)
            .cloned()
            .or(default_val)
            .map(|orig_value| BTreeMapEntryTransaction {
                new_value: orig_value,
                tree_ref,
                key,
            })
    }
}

impl<K, V> Deref for BTreeMapEntryTransaction<'_, K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.new_value
    }
}

impl<K, V> DerefMut for BTreeMapEntryTransaction<'_, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.new_value
    }
}

impl<K: Ord, V: PartialEq> InMemValTransaction for BTreeMapEntryTransaction<'_, K, V> {
    fn commit(self) {
        self.tree_ref.insert(self.key, self.new_value);
    }
}

impl<K: Ord, V: PartialEq + Transactional<TXN>, TXN> ValTransaction<TXN>
    for BTreeMapEntryTransaction<'_, K, V>
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        if !self.tree_ref.contains_key(&self.key)
            || *self.tree_ref.get(&self.key).unwrap() != self.new_value
        {
            self.new_value.upsert_in_transaction(txn).await?
        }
        Ok(())
    }
}

impl<T: InMemValTransaction> InMemValTransaction for Option<T> {
    fn commit(self) {
        if let Some(inner) = self {
            inner.commit();
        }
    }
}

impl<T: ValTransaction<TXN>, TXN> ValTransaction<TXN> for Option<T> {
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        if let Some(inner) = &self {
            inner.apply_to_txn(txn).await?;
        }
        Ok(())
    }
}

pub struct DerefMutForward<
    Inner,
    Target,
    P: DerefMut<Target = Inner>,
    F: Fn(&Inner) -> &Target,
    FMut: Fn(&mut Inner) -> &mut Target,
> {
    ptr: P,
    f: F,
    f_mut: FMut,
}

impl<
    Inner,
    Target,
    P: DerefMut<Target = Inner>,
    F: Fn(&Inner) -> &Target,
    FMut: Fn(&mut Inner) -> &mut Target,
> DerefMutForward<Inner, Target, P, F, FMut>
{
    pub fn new(ptr: P, f: F, f_mut: FMut) -> Self {
        Self { ptr, f, f_mut }
    }
}

impl<
    Inner,
    Target,
    P: DerefMut<Target = Inner>,
    F: Fn(&Inner) -> &Target,
    FMut: Fn(&mut Inner) -> &mut Target,
> Deref for DerefMutForward<Inner, Target, P, F, FMut>
{
    type Target = Target;

    fn deref(&self) -> &Self::Target {
        (self.f)(&self.ptr)
    }
}

impl<
    Inner,
    Target,
    P: DerefMut<Target = Inner>,
    F: Fn(&Inner) -> &Target,
    FMut: Fn(&mut Inner) -> &mut Target,
> DerefMut for DerefMutForward<Inner, Target, P, F, FMut>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        (self.f_mut)(&mut self.ptr)
    }
}
