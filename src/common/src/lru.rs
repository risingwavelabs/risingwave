// Copyright 2024 RisingWave Labs
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

use std::alloc::{Allocator, Global};
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub use ahash::RandomState;
use hashbrown::hash_table::Entry;
use hashbrown::HashTable;

pub type Sequence = usize;
pub type AtomicSequence = AtomicUsize;

struct LruEntry<K, V>
where
    K: Hash + Eq,
{
    prev: Option<NonNull<LruEntry<K, V>>>,
    next: Option<NonNull<LruEntry<K, V>>>,
    key: MaybeUninit<K>,
    value: MaybeUninit<V>,
    hash: u64,
    sequence: Sequence,
}

impl<K, V> LruEntry<K, V>
where
    K: Hash + Eq,
{
    fn key(&self) -> &K {
        unsafe { self.key.assume_init_ref() }
    }

    fn value(&self) -> &V {
        unsafe { self.value.assume_init_ref() }
    }

    fn value_mut(&mut self) -> &mut V {
        unsafe { self.value.assume_init_mut() }
    }
}

unsafe impl<K, V> Send for LruEntry<K, V> where K: Hash + Eq {}
unsafe impl<K, V> Sync for LruEntry<K, V> where K: Hash + Eq {}

pub struct LruCache<K, V, S = RandomState, A = Global>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
    A: Clone + Allocator,
{
    map: HashTable<NonNull<LruEntry<K, V>>, A>,
    /// dummy node of the lru linked list
    dummy: Box<LruEntry<K, V>, A>,
    /// Global sequence.
    sequence: Arc<AtomicSequence>,

    alloc: A,
    hash_builder: S,
}

unsafe impl<K, V, S, A> Send for LruCache<K, V, S, A>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
    A: Clone + Allocator,
{
}
unsafe impl<K, V, S, A> Sync for LruCache<K, V, S, A>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
    A: Clone + Allocator,
{
}

impl<K, V> LruCache<K, V>
where
    K: Hash + Eq,
{
    pub fn unbounded(sequence: Arc<AtomicSequence>) -> Self {
        Self::unbounded_with_hasher_in(sequence, RandomState::default(), Global)
    }
}

impl<K, V, S, A> LruCache<K, V, S, A>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
    A: Clone + Allocator,
{
    pub fn unbounded_with_hasher_in(
        sequence: Arc<AtomicSequence>,
        hash_builder: S,
        alloc: A,
    ) -> Self {
        let map = HashTable::new_in(alloc.clone());
        let mut dummy = Box::new_in(
            LruEntry {
                prev: None,
                next: None,
                key: MaybeUninit::uninit(),
                value: MaybeUninit::uninit(),
                hash: 0,
                sequence: Sequence::default(),
            },
            alloc.clone(),
        );
        let ptr = unsafe { NonNull::new_unchecked(dummy.as_mut() as *mut _) };
        dummy.next = Some(ptr);
        dummy.prev = Some(ptr);
        Self {
            map,
            dummy,
            sequence,
            alloc,
            hash_builder,
        }
    }

    pub fn put(&mut self, key: K, mut value: V) -> Option<V> {
        unsafe {
            let hash = self.hash_builder.hash_one(&key);

            match self
                .map
                .entry(hash, |p| p.as_ref().key() == &key, |p| p.as_ref().hash)
            {
                Entry::Occupied(o) => {
                    let mut ptr = *o.get();
                    let entry = ptr.as_mut();
                    std::mem::swap(&mut value, entry.value_mut());
                    self.detach(ptr);
                    self.attach(ptr);
                    Some(value)
                }
                Entry::Vacant(v) => {
                    let entry = Box::new_in(
                        LruEntry {
                            prev: None,
                            next: None,
                            key: MaybeUninit::new(key),
                            value: MaybeUninit::new(value),
                            hash,
                            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
                        },
                        self.alloc.clone(),
                    );
                    let ptr = NonNull::new_unchecked(Box::into_raw(entry));
                    v.insert(ptr);
                    self.attach(ptr);
                    None
                }
            }
        }
    }

    pub fn get<'a, Q>(&'a mut self, key: &Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        unsafe {
            let key = key.borrow();
            let hash = self.hash_builder.hash_one(key);
            if let Some(ptr) = self.map.find(hash, |p| p.as_ref().key().borrow() == key) {
                let ptr = *ptr;
                self.detach(ptr);
                self.attach(ptr);
                Some(ptr.as_ref().value())
            } else {
                None
            }
        }
    }

    pub fn get_mut<'a, Q>(&'a mut self, key: &Q) -> Option<&'a mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        unsafe {
            let key = key.borrow();
            let hash = self.hash_builder.hash_one(key);
            if let Some(ptr) = self
                .map
                .find_mut(hash, |p| p.as_ref().key().borrow() == key)
            {
                let mut ptr = *ptr;
                self.detach(ptr);
                self.attach(ptr);
                Some(ptr.as_mut().value_mut())
            } else {
                None
            }
        }
    }

    pub fn peek<'a, Q>(&'a self, key: &Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        unsafe {
            let key = key.borrow();
            let hash = self.hash_builder.hash_one(key);
            self.map
                .find(hash, |p| p.as_ref().key().borrow() == key)
                .map(|ptr| ptr.as_ref().value())
        }
    }

    pub fn peek_mut<'a, Q>(&'a mut self, key: &Q) -> Option<&'a mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        unsafe {
            let key = key.borrow();
            let hash = self.hash_builder.hash_one(key);
            self.map
                .find(hash, |p| p.as_ref().key().borrow() == key)
                .map(|ptr| ptr.clone().as_mut().value_mut())
        }
    }

    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        unsafe {
            let key = key.borrow();
            let hash = self.hash_builder.hash_one(key);
            self.map
                .find(hash, |p| p.as_ref().key().borrow() == key)
                .is_some()
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pop first entry if its sequence is less than the given sequence.
    pub fn pop_with_sequence(&mut self, sequence: Sequence) -> Option<(K, V, Sequence)> {
        unsafe {
            if self.is_empty() {
                return None;
            }

            let ptr = self.dummy.next.unwrap_unchecked();
            if ptr.as_ref().sequence >= sequence {
                return None;
            }

            self.detach(ptr);

            let entry = Box::from_raw_in(ptr.as_ptr(), self.alloc.clone());

            let key = entry.key.assume_init();
            let value = entry.value.assume_init();
            let sequence = entry.sequence;

            let hash = self.hash_builder.hash_one(&key);

            match self
                .map
                .entry(hash, |p| p.as_ref().key() == &key, |p| p.as_ref().hash)
            {
                Entry::Occupied(o) => {
                    o.remove();
                }
                Entry::Vacant(_) => {}
            }

            Some((key, value, sequence))
        }
    }

    pub fn clear(&mut self) {
        unsafe {
            while !self.is_empty() {
                let ptr = self.dummy.next.unwrap_unchecked();
                self.detach(ptr);
                let entry = Box::from_raw_in(ptr.as_ptr(), self.alloc.clone());
                let key = entry.key.assume_init();
                let hash = self.hash_builder.hash_one(&key);
                match self
                    .map
                    .entry(hash, |p| p.as_ref().key() == &key, |p| p.as_ref().hash)
                {
                    Entry::Occupied(o) => o.remove(),
                    Entry::Vacant(_) => unreachable!(),
                };
            }
        }
    }

    fn detach(&mut self, mut ptr: NonNull<LruEntry<K, V>>) {
        unsafe {
            let entry = ptr.as_mut();

            debug_assert!(entry.prev.is_some() && entry.next.is_some());

            entry.prev.unwrap_unchecked().as_mut().next = entry.next;
            entry.next.unwrap_unchecked().as_mut().prev = entry.prev;

            entry.next = None;
            entry.prev = None;
        }
    }

    fn attach(&mut self, mut ptr: NonNull<LruEntry<K, V>>) {
        unsafe {
            let entry = ptr.as_mut();

            debug_assert!(entry.prev.is_none() && entry.next.is_none());

            entry.next = Some(NonNull::new_unchecked(self.dummy.as_mut() as *mut _));
            entry.prev = self.dummy.prev;

            self.dummy.prev.unwrap_unchecked().as_mut().next = Some(ptr);
            self.dummy.prev = Some(ptr);

            entry.sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {}
