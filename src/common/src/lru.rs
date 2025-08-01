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

use std::alloc::{Allocator, Global};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::hash::{BuildHasher, Hash};
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;

pub use ahash::RandomState;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;

use crate::sequence::{AtomicSequence, Sequence, Sequencer};

thread_local! {
    pub static SEQUENCER: RefCell<Sequencer> = const { RefCell::new(Sequencer::new(Sequencer::DEFAULT_STEP, Sequencer::DEFAULT_LAG)) };
}

static SEQUENCER_DEFAULT_STEP: AtomicSequence = AtomicSequence::new(Sequencer::DEFAULT_STEP);
static SEQUENCER_DEFAULT_LAG: AtomicSequence = AtomicSequence::new(Sequencer::DEFAULT_LAG);

pub fn init_global_sequencer_args(step: Sequence, lag: Sequence) {
    SEQUENCER_DEFAULT_STEP.store(step, Ordering::Relaxed);
    SEQUENCER_DEFAULT_LAG.store(lag, Ordering::Relaxed);
}

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
    pub fn unbounded() -> Self {
        Self::unbounded_with_hasher_in(RandomState::default(), Global)
    }
}

impl<K, V, S, A> LruCache<K, V, S, A>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
    A: Clone + Allocator,
{
    pub fn unbounded_with_hasher_in(hash_builder: S, alloc: A) -> Self {
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
                    Self::detach(ptr);
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
                            // sequence will be updated by `attach`
                            sequence: 0,
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

    pub fn remove(&mut self, key: &K) -> Option<V> {
        unsafe {
            let hash = self.hash_builder.hash_one(key);

            match self
                .map
                .entry(hash, |p| p.as_ref().key() == key, |p| p.as_ref().hash)
            {
                Entry::Occupied(o) => {
                    let ptr = *o.get();

                    // Detach the entry from the LRU list
                    Self::detach(ptr);

                    // Extract entry from the box and get its value
                    let mut entry = Box::from_raw_in(ptr.as_ptr(), self.alloc.clone());
                    entry.key.assume_init_drop();
                    let value = entry.value.assume_init();

                    // Remove entry from the hash table
                    o.remove();

                    Some(value)
                }
                Entry::Vacant(_) => None,
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
                Self::detach(ptr);
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
                Self::detach(ptr);
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

            Self::detach(ptr);

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
            let mut map = HashTable::new_in(self.alloc.clone());
            std::mem::swap(&mut map, &mut self.map);

            for ptr in map.drain() {
                Self::detach(ptr);
                let mut entry = Box::from_raw_in(ptr.as_ptr(), self.alloc.clone());
                entry.key.assume_init_drop();
                entry.value.assume_init_drop();
            }

            debug_assert!(self.is_empty());
            debug_assert_eq!(
                self.dummy.as_mut() as *mut _,
                self.dummy.next.unwrap_unchecked().as_ptr()
            )
        }
    }

    fn detach(mut ptr: NonNull<LruEntry<K, V>>) {
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

            entry.sequence = SEQUENCER.with(|s| s.borrow_mut().alloc());
        }
    }
}

impl<K, V, S, A> Drop for LruCache<K, V, S, A>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
    A: Clone + Allocator,
{
    fn drop(&mut self) {
        self.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unbounded() {
        let cache: LruCache<i32, &str> = LruCache::unbounded();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_unbounded_with_hasher_in() {
        let cache: LruCache<i32, &str, RandomState, Global> =
            LruCache::unbounded_with_hasher_in(RandomState::default(), Global);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_put() {
        let mut cache = LruCache::unbounded();

        // Put new entry
        assert_eq!(cache.put(1, "one"), None);
        assert_eq!(cache.len(), 1);

        // Update existing entry
        assert_eq!(cache.put(1, "ONE"), Some("one"));
        assert_eq!(cache.len(), 1);

        // Multiple entries
        assert_eq!(cache.put(2, "two"), None);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_remove() {
        let mut cache = LruCache::unbounded();

        // Remove non-existent key
        assert_eq!(cache.remove(&1), None);

        // Remove existing key
        cache.put(1, "one");
        assert_eq!(cache.remove(&1), Some("one"));
        assert!(cache.is_empty());

        // Remove already removed key
        assert_eq!(cache.remove(&1), None);

        // Multiple entries
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");
        assert_eq!(cache.remove(&2), Some("two"));
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains(&2));
    }

    #[test]
    fn test_get() {
        let mut cache = LruCache::unbounded();

        // Get non-existent key
        assert_eq!(cache.get(&1), None);

        // Get existing key
        cache.put(1, "one");
        assert_eq!(cache.get(&1), Some(&"one"));

        // Check LRU order updated after get
        cache.put(2, "two");
        let _ = cache.get(&1); // Moves 1 to most recently used

        // Verify LRU order by using pop_with_sequence
        let (key, _, _) = cache.pop_with_sequence(u64::MAX).unwrap();
        assert_eq!(key, 2); // key 2 should be least recently used
    }

    #[test]
    fn test_get_mut() {
        let mut cache = LruCache::unbounded();

        // Get_mut non-existent key
        assert_eq!(cache.get_mut(&1), None);

        // Get_mut and modify existing key
        cache.put(1, String::from("one"));
        {
            let val = cache.get_mut(&1).unwrap();
            *val = String::from("ONE");
        }
        assert_eq!(cache.get(&1), Some(&String::from("ONE")));

        // Check LRU order updated after get_mut
        cache.put(2, String::from("two"));
        let _ = cache.get_mut(&1); // Moves 1 to most recently used

        // Verify LRU order by using pop_with_sequence
        let (key, _, _) = cache.pop_with_sequence(u64::MAX).unwrap();
        assert_eq!(key, 2); // key 2 should be least recently used
    }

    #[test]
    fn test_peek() {
        let mut cache = LruCache::unbounded();

        // Peek non-existent key
        assert_eq!(cache.peek(&1), None);

        // Peek existing key
        cache.put(1, "one");
        cache.put(2, "two");
        assert_eq!(cache.peek(&1), Some(&"one"));

        // Verify LRU order NOT updated after peek
        let (key, _, _) = cache.pop_with_sequence(u64::MAX).unwrap();
        assert_eq!(key, 1); // key 1 should still be least recently used
    }

    #[test]
    fn test_peek_mut() {
        let mut cache = LruCache::unbounded();

        // Peek_mut non-existent key
        assert_eq!(cache.peek_mut(&1), None);

        // Peek_mut and modify existing key
        cache.put(1, String::from("one"));
        cache.put(2, String::from("two"));
        {
            let val = cache.peek_mut(&1).unwrap();
            *val = String::from("ONE");
        }
        assert_eq!(cache.peek(&1), Some(&String::from("ONE")));

        // Verify LRU order NOT updated after peek_mut
        let (key, _, _) = cache.pop_with_sequence(u64::MAX).unwrap();
        assert_eq!(key, 1); // key 1 should still be least recently used
    }

    #[test]
    fn test_contains() {
        let mut cache = LruCache::unbounded();

        // Contains on empty cache
        assert!(!cache.contains(&1));

        // Contains after put
        cache.put(1, "one");
        assert!(cache.contains(&1));

        // Contains after remove
        cache.remove(&1);
        assert!(!cache.contains(&1));
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut cache = LruCache::unbounded();

        // Empty cache
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        // Non-empty cache
        cache.put(1, "one");
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        // After multiple operations
        cache.put(2, "two");
        assert_eq!(cache.len(), 2);
        cache.remove(&1);
        assert_eq!(cache.len(), 1);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_clear() {
        let mut cache = LruCache::unbounded();

        // Clear empty cache
        cache.clear();
        assert!(cache.is_empty());

        // Clear non-empty cache
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");
        assert_eq!(cache.len(), 3);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(!cache.contains(&1));
        assert!(!cache.contains(&2));
        assert!(!cache.contains(&3));
    }

    #[test]
    fn test_lru_behavior() {
        let mut cache = LruCache::unbounded();

        // Insert in order
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        // Manipulate LRU order
        let _ = cache.get(&1); // Moves 1 to most recently used

        // Check order: 2->3->1
        let (key, _, _) = cache.pop_with_sequence(u64::MAX).unwrap();
        assert_eq!(key, 2);
        let (key, _, _) = cache.pop_with_sequence(u64::MAX).unwrap();
        assert_eq!(key, 3);
        let (key, _, _) = cache.pop_with_sequence(u64::MAX).unwrap();
        assert_eq!(key, 1);
        assert!(cache.is_empty());
    }
}
