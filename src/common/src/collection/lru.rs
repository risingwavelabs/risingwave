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

// MIT License

// Copyright (c) 2016 Jerome Froelich

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! An implementation of a LRU cache. The cache supports `get`, `get_mut`, `put`,
//! and `pop` operations, all of which are O(1). This crate was heavily influenced
//! by the [LRU Cache implementation in an earlier version of Rust's `std::collections` crate](https://doc.rust-lang.org/0.12.0/std/collections/lru_cache/struct.LruCache.html).
//!
//! ## Example
//!
//! ```rust
//! extern crate lru;
//!
//! use lru::LruCache;
//!
//! fn main() {
//!     let mut cache = LruCache::new(2);
//!     cache.put("apple", 3);
//!     cache.put("banana", 2);
//!
//!     assert_eq!(*cache.get(&"apple").unwrap(), 3);
//!     assert_eq!(*cache.get(&"banana").unwrap(), 2);
//!     assert!(cache.get(&"pear").is_none());
//!
//!     assert_eq!(cache.put("banana", 4), Some(2));
//!     assert_eq!(cache.put("pear", 5), None);
//!
//!     assert_eq!(*cache.get(&"pear").unwrap(), 5);
//!     assert_eq!(*cache.get(&"banana").unwrap(), 4);
//!     assert!(cache.get(&"apple").is_none());
//!
//!     {
//!         let v = cache.get_mut(&"banana").unwrap();
//!         *v = 6;
//!     }
//!
//!     assert_eq!(*cache.get(&"banana").unwrap(), 6);
//! }
//! ```

use alloc::alloc::Global;
use alloc::borrow::Borrow;
use alloc::boxed::Box;
use std::hash::{BuildHasher, Hash, Hasher};
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::{fmt, mem, ptr};

use hashbrown::HashMap;
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};

use super::estimate_size::*;

extern crate alloc;

type Epoch = u64;

struct StatsMaybeUninit<T> {
    inner: mem::MaybeUninit<T>,
}

impl<T> StatsMaybeUninit<T> {
    pub const fn uninit() -> Self {
        Self {
            inner: mem::MaybeUninit::uninit(),
        }
    }

    fn as_mut_ptr(&mut self) -> *mut T {
        self.inner.as_mut_ptr()
    }

    const fn as_ptr(&self) -> *const T {
        self.inner.as_ptr()
    }
}

impl<T: EstimateSize> StatsMaybeUninit<T> {
    pub fn new(val: T, kv_size: &mut usize) -> Self {
        *kv_size += val.estimated_heap_size();
        Self {
            inner: mem::MaybeUninit::new(val),
        }
    }

    unsafe fn drop_in_place(&mut self, kv_size: &mut usize) {
        *kv_size -= self.inner.assume_init_ref().estimated_heap_size();
        ptr::drop_in_place(self.inner.as_mut_ptr())
    }

    unsafe fn assume_init(self, kv_size: &mut usize) -> T {
        *kv_size -= self.inner.assume_init_ref().estimated_heap_size();
        self.inner.assume_init()
    }
}

// Struct used to hold a reference to a key
#[doc(hidden)]
#[derive(Eq)]
pub struct KeyRef<K> {
    k: *const K,
}

unsafe impl<K: Send> Send for KeyRef<K> {}
unsafe impl<K: Sync> Sync for KeyRef<K> {}

impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.k).hash(state) }
    }
}

impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &KeyRef<K>) -> bool {
        unsafe { (*self.k).eq(&*other.k) }
    }
}

impl<K> Borrow<K> for KeyRef<K> {
    fn borrow(&self) -> &K {
        unsafe { &*self.k }
    }
}

impl Borrow<str> for KeyRef<alloc::string::String> {
    fn borrow(&self) -> &str {
        unsafe { &*self.k }
    }
}

impl<T: ?Sized> Borrow<T> for KeyRef<Box<T>> {
    fn borrow(&self) -> &T {
        unsafe { &*self.k }
    }
}

impl<T> Borrow<[T]> for KeyRef<alloc::vec::Vec<T>> {
    fn borrow(&self) -> &[T] {
        unsafe { &*self.k }
    }
}

// Struct used to hold a key value pair. Also contains references to previous and next entries
// so we can maintain the entries in a linked list ordered by their use.
struct LruEntry<K, V> {
    key: StatsMaybeUninit<K>,
    val: StatsMaybeUninit<V>,
    prev: *mut LruEntry<K, V>,
    next: *mut LruEntry<K, V>,
    epoch: Epoch,
}

impl<K, V> LruEntry<K, V> {
    fn new_sigil() -> Self {
        LruEntry {
            key: StatsMaybeUninit::uninit(),
            val: StatsMaybeUninit::uninit(),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            epoch: 0,
        }
    }
}

impl<K: EstimateSize, V: EstimateSize> LruEntry<K, V> {
    fn new(key: K, val: V, epoch: Epoch, kv_size: &mut usize) -> Self {
        LruEntry {
            key: StatsMaybeUninit::new(key, kv_size),
            val: StatsMaybeUninit::new(val, kv_size),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            epoch,
        }
    }
}

pub type DefaultHasher = hashbrown::hash_map::DefaultHashBuilder;
pub type LruAlloc = SharedStatsAlloc<Global>;
type BoxedLruEntry<K, V> = Box<LruEntry<K, V>, LruAlloc>;
type LruMap<K, V, S> = HashMap<KeyRef<K>, BoxedLruEntry<K, V>, S, LruAlloc>;

/// An LRU Cache
pub struct LruCache<K, V, S = DefaultHasher> {
    map: LruMap<K, V, S>,
    cap: usize,

    // head and tail are sigil nodes to faciliate inserting entries
    head: *mut LruEntry<K, V>,
    tail: *mut LruEntry<K, V>,

    /// used for epoch based eviction
    cur_epoch: Epoch,

    alloc: LruAlloc,

    kv_size: usize,
}

impl<K, V, S> EstimateSize for LruCache<K, V, S> {
    fn estimated_heap_size(&self) -> usize {
        self.kv_size + self.alloc.bytes_in_use()
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> LruCache<K, V, S> {
    /// Creates a new LRU Cache with the given capacity and allocator.
    fn construct_in(
        cap: usize,
        map: LruMap<K, V, S>,
        alloc: LruAlloc,
    ) -> LruCache<K, V, S> {
        // NB: The compiler warns that cache does not need to be marked as mutable if we
        // declare it as such since we only mutate it inside the unsafe block.
        let cache = LruCache {
            map,
            cap,
            head: Box::into_raw(Box::new_in(LruEntry::new_sigil(), alloc.clone())),
            tail: Box::into_raw(Box::new_in(LruEntry::new_sigil(), alloc.clone())),
            cur_epoch: 0,
            alloc,
            kv_size: 0,
        };

        unsafe {
            (*cache.head).next = cache.tail;
            (*cache.tail).prev = cache.head;
        }

        cache
    }
}

impl<K: Hash + Eq, V> LruCache<K, V> {
    /// Creates a new LRU Cache that holds at most `cap` items.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache: LruCache<isize, &str> = LruCache::new(10);
    /// ```
    pub fn new(cap: usize) -> LruCache<K, V> {
        let alloc = StatsAlloc::new(Global).shared();
        LruCache::construct_in(cap, HashMap::with_capacity_in(cap, alloc.clone()), alloc)
    }

    /// Creates a new LRU Cache that never automatically evicts items.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache: LruCache<isize, &str> = LruCache::unbounded();
    /// ```
    pub fn unbounded() -> LruCache<K, V> {
        let alloc = StatsAlloc::new(Global).shared();
        LruCache::construct_in(usize::MAX, HashMap::new_in(alloc.clone()), alloc)
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> LruCache<K, V, S> {
    /// Creates a new LRU Cache that holds at most `cap` items and
    /// uses the provided hash builder to hash keys.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::{DefaultHasher, LruCache};
    ///
    /// let s = DefaultHasher::default();
    /// let mut cache: LruCache<isize, &str> = LruCache::with_hasher(10, s);
    /// ```
    pub fn with_hasher(cap: usize, hash_builder: S) -> LruCache<K, V, S> {
        let alloc = StatsAlloc::new(Global).shared();
        LruCache::construct_in(
            cap,
            HashMap::with_capacity_and_hasher_in(cap, hash_builder, alloc.clone()),
            alloc,
        )
    }

    /// Creates a new LRU Cache that never automatically evicts items and
    /// uses the provided hash builder to hash keys.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::{DefaultHasher, LruCache};
    ///
    /// let s = DefaultHasher::default();
    /// let mut cache: LruCache<isize, &str> = LruCache::unbounded_with_hasher(s);
    /// ```
    pub fn unbounded_with_hasher(hash_builder: S) -> LruCache<K, V, S> {
        let alloc = StatsAlloc::new(Global).shared();
        LruCache::construct_in(
            usize::MAX,
            HashMap::with_hasher_in(hash_builder, alloc.clone()),
            alloc,
        )
    }
}

impl<K: Hash + Eq + EstimateSize, V: EstimateSize, S: BuildHasher> LruCache<K, V, S> {
    /// Puts a key-value pair into cache. If the key already exists in the cache, then it updates
    /// the key's value and returns the old value. Otherwise, `None` is returned.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// assert_eq!(None, cache.put(1, "a"));
    /// assert_eq!(None, cache.put(2, "b"));
    /// assert_eq!(Some("b"), cache.put(2, "beta"));
    ///
    /// assert_eq!(cache.get(&1), Some(&"a"));
    /// assert_eq!(cache.get(&2), Some(&"beta"));
    /// ```
    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        self.capturing_put(k, v, false).map(|(_, v)| v)
    }

    // Used internally by `put` and `push` to add a new entry to the lru.
    // Takes ownership of and returns entries replaced due to the cache's capacity
    // when `capture` is true.
    fn capturing_put(&mut self, k: K, mut v: V, capture: bool) -> Option<(K, V)> {
        let node_ref = self.map.get_mut(&KeyRef { k: &k });

        match node_ref {
            Some(node_ref) => {
                let node_ptr: *mut LruEntry<K, V> = &mut **node_ref;

                // if the key is already in the cache just update its value and move it to the
                // front of the list
                unsafe { mem::swap(&mut v, &mut (*(*node_ptr).val.as_mut_ptr()) as &mut V) }
                self.detach(node_ptr);
                self.attach(node_ptr);
                Some((k, v))
            }
            None => {
                // if the capacity is zero, do nothing
                if self.cap() == 0 {
                    return None;
                }

                let (replaced, mut node) = self.replace_or_create_node(k, v);

                let node_ptr: *mut LruEntry<K, V> = &mut *node;
                self.attach(node_ptr);

                let keyref = unsafe { (*node_ptr).key.as_ptr() };
                self.map.insert(KeyRef { k: keyref }, node);

                replaced.filter(|_| capture)
            }
        }
    }

    // Used internally to swap out a node if the cache is full or to create a new node if space
    // is available. Shared between `put`, `push`, and `get_or_insert`.
    fn replace_or_create_node(
        &mut self,
        k: K,
        v: V,
    ) -> (Option<(K, V)>, BoxedLruEntry<K, V>) {
        if self.len() == self.cap() {
            // if the cache is full, remove the last entry so we can use it for the new key
            let old_key = KeyRef {
                k: unsafe { &(*(*(*self.tail).prev).key.as_ptr()) },
            };
            let mut old_node = self.map.remove(&old_key).unwrap();

            // read out the node's old key and value and then replace it
            let replaced = unsafe {
                (
                    old_node.key.assume_init(&mut self.kv_size),
                    old_node.val.assume_init(&mut self.kv_size),
                )
            };

            old_node.key = StatsMaybeUninit::new(k, &mut self.kv_size);
            old_node.val = StatsMaybeUninit::new(v, &mut self.kv_size);

            let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
            self.detach(node_ptr);

            (Some(replaced), old_node)
        } else {
            // if the cache is not full allocate a new LruEntry
            (
                None,
                Box::<_, LruAlloc>::new_in(
                    LruEntry::new(k, v, self.cur_epoch, &mut self.kv_size),
                    self.alloc.clone(),
                ),
            )
        }
    }

    /// Pushes a key-value pair into the cache. If an entry with key `k` already exists in
    /// the cache or another cache entry is removed (due to the lru's capacity),
    /// then it returns the old entry's key-value pair. Otherwise, returns `None`.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// assert_eq!(None, cache.push(1, "a"));
    /// assert_eq!(None, cache.push(2, "b"));
    ///
    /// // This push call returns (2, "b") because that was previously 2's entry in the cache.
    /// assert_eq!(Some((2, "b")), cache.push(2, "beta"));
    ///
    /// // This push call returns (1, "a") because the cache is at capacity and 1's entry was the lru entry.
    /// assert_eq!(Some((1, "a")), cache.push(3, "alpha"));
    ///
    /// assert_eq!(cache.get(&1), None);
    /// assert_eq!(cache.get(&2), Some(&"beta"));
    /// assert_eq!(cache.get(&3), Some(&"alpha"));
    /// ```
    pub fn push(&mut self, k: K, v: V) -> Option<(K, V)> {
        self.capturing_put(k, v, true)
    }

    /// Returns a reference to the value of the key in the cache if it is
    /// present in the cache and moves the key to the head of the LRU list.
    /// If the key does not exist the provided `Fn` is used to populate the list and a reference
    /// is returned.
    ///
    /// This method will only return `None` when the capacity of the cache is 0 and no entries
    /// can be populated.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    /// cache.put(2, "c");
    /// cache.put(3, "d");
    ///
    /// assert_eq!(cache.get_or_insert(2, || "a"), Some(&"c"));
    /// assert_eq!(cache.get_or_insert(3, || "a"), Some(&"d"));
    /// assert_eq!(cache.get_or_insert(1, || "a"), Some(&"a"));
    /// assert_eq!(cache.get_or_insert(1, || "b"), Some(&"a"));
    /// ```
    pub fn get_or_insert<F>(&mut self, k: K, f: F) -> Option<&'_ V>
    where
        F: Fn() -> V,
    {
        if let Some(node) = self.map.get_mut(&k) {
            let node_ptr: *mut LruEntry<K, V> = &mut **node;

            self.detach(node_ptr);
            self.attach(node_ptr);

            Some(unsafe { &(*(*node_ptr).val.as_ptr()) as &V })
        } else {
            // If the capacity is 0 we do nothing,
            // this is the only circumstance that should return None
            if self.cap() == 0 {
                return None;
            }
            let v = f();
            let (_, mut node) = self.replace_or_create_node(k, v);

            let node_ptr: *mut LruEntry<K, V> = &mut *node;
            self.attach(node_ptr);

            let keyref = unsafe { (*node_ptr).key.as_ptr() };
            self.map.insert(KeyRef { k: keyref }, node);
            Some(unsafe { &(*(*node_ptr).val.as_ptr()) as &V })
        }
    }

    /// Removes and returns the value corresponding to the key from the cache or
    /// `None` if it does not exist.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(2, "a");
    ///
    /// assert_eq!(cache.pop(&1), None);
    /// assert_eq!(cache.pop(&2), Some("a"));
    /// assert_eq!(cache.pop(&2), None);
    /// assert_eq!(cache.len(), 0);
    /// ```
    pub fn pop<Q>(&mut self, k: &Q) -> Option<V>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.map.remove(k) {
            None => None,
            Some(mut old_node) => {
                unsafe {
                    ptr::drop_in_place(old_node.key.as_mut_ptr());
                }
                let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
                self.detach(node_ptr);
                unsafe { Some(old_node.val.assume_init(&mut self.kv_size)) }
            }
        }
    }

    /// Removes and returns the key and the value corresponding to the key from the cache or
    /// `None` if it does not exist.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "a");
    ///
    /// assert_eq!(cache.pop(&1), Some("a"));
    /// assert_eq!(cache.pop_entry(&2), Some((2, "a")));
    /// assert_eq!(cache.pop(&1), None);
    /// assert_eq!(cache.pop_entry(&2), None);
    /// assert_eq!(cache.len(), 0);
    /// ```
    pub fn pop_entry<Q>(&mut self, k: &Q) -> Option<(K, V)>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.map.remove(k) {
            None => None,
            Some(mut old_node) => {
                let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
                self.detach(node_ptr);
                unsafe {
                    Some((
                        old_node.key.assume_init(&mut self.kv_size),
                        old_node.val.assume_init(&mut self.kv_size),
                    ))
                }
            }
        }
    }

    /// Removes and returns the key and value corresponding to the least recently
    /// used item or `None` if the cache is empty.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(2, "a");
    /// cache.put(3, "b");
    /// cache.put(4, "c");
    /// cache.get(&3);
    ///
    /// assert_eq!(cache.pop_lru(), Some((4, "c")));
    /// assert_eq!(cache.pop_lru(), Some((3, "b")));
    /// assert_eq!(cache.pop_lru(), None);
    /// assert_eq!(cache.len(), 0);
    /// ```
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        let node = self.remove_last()?;
        // N.B.: Can't destructure directly because of https://github.com/rust-lang/rust/issues/28536
        let node = *node;
        let LruEntry { key, val, .. } = node;
        unsafe {
            Some((
                key.assume_init(&mut self.kv_size),
                val.assume_init(&mut self.kv_size),
            ))
        }
    }

    /// Resizes the cache. If the new capacity is smaller than the size of the current
    /// cache any entries past the new capacity are discarded.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache: LruCache<isize, &str> = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    /// cache.resize(4);
    /// cache.put(3, "c");
    /// cache.put(4, "d");
    ///
    /// assert_eq!(cache.len(), 4);
    /// assert_eq!(cache.get(&1), Some(&"a"));
    /// assert_eq!(cache.get(&2), Some(&"b"));
    /// assert_eq!(cache.get(&3), Some(&"c"));
    /// assert_eq!(cache.get(&4), Some(&"d"));
    /// ```
    pub fn resize(&mut self, cap: usize) {
        // return early if capacity doesn't change
        if cap == self.cap {
            return;
        }

        while self.map.len() > cap {
            self.pop_lru();
        }
        self.map.shrink_to_fit();

        self.cap = cap;
    }

    /// Update the current epoch. The given epoch should be greater than the current epoch.
    pub fn update_epoch(&mut self, epoch: Epoch) {
        assert!(epoch > self.cur_epoch);
        self.cur_epoch = epoch;
    }

    pub fn evict_by_epoch(&mut self, epoch: Epoch) {
        loop {
            if self.is_empty() {
                break;
            }

            let node = unsafe { (*self.tail).prev };
            let node_epoch = unsafe { (*node).epoch };
            if node_epoch < epoch {
                let old_key = KeyRef {
                    k: unsafe { &(*(*node).key.as_ptr()) },
                };
                let mut old_node = self.map.remove(&old_key).unwrap();
                unsafe {
                    old_node.key.drop_in_place(&mut self.kv_size);
                    old_node.val.drop_in_place(&mut self.kv_size);
                }
                let node_ptr: *mut LruEntry<K, V> = &mut *old_node;

                self.detach(node_ptr);
            } else {
                break;
            }
        }

        self.map.shrink_to_fit();
    }

    /// Clears the contents of the cache.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache: LruCache<isize, &str> = LruCache::new(2);
    /// assert_eq!(cache.len(), 0);
    ///
    /// cache.put(1, "a");
    /// assert_eq!(cache.len(), 1);
    ///
    /// cache.put(2, "b");
    /// assert_eq!(cache.len(), 2);
    ///
    /// cache.clear();
    /// assert_eq!(cache.len(), 0);
    /// ```
    pub fn clear(&mut self) {
        while self.pop_lru().is_some() {}
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> LruCache<K, V, S> {
    #[cfg(test)]
    fn estimate_kv_size(&self) -> usize {
        self.kv_size
    }

    /// Returns a reference to the value of the key in the cache or `None` if it is not
    /// present in the cache. Moves the key to the head of the LRU list if it exists.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    /// cache.put(2, "c");
    /// cache.put(3, "d");
    ///
    /// assert_eq!(cache.get(&1), None);
    /// assert_eq!(cache.get(&2), Some(&"c"));
    /// assert_eq!(cache.get(&3), Some(&"d"));
    /// ```
    pub fn get<'a, Q>(&'a mut self, k: &Q) -> Option<&'a V>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(node) = self.map.get_mut(k) {
            let node_ptr: *mut LruEntry<K, V> = &mut **node;

            self.detach(node_ptr);
            self.attach(node_ptr);

            Some(unsafe { &(*(*node_ptr).val.as_ptr()) as &V })
        } else {
            None
        }
    }

    /// Returns a mutable reference to the value of the key in the cache or `None` if it
    /// is not present in the cache. Moves the key to the head of the LRU list if it exists.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put("apple", 8);
    /// cache.put("banana", 4);
    /// cache.put("banana", 6);
    /// cache.put("pear", 2);
    ///
    /// assert_eq!(cache.get_mut(&"apple"), None);
    /// assert_eq!(cache.get_mut(&"banana"), Some(&mut 6));
    /// assert_eq!(cache.get_mut(&"pear"), Some(&mut 2));
    /// ```
    pub fn get_mut<'a, Q>(&'a mut self, k: &Q) -> Option<&'a mut V>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(node) = self.map.get_mut(k) {
            let node_ptr: *mut LruEntry<K, V> = &mut **node;

            self.detach(node_ptr);
            self.attach(node_ptr);

            Some(unsafe { &mut (*(*node_ptr).val.as_mut_ptr()) as &mut V })
        } else {
            None
        }
    }

    /// Returns a reference to the value corresponding to the key in the cache or `None` if it is
    /// not present in the cache. Unlike `get`, `peek` does not update the LRU list so the key's
    /// position will be unchanged.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    ///
    /// assert_eq!(cache.peek(&1), Some(&"a"));
    /// assert_eq!(cache.peek(&2), Some(&"b"));
    /// ```
    pub fn peek<'a, Q>(&'a self, k: &Q) -> Option<&'a V>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map
            .get(k)
            .map(|node| unsafe { &(*node.val.as_ptr()) as &V })
    }

    /// Returns a mutable reference to the value corresponding to the key in the cache or `None`
    /// if it is not present in the cache. Unlike `get_mut`, `peek_mut` does not update the LRU
    /// list so the key's position will be unchanged.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    ///
    /// assert_eq!(cache.peek_mut(&1), Some(&mut "a"));
    /// assert_eq!(cache.peek_mut(&2), Some(&mut "b"));
    /// ```
    pub fn peek_mut<'a, Q>(&'a mut self, k: &Q) -> Option<&'a mut V>
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.map.get_mut(k) {
            None => None,
            Some(node) => Some(unsafe { &mut (*node.val.as_mut_ptr()) as &mut V }),
        }
    }

    /// Returns the value corresponding to the least recently used item or `None` if the
    /// cache is empty. Like `peek`, `peek_lru` does not update the LRU list so the item's
    /// position will be unchanged.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    ///
    /// assert_eq!(cache.peek_lru(), Some((&1, &"a")));
    /// ```
    pub fn peek_lru<'a>(&'_ self) -> Option<(&'a K, &'a V)> {
        if self.is_empty() {
            return None;
        }

        let (key, val);
        unsafe {
            let node = (*self.tail).prev;
            key = &(*(*node).key.as_ptr()) as &K;
            val = &(*(*node).val.as_ptr()) as &V;
        }

        Some((key, val))
    }

    /// Returns a bool indicating whether the given key is in the cache. Does not update the
    /// LRU list.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    ///
    /// cache.put(1, "a");
    /// cache.put(2, "b");
    /// cache.put(3, "c");
    ///
    /// assert!(!cache.contains(&1));
    /// assert!(cache.contains(&2));
    /// assert!(cache.contains(&3));
    /// ```
    pub fn contains<Q>(&self, k: &Q) -> bool
    where
        KeyRef<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.contains_key(k)
    }

    /// Returns the number of key-value pairs that are currently in the the cache.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    /// assert_eq!(cache.len(), 0);
    ///
    /// cache.put(1, "a");
    /// assert_eq!(cache.len(), 1);
    ///
    /// cache.put(2, "b");
    /// assert_eq!(cache.len(), 2);
    ///
    /// cache.put(3, "c");
    /// assert_eq!(cache.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns a bool indicating whether the cache is empty or not.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache = LruCache::new(2);
    /// assert!(cache.is_empty());
    ///
    /// cache.put(1, "a");
    /// assert!(!cache.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }

    /// Returns the maximum number of key-value pairs the cache can hold.
    ///
    /// # Example
    ///
    /// ```
    /// use lru::LruCache;
    /// let mut cache: LruCache<isize, &str> = LruCache::new(2);
    /// assert_eq!(cache.cap(), 2);
    /// ```
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// An iterator visiting all entries in most-recently used order. The iterator element type is
    /// `(&K, &V)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lru::LruCache;
    ///
    /// let mut cache = LruCache::new(3);
    /// cache.put("a", 1);
    /// cache.put("b", 2);
    /// cache.put("c", 3);
    ///
    /// for (key, val) in cache.iter() {
    ///     println!("key: {} val: {}", key, val);
    /// }
    /// ```
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            len: self.len(),
            ptr: unsafe { (*self.head).next },
            end: unsafe { (*self.tail).prev },
            phantom: PhantomData,
        }
    }

    /// An iterator visiting all entries in most-recently-used order, giving a mutable reference on
    /// V.  The iterator element type is `(&K, &mut V)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lru::{EstimateSize, LruCache};
    ///
    /// struct HddBlock {
    ///     dirty: bool,
    ///     data: [u8; 512],
    /// }
    ///
    /// impl EstimateSize for HddBlock {
    ///     fn estimated_heap_size(&self) -> usize {
    ///         0
    ///     }
    /// }
    ///
    /// let mut cache = LruCache::new(3);
    /// cache.put(
    ///     0,
    ///     HddBlock {
    ///         dirty: false,
    ///         data: [0x00; 512],
    ///     },
    /// );
    /// cache.put(
    ///     1,
    ///     HddBlock {
    ///         dirty: true,
    ///         data: [0x55; 512],
    ///     },
    /// );
    /// cache.put(
    ///     2,
    ///     HddBlock {
    ///         dirty: true,
    ///         data: [0x77; 512],
    ///     },
    /// );
    ///
    /// // write dirty blocks to disk.
    /// for (block_id, block) in cache.iter_mut() {
    ///     if block.dirty {
    ///         // write block to disk
    ///         block.dirty = false
    ///     }
    /// }
    /// ```
    pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
        IterMut {
            len: self.len(),
            ptr: unsafe { (*self.head).next },
            end: unsafe { (*self.tail).prev },
            phantom: PhantomData,
        }
    }

    fn remove_last(&mut self) -> Option<Box<LruEntry<K, V>, LruAlloc>> {
        let prev;
        unsafe { prev = (*self.tail).prev }
        if prev != self.head {
            let old_key = KeyRef {
                k: unsafe { &(*(*(*self.tail).prev).key.as_ptr()) },
            };
            let mut old_node = self.map.remove(&old_key).unwrap();
            let node_ptr: *mut LruEntry<K, V> = &mut *old_node;
            self.detach(node_ptr);
            Some(old_node)
        } else {
            None
        }
    }

    fn detach(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            (*(*node).prev).next = (*node).next;
            (*(*node).next).prev = (*node).prev;
        }
    }

    fn attach(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            (*node).epoch = self.cur_epoch;
            (*node).next = (*self.head).next;
            (*node).prev = self.head;
            (*self.head).next = node;
            (*(*node).next).prev = node;
        }
    }
}

impl<K, V, S> Drop for LruCache<K, V, S> {
    fn drop(&mut self) {
        self.map.values_mut().for_each(|e| unsafe {
            ptr::drop_in_place(e.key.as_mut_ptr());
            ptr::drop_in_place(e.val.as_mut_ptr());
        });
        // We rebox the head/tail, and because these are maybe-uninit
        // they do not have the absent k/v dropped.
        unsafe {
            let _head = *Box::from_raw_in(self.head, self.alloc.clone());
            let _tail = *Box::from_raw_in(self.tail, self.alloc.clone());
        }
    }
}

impl<'a, K: Hash + Eq, V, S: BuildHasher> IntoIterator for &'a LruCache<K, V, S> {
    type IntoIter = Iter<'a, K, V>;
    type Item = (&'a K, &'a V);

    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}

impl<'a, K: Hash + Eq, V, S: BuildHasher> IntoIterator for &'a mut LruCache<K, V, S> {
    type IntoIter = IterMut<'a, K, V>;
    type Item = (&'a K, &'a mut V);

    fn into_iter(self) -> IterMut<'a, K, V> {
        self.iter_mut()
    }
}

// The compiler does not automatically derive Send and Sync for LruCache because it contains
// raw pointers. The raw pointers are safely encapsulated by LruCache though so we can
// implement Send and Sync for it below.
unsafe impl<K: Send, V: Send, S: Send> Send for LruCache<K, V, S> {}
unsafe impl<K: Sync, V: Sync, S: Sync> Sync for LruCache<K, V, S> {}

impl<K: Hash + Eq, V> fmt::Debug for LruCache<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LruCache")
            .field("len", &self.len())
            .field("cap", &self.cap())
            .finish()
    }
}

/// An iterator over the entries of a `LruCache`.
///
/// This `struct` is created by the [`iter`] method on [`LruCache`][`LruCache`]. See its
/// documentation for more.
///
/// [`iter`]: struct.LruCache.html#method.iter
/// [`LruCache`]: struct.LruCache.html
pub struct Iter<'a, K, V: 'a> {
    len: usize,

    ptr: *const LruEntry<K, V>,
    end: *const LruEntry<K, V>,

    phantom: PhantomData<&'a K>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        if self.len == 0 {
            return None;
        }

        let key = unsafe { &(*(*self.ptr).key.as_ptr()) as &K };
        let val = unsafe { &(*(*self.ptr).val.as_ptr()) as &V };

        self.len -= 1;
        self.ptr = unsafe { (*self.ptr).next };

        Some((key, val))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    fn count(self) -> usize {
        self.len
    }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a V)> {
        if self.len == 0 {
            return None;
        }

        let key = unsafe { &(*(*self.end).key.as_ptr()) as &K };
        let val = unsafe { &(*(*self.end).val.as_ptr()) as &V };

        self.len -= 1;
        self.end = unsafe { (*self.end).prev };

        Some((key, val))
    }
}

impl<'a, K, V> ExactSizeIterator for Iter<'a, K, V> {}
impl<'a, K, V> FusedIterator for Iter<'a, K, V> {}

impl<'a, K, V> Clone for Iter<'a, K, V> {
    fn clone(&self) -> Iter<'a, K, V> {
        Iter {
            len: self.len,
            ptr: self.ptr,
            end: self.end,
            phantom: PhantomData,
        }
    }
}

// The compiler does not automatically derive Send and Sync for Iter because it contains
// raw pointers.
unsafe impl<'a, K: Send, V: Send> Send for Iter<'a, K, V> {}
unsafe impl<'a, K: Sync, V: Sync> Sync for Iter<'a, K, V> {}

/// An iterator over mutables entries of a `LruCache`.
///
/// This `struct` is created by the [`iter_mut`] method on [`LruCache`][`LruCache`]. See its
/// documentation for more.
///
/// [`iter_mut`]: struct.LruCache.html#method.iter_mut
/// [`LruCache`]: struct.LruCache.html
pub struct IterMut<'a, K, V: 'a> {
    len: usize,

    ptr: *mut LruEntry<K, V>,
    end: *mut LruEntry<K, V>,

    phantom: PhantomData<&'a K>,
}

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<(&'a K, &'a mut V)> {
        if self.len == 0 {
            return None;
        }

        let key = unsafe { &mut (*(*self.ptr).key.as_mut_ptr()) as &mut K };
        let val = unsafe { &mut (*(*self.ptr).val.as_mut_ptr()) as &mut V };

        self.len -= 1;
        self.ptr = unsafe { (*self.ptr).next };

        Some((key, val))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    fn count(self) -> usize {
        self.len
    }
}

impl<'a, K, V> DoubleEndedIterator for IterMut<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a mut V)> {
        if self.len == 0 {
            return None;
        }

        let key = unsafe { &mut (*(*self.end).key.as_mut_ptr()) as &mut K };
        let val = unsafe { &mut (*(*self.end).val.as_mut_ptr()) as &mut V };

        self.len -= 1;
        self.end = unsafe { (*self.end).prev };

        Some((key, val))
    }
}

impl<'a, K, V> ExactSizeIterator for IterMut<'a, K, V> {}
impl<'a, K, V> FusedIterator for IterMut<'a, K, V> {}

// The compiler does not automatically derive Send and Sync for Iter because it contains
// raw pointers.
unsafe impl<'a, K: Send, V: Send> Send for IterMut<'a, K, V> {}
unsafe impl<'a, K: Sync, V: Sync> Sync for IterMut<'a, K, V> {}

/// An iterator that moves out of a `LruCache`.
///
/// This `struct` is created by the [`into_iter`] method on [`LruCache`][`LruCache`]. See its
/// documentation for more.
///
/// [`into_iter`]: struct.LruCache.html#method.into_iter
/// [`LruCache`]: struct.LruCache.html
pub struct IntoIter<K, V>
where
    K: Hash + Eq,
{
    cache: LruCache<K, V>,
}

impl<K: EstimateSize, V: EstimateSize> Iterator for IntoIter<K, V>
where
    K: Hash + Eq,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        self.cache.pop_lru()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.cache.len();
        (len, Some(len))
    }

    fn count(self) -> usize {
        self.cache.len()
    }
}

impl<K: EstimateSize, V: EstimateSize> ExactSizeIterator for IntoIter<K, V> where K: Hash + Eq {}
impl<K: EstimateSize, V: EstimateSize> FusedIterator for IntoIter<K, V> where K: Hash + Eq {}

impl<K: Hash + Eq + EstimateSize, V: EstimateSize> IntoIterator for LruCache<K, V> {
    type IntoIter = IntoIter<K, V>;
    type Item = (K, V);

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter { cache: self }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use scoped_threadpool::Pool;

    use super::{EstimateSize, LruCache};
    extern crate alloc;

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    fn assert_opt_eq_mut<V: PartialEq + Debug>(opt: Option<&mut V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    fn assert_opt_eq_tuple<K: PartialEq + Debug, V: PartialEq + Debug>(
        opt: Option<(&K, &V)>,
        kv: (K, V),
    ) {
        assert!(opt.is_some());
        let res = opt.unwrap();
        assert_eq!(res.0, &kv.0);
        assert_eq!(res.1, &kv.1);
    }

    fn assert_opt_eq_mut_tuple<K: PartialEq + Debug, V: PartialEq + Debug>(
        opt: Option<(&K, &mut V)>,
        kv: (K, V),
    ) {
        assert!(opt.is_some());
        let res = opt.unwrap();
        assert_eq!(res.0, &kv.0);
        assert_eq!(res.1, &kv.1);
    }

    #[test]
    fn test_unbounded() {
        let mut cache = LruCache::unbounded();
        for i in 0..13370 {
            cache.put(i, ());
        }
        assert_eq!(cache.len(), 13370);
    }

    #[test]
    fn test_with_hasher() {
        use hashbrown::hash_map::DefaultHashBuilder;

        let s = DefaultHashBuilder::default();
        let mut cache = LruCache::with_hasher(16, s);

        for i in 0..13370 {
            cache.put(i, ());
        }
        assert_eq!(cache.len(), 16);
    }

    #[test]
    fn test_put_and_get() {
        let mut cache = LruCache::new(2);
        assert!(cache.is_empty());

        assert_eq!(cache.put("apple", "red"), None);
        assert_eq!(cache.put("banana", "yellow"), None);

        assert_eq!(cache.cap(), 2);
        assert_eq!(cache.len(), 2);
        assert!(!cache.is_empty());
        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_put_and_get_or_insert() {
        let mut cache = LruCache::new(2);
        assert!(cache.is_empty());

        assert_eq!(cache.put("apple", "red"), None);
        assert_eq!(cache.put("banana", "yellow"), None);

        assert_eq!(cache.cap(), 2);
        assert_eq!(cache.len(), 2);
        assert!(!cache.is_empty());
        assert_opt_eq(cache.get_or_insert("apple", || "orange"), "red");
        assert_opt_eq(cache.get_or_insert("banana", || "orange"), "yellow");
        assert_opt_eq(cache.get_or_insert("lemon", || "orange"), "orange");
        assert_opt_eq(cache.get_or_insert("lemon", || "red"), "orange");
    }

    #[test]
    fn test_put_and_get_mut() {
        let mut cache = LruCache::new(2);

        cache.put("apple", "red");
        cache.put("banana", "yellow");

        assert_eq!(cache.cap(), 2);
        assert_eq!(cache.len(), 2);
        assert_opt_eq_mut(cache.get_mut(&"apple"), "red");
        assert_opt_eq_mut(cache.get_mut(&"banana"), "yellow");
    }

    #[test]
    fn test_get_mut_and_update() {
        let mut cache = LruCache::new(2);

        cache.put("apple", 1);
        cache.put("banana", 3);

        {
            let v = cache.get_mut(&"apple").unwrap();
            *v = 4;
        }

        assert_eq!(cache.cap(), 2);
        assert_eq!(cache.len(), 2);
        assert_opt_eq_mut(cache.get_mut(&"apple"), 4);
        assert_opt_eq_mut(cache.get_mut(&"banana"), 3);
    }

    #[test]
    fn test_put_update() {
        let mut cache = LruCache::new(1);

        assert_eq!(cache.put("apple", "red"), None);
        assert_eq!(cache.put("apple", "green"), Some("red"));

        assert_eq!(cache.len(), 1);
        assert_opt_eq(cache.get(&"apple"), "green");
    }

    #[test]
    fn test_put_removes_oldest() {
        let mut cache = LruCache::new(2);

        assert_eq!(cache.put("apple", "red"), None);
        assert_eq!(cache.put("banana", "yellow"), None);
        assert_eq!(cache.put("pear", "green"), None);

        assert!(cache.get(&"apple").is_none());
        assert_opt_eq(cache.get(&"banana"), "yellow");
        assert_opt_eq(cache.get(&"pear"), "green");

        // Even though we inserted "apple" into the cache earlier it has since been removed from
        // the cache so there is no current value for `put` to return.
        assert_eq!(cache.put("apple", "green"), None);
        assert_eq!(cache.put("tomato", "red"), None);

        assert!(cache.get(&"pear").is_none());
        assert_opt_eq(cache.get(&"apple"), "green");
        assert_opt_eq(cache.get(&"tomato"), "red");
    }

    #[test]
    fn test_peek() {
        let mut cache = LruCache::new(2);

        cache.put("apple", "red");
        cache.put("banana", "yellow");

        assert_opt_eq(cache.peek(&"banana"), "yellow");
        assert_opt_eq(cache.peek(&"apple"), "red");

        cache.put("pear", "green");

        assert!(cache.peek(&"apple").is_none());
        assert_opt_eq(cache.peek(&"banana"), "yellow");
        assert_opt_eq(cache.peek(&"pear"), "green");
    }

    #[test]
    fn test_peek_mut() {
        let mut cache = LruCache::new(2);

        cache.put("apple", "red");
        cache.put("banana", "yellow");

        assert_opt_eq_mut(cache.peek_mut(&"banana"), "yellow");
        assert_opt_eq_mut(cache.peek_mut(&"apple"), "red");
        assert!(cache.peek_mut(&"pear").is_none());

        cache.put("pear", "green");

        assert!(cache.peek_mut(&"apple").is_none());
        assert_opt_eq_mut(cache.peek_mut(&"banana"), "yellow");
        assert_opt_eq_mut(cache.peek_mut(&"pear"), "green");

        {
            let v = cache.peek_mut(&"banana").unwrap();
            *v = "green";
        }

        assert_opt_eq_mut(cache.peek_mut(&"banana"), "green");
    }

    #[test]
    fn test_peek_lru() {
        let mut cache = LruCache::new(2);

        assert!(cache.peek_lru().is_none());

        cache.put("apple", "red");
        cache.put("banana", "yellow");
        assert_opt_eq_tuple(cache.peek_lru(), ("apple", "red"));

        cache.get(&"apple");
        assert_opt_eq_tuple(cache.peek_lru(), ("banana", "yellow"));

        cache.clear();
        assert!(cache.peek_lru().is_none());
    }

    #[test]
    fn test_contains() {
        let mut cache = LruCache::new(2);

        cache.put("apple", "red");
        cache.put("banana", "yellow");
        cache.put("pear", "green");

        assert!(!cache.contains(&"apple"));
        assert!(cache.contains(&"banana"));
        assert!(cache.contains(&"pear"));
    }

    #[test]
    fn test_pop() {
        let mut cache = LruCache::new(2);

        cache.put("apple", "red");
        cache.put("banana", "yellow");

        assert_eq!(cache.len(), 2);
        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");

        let popped = cache.pop(&"apple");
        assert!(popped.is_some());
        assert_eq!(popped.unwrap(), "red");
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&"apple").is_none());
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_pop_entry() {
        let mut cache = LruCache::new(2);
        cache.put("apple", "red");
        cache.put("banana", "yellow");

        assert_eq!(cache.len(), 2);
        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");

        let popped = cache.pop_entry(&"apple");
        assert!(popped.is_some());
        assert_eq!(popped.unwrap(), ("apple", "red"));
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&"apple").is_none());
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_pop_lru() {
        let mut cache = LruCache::new(200);

        for i in 0..75 {
            cache.put(i, "A");
        }
        for i in 0..75 {
            cache.put(i + 100, "B");
        }
        for i in 0..75 {
            cache.put(i + 200, "C");
        }
        assert_eq!(cache.len(), 200);

        for i in 0..75 {
            assert_opt_eq(cache.get(&(74 - i + 100)), "B");
        }
        assert_opt_eq(cache.get(&25), "A");

        for i in 26..75 {
            assert_eq!(cache.pop_lru(), Some((i, "A")));
        }
        for i in 0..75 {
            assert_eq!(cache.pop_lru(), Some((i + 200, "C")));
        }
        for i in 0..75 {
            assert_eq!(cache.pop_lru(), Some((74 - i + 100, "B")));
        }
        assert_eq!(cache.pop_lru(), Some((25, "A")));
        for _ in 0..50 {
            assert_eq!(cache.pop_lru(), None);
        }
    }

    #[test]
    fn test_clear() {
        let mut cache = LruCache::new(2);

        cache.put("apple", "red");
        cache.put("banana", "yellow");

        assert_eq!(cache.len(), 2);
        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");

        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_resize_larger() {
        let mut cache = LruCache::new(2);

        cache.put(1, "a");
        cache.put(2, "b");
        cache.resize(4);
        cache.put(3, "c");
        cache.put(4, "d");

        assert_eq!(cache.len(), 4);
        assert_eq!(cache.get(&1), Some(&"a"));
        assert_eq!(cache.get(&2), Some(&"b"));
        assert_eq!(cache.get(&3), Some(&"c"));
        assert_eq!(cache.get(&4), Some(&"d"));
    }

    #[test]
    fn test_resize_smaller() {
        let mut cache = LruCache::new(4);

        cache.put(1, "a");
        cache.put(2, "b");
        cache.put(3, "c");
        cache.put(4, "d");

        cache.resize(2);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&1).is_none());
        assert!(cache.get(&2).is_none());
        assert_eq!(cache.get(&3), Some(&"c"));
        assert_eq!(cache.get(&4), Some(&"d"));
    }

    #[test]
    fn test_evict_by_epoch() {
        let mut cache = LruCache::new(4);

        cache.put(1, "a");
        cache.put(2, "b");

        cache.update_epoch(1);

        cache.put(3, "c");
        cache.put(4, "d");

        cache.evict_by_epoch(1);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&1).is_none());
        assert!(cache.get(&2).is_none());
        assert_eq!(cache.get(&3), Some(&"c"));
        assert_eq!(cache.get(&4), Some(&"d"));

        cache.evict_by_epoch(2);
        assert_eq!(cache.len(), 0);
        assert!(cache.get(&3).is_none());
        assert!(cache.get(&4).is_none());
    }

    #[test]
    fn test_no_memory_leaks_evict_by_epoch() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl EstimateSize for DropCounter {
            fn estimated_heap_size(&self) -> usize {
                0
            }
        }

        let n = 100usize;

        for _ in 0..n {
            DROP_COUNT.store(0, Ordering::SeqCst);
            let mut cache = LruCache::unbounded();
            for i in 1..n + 1 {
                cache.update_epoch(i as u64);
                cache.put(i, DropCounter {});
            }
            cache.evict_by_epoch(51);
            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 50);
        }
    }

    #[test]
    fn test_send() {
        use std::thread;

        let mut cache = LruCache::new(4);
        cache.put(1, "a");

        let handle = thread::spawn(move || {
            assert_eq!(cache.get(&1), Some(&"a"));
        });

        assert!(handle.join().is_ok());
    }

    #[test]
    fn test_multiple_threads() {
        let mut pool = Pool::new(1);
        let mut cache = LruCache::new(4);
        cache.put(1, "a");

        let cache_ref = &cache;
        pool.scoped(|scoped| {
            scoped.execute(move || {
                assert_eq!(cache_ref.peek(&1), Some(&"a"));
            });
        });

        assert_eq!((cache_ref).peek(&1), Some(&"a"));
    }

    #[test]
    fn test_iter_forwards() {
        let mut cache = LruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);

        {
            // iter const
            let mut iter = cache.iter();
            assert_eq!(iter.len(), 3);
            assert_opt_eq_tuple(iter.next(), ("c", 3));

            assert_eq!(iter.len(), 2);
            assert_opt_eq_tuple(iter.next(), ("b", 2));

            assert_eq!(iter.len(), 1);
            assert_opt_eq_tuple(iter.next(), ("a", 1));

            assert_eq!(iter.len(), 0);
            assert_eq!(iter.next(), None);
        }
        {
            // iter mut
            let mut iter = cache.iter_mut();
            assert_eq!(iter.len(), 3);
            assert_opt_eq_mut_tuple(iter.next(), ("c", 3));

            assert_eq!(iter.len(), 2);
            assert_opt_eq_mut_tuple(iter.next(), ("b", 2));

            assert_eq!(iter.len(), 1);
            assert_opt_eq_mut_tuple(iter.next(), ("a", 1));

            assert_eq!(iter.len(), 0);
            assert_eq!(iter.next(), None);
        }
    }

    #[test]
    fn test_iter_backwards() {
        let mut cache = LruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);

        {
            // iter const
            let mut iter = cache.iter();
            assert_eq!(iter.len(), 3);
            assert_opt_eq_tuple(iter.next_back(), ("a", 1));

            assert_eq!(iter.len(), 2);
            assert_opt_eq_tuple(iter.next_back(), ("b", 2));

            assert_eq!(iter.len(), 1);
            assert_opt_eq_tuple(iter.next_back(), ("c", 3));

            assert_eq!(iter.len(), 0);
            assert_eq!(iter.next_back(), None);
        }

        {
            // iter mut
            let mut iter = cache.iter_mut();
            assert_eq!(iter.len(), 3);
            assert_opt_eq_mut_tuple(iter.next_back(), ("a", 1));

            assert_eq!(iter.len(), 2);
            assert_opt_eq_mut_tuple(iter.next_back(), ("b", 2));

            assert_eq!(iter.len(), 1);
            assert_opt_eq_mut_tuple(iter.next_back(), ("c", 3));

            assert_eq!(iter.len(), 0);
            assert_eq!(iter.next_back(), None);
        }
    }

    #[test]
    fn test_iter_forwards_and_backwards() {
        let mut cache = LruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);

        {
            // iter const
            let mut iter = cache.iter();
            assert_eq!(iter.len(), 3);
            assert_opt_eq_tuple(iter.next(), ("c", 3));

            assert_eq!(iter.len(), 2);
            assert_opt_eq_tuple(iter.next_back(), ("a", 1));

            assert_eq!(iter.len(), 1);
            assert_opt_eq_tuple(iter.next(), ("b", 2));

            assert_eq!(iter.len(), 0);
            assert_eq!(iter.next_back(), None);
        }
        {
            // iter mut
            let mut iter = cache.iter_mut();
            assert_eq!(iter.len(), 3);
            assert_opt_eq_mut_tuple(iter.next(), ("c", 3));

            assert_eq!(iter.len(), 2);
            assert_opt_eq_mut_tuple(iter.next_back(), ("a", 1));

            assert_eq!(iter.len(), 1);
            assert_opt_eq_mut_tuple(iter.next(), ("b", 2));

            assert_eq!(iter.len(), 0);
            assert_eq!(iter.next_back(), None);
        }
    }

    #[test]
    fn test_iter_multiple_threads() {
        let mut pool = Pool::new(1);
        let mut cache = LruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);

        let mut iter = cache.iter();
        assert_eq!(iter.len(), 3);
        assert_opt_eq_tuple(iter.next(), ("c", 3));

        {
            let iter_ref = &mut iter;
            pool.scoped(|scoped| {
                scoped.execute(move || {
                    assert_eq!(iter_ref.len(), 2);
                    assert_opt_eq_tuple(iter_ref.next(), ("b", 2));
                });
            });
        }

        assert_eq!(iter.len(), 1);
        assert_opt_eq_tuple(iter.next(), ("a", 1));

        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_iter_clone() {
        let mut cache = LruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);

        let mut iter = cache.iter();
        let mut iter_clone = iter.clone();

        assert_eq!(iter.len(), 2);
        assert_opt_eq_tuple(iter.next(), ("b", 2));
        assert_eq!(iter_clone.len(), 2);
        assert_opt_eq_tuple(iter_clone.next(), ("b", 2));

        assert_eq!(iter.len(), 1);
        assert_opt_eq_tuple(iter.next(), ("a", 1));
        assert_eq!(iter_clone.len(), 1);
        assert_opt_eq_tuple(iter_clone.next(), ("a", 1));

        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter_clone.len(), 0);
        assert_eq!(iter_clone.next(), None);
    }

    #[test]
    fn test_into_iter() {
        let mut cache = LruCache::new(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);

        let mut iter = cache.into_iter();
        assert_eq!(iter.len(), 3);
        assert_eq!(iter.next(), Some(("a", 1)));

        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some(("b", 2)));

        assert_eq!(iter.len(), 1);
        assert_eq!(iter.next(), Some(("c", 3)));

        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_that_pop_actually_detaches_node() {
        let mut cache = LruCache::new(5);

        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        cache.put("d", 4);
        cache.put("e", 5);

        assert_eq!(cache.pop(&"c"), Some(3));

        cache.put("f", 6);

        let mut iter = cache.iter();
        assert_opt_eq_tuple(iter.next(), ("f", 6));
        assert_opt_eq_tuple(iter.next(), ("e", 5));
        assert_opt_eq_tuple(iter.next(), ("d", 4));
        assert_opt_eq_tuple(iter.next(), ("b", 2));
        assert_opt_eq_tuple(iter.next(), ("a", 1));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_get_with_borrow() {
        use alloc::string::String;

        let mut cache = LruCache::new(2);

        let key = String::from("apple");
        cache.put(key, "red");

        assert_opt_eq(cache.get("apple"), "red");
    }

    #[test]
    fn test_get_mut_with_borrow() {
        use alloc::string::String;

        let mut cache = LruCache::new(2);

        let key = String::from("apple");
        cache.put(key, "red");

        assert_opt_eq_mut(cache.get_mut("apple"), "red");
    }

    #[test]
    fn test_no_memory_leaks() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl EstimateSize for DropCounter {
            fn estimated_heap_size(&self) -> usize {
                0
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = LruCache::new(1);
            for i in 0..n {
                cache.put(i, DropCounter {});
            }
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), n * n);
    }

    #[test]
    fn test_no_memory_leaks_with_clear() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl EstimateSize for DropCounter {
            fn estimated_heap_size(&self) -> usize {
                0
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = LruCache::new(1);
            for i in 0..n {
                cache.put(i, DropCounter {});
            }
            cache.clear();
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), n * n);
    }

    #[test]
    fn test_no_memory_leaks_with_resize() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl EstimateSize for DropCounter {
            fn estimated_heap_size(&self) -> usize {
                0
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = LruCache::new(1);
            for i in 0..n {
                cache.put(i, DropCounter {});
            }
            cache.resize(0);
        }
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), n * n);
    }

    #[test]
    fn test_no_memory_leaks_with_pop() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Hash, PartialEq, Eq)]
        struct KeyDropCounter(usize);

        impl Drop for KeyDropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl EstimateSize for KeyDropCounter {
            fn estimated_heap_size(&self) -> usize {
                0
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = LruCache::new(1);

            for i in 0..100 {
                cache.put(KeyDropCounter(i), i);
                cache.pop(&KeyDropCounter(i));
            }
        }

        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), n * n * 2);
    }

    #[test]
    fn test_zero_cap_no_crash() {
        let mut cache = LruCache::new(0);
        cache.put("reizeiin", "tohka");
    }

    #[test]
    fn test_heap_memory_stats() {
        let mut cache = LruCache::new(4);
        cache.put(1, vec![1u64]);
        cache.put(2, vec![2, 2]);

        assert_eq!(cache.estimate_kv_size(), 24);
        cache.update_epoch(1);

        cache.put(3, vec![3, 3, 3]);
        cache.put(4, vec![4, 4, 4, 4]);

        assert_eq!(cache.estimate_kv_size(), 80);
        cache.evict_by_epoch(1);

        assert_eq!(cache.estimate_kv_size(), 56);

        cache.evict_by_epoch(2);
        assert_eq!(cache.estimate_kv_size(), 0);

        assert_eq!(cache.len(), 0);
    }
}
