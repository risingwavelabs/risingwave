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

//! `LruCache` implementation port from github.com/facebook/rocksdb. The class `LruCache` is
//! thread-safe, because every operation on cache will be protected by a spin lock.
use std::ptr::null_mut;
use std::sync::Arc;

use spin::Mutex;

const IN_CACHE: u8 = 1;
const REVERSE_IN_CACHE: u8 = !IN_CACHE;

/// An entry is a variable length heap-allocated structure.
/// Entries are referenced by cache and/or by any external entity.
/// The cache keeps all its entries in a hash table. Some elements
/// are also stored on LRU list.
///
/// `LruHandle` can be in these states:
/// 1. Referenced externally AND in hash table.
///    In that case the entry is *not* in the LRU list
///    (`refs` >= 1 && `in_cache` == true)
/// 2. Not referenced externally AND in hash table.
///    In that case the entry is in the LRU list and can be freed.
///    (`refs` == 0 && `in_cache` == true)
/// 3. Referenced externally AND not in hash table.
///    In that case the entry is not in the LRU list and not in hash table.
///    The entry can be freed when refs becomes 0.
///    (`refs` >= 1 && `in_cache` == false)
///
/// All newly created `LruHandle`s are in state 1. If you call
/// `LruCacheShard::release` on entry in state 1, it will go into state 2.
/// To move from state 1 to state 3, either call `LruCacheShard::erase` or
/// `LruCacheShard::insert` with the same key (but possibly different value).
/// To move from state 2 to state 1, use `LruCacheShard::lookup`.
/// Before destruction, make sure that no handles are in state 1. This means
/// that any successful `LruCacheShard::lookup/LruCacheShard::insert` have a
/// matching `LruCache::release` (to move into state 2) or `LruCacheShard::erase`
/// (to move into state 3).
pub struct LruHandle<K: PartialEq + Default, T> {
    // next element in the linked-list of hash bucket, only used by hash-table.
    next_hash: *mut LruHandle<K, T>,

    // next element in LRU linked list
    next: *mut LruHandle<K, T>,

    // prev element in LRU linked list
    prev: *mut LruHandle<K, T>,

    key: K,
    hash: u64,
    value: Option<T>,
    charge: usize,
    refs: u32,
    flags: u8,
}

impl<K: PartialEq + Default, T> Default for LruHandle<K, T> {
    fn default() -> Self {
        Self {
            next_hash: null_mut(),
            next: null_mut(),
            prev: null_mut(),
            key: K::default(),
            hash: 0,
            value: None,
            charge: 0,
            refs: 0,
            flags: 0,
        }
    }
}

impl<K: PartialEq + Default, T> LruHandle<K, T> {
    pub fn new(key: K, value: Option<T>) -> Self {
        Self {
            key,
            hash: 0,
            value,
            next_hash: null_mut(),
            prev: null_mut(),
            next: null_mut(),
            charge: 0,
            flags: 0,
            refs: 0,
        }
    }

    fn set_in_cache(&mut self, in_cache: bool) {
        if in_cache {
            self.flags |= IN_CACHE;
        } else {
            self.flags &= REVERSE_IN_CACHE;
        }
    }

    fn add_ref(&mut self) {
        self.refs += 1;
    }

    fn unref(&mut self) -> bool {
        self.refs -= 1;
        self.refs == 0
    }

    fn has_refs(&self) -> bool {
        self.refs > 0
    }

    fn is_in_cache(&self) -> bool {
        (self.flags & IN_CACHE) > 0
    }
}

unsafe impl<K: PartialEq + Default, T> Send for LruHandle<K, T> {}

pub struct LruHandleTable<K: PartialEq + Default, T> {
    list: Vec<*mut LruHandle<K, T>>,
    elems: usize,
}

impl<K: PartialEq + Default, T> LruHandleTable<K, T> {
    fn new() -> Self {
        Self {
            list: vec![null_mut(); 16],
            elems: 0,
        }
    }

    unsafe fn find_pointer(
        &self,
        idx: usize,
        key: &K,
    ) -> (*mut LruHandle<K, T>, *mut LruHandle<K, T>) {
        let mut ptr = self.list[idx];
        let mut prev = null_mut();
        while !ptr.is_null() && !(*ptr).key.eq(key) {
            prev = ptr;
            ptr = (*ptr).next_hash;
        }
        (prev, ptr)
    }

    unsafe fn remove(&mut self, hash: u64, key: &K) -> *mut LruHandle<K, T> {
        let idx = (hash as usize) & (self.list.len() - 1);
        let (mut prev, ptr) = self.find_pointer(idx, key);
        if ptr.is_null() {
            return null_mut();
        }
        if prev.is_null() {
            self.list[idx] = (*ptr).next_hash;
        } else {
            (*prev).next_hash = (*ptr).next_hash;
        }
        self.elems -= 1;
        ptr
    }

    unsafe fn insert(&mut self, hash: u64, h: *mut LruHandle<K, T>) -> *mut LruHandle<K, T> {
        debug_assert!(self.list.len().is_power_of_two());
        let idx = (hash as usize) & (self.list.len() - 1);
        let (mut prev, ptr) = self.find_pointer(idx, &(*h).key);
        if prev.is_null() {
            self.list[idx] = h;
        } else {
            (*prev).next_hash = h;
        }

        if !ptr.is_null() {
            debug_assert!((*ptr).key.eq(&(*h).key));
            (*h).next_hash = (*ptr).next_hash;
            return ptr;
        } else {
            (*h).next_hash = ptr;
            debug_assert!(ptr.is_null());
        }
        self.elems += 1;
        if self.elems > self.list.len() {
            self.resize();
        }
        null_mut()
    }

    unsafe fn lookup(&self, hash: u64, key: &K) -> *mut LruHandle<K, T> {
        let idx = (hash as usize) & (self.list.len() - 1);
        let (_, ptr) = self.find_pointer(idx, key);
        ptr
    }

    unsafe fn resize(&mut self) {
        let mut l = std::cmp::max(16, self.list.len());
        let next_capacity = self.elems * 3 / 2;
        while l < next_capacity {
            l <<= 1;
        }
        let mut count = 0;
        let mut new_list = vec![null_mut(); l];
        for head in self.list.drain(..) {
            let mut handle = head;
            while !handle.is_null() {
                let idx = (*handle).hash as usize & (l - 1);
                let next = (*handle).next_hash;
                (*handle).next_hash = new_list[idx];
                new_list[idx] = handle;
                handle = next;
                count += 1;
            }
        }
        assert_eq!(count, self.elems);
        self.list = new_list;
    }
}

pub struct LruCacheShard<K: PartialEq + Default, T> {
    lru: Box<LruHandle<K, T>>,
    table: LruHandleTable<K, T>,
    object_pool: Vec<Box<LruHandle<K, T>>>,
    lru_usage: usize,
    usage: usize,
    capacity: usize,
}

unsafe impl<K: PartialEq + Default, T> Send for LruCacheShard<K, T> {}
unsafe impl<K: PartialEq + Default, T> Sync for LruCacheShard<K, T> {}

impl<K: PartialEq + Default, T> LruCacheShard<K, T> {
    fn new(capacity: usize, object_capacity: usize) -> Self {
        let mut lru = Box::new(LruHandle::new(K::default(), None));
        lru.prev = lru.as_mut();
        lru.next = lru.as_mut();
        let mut object_pool = Vec::with_capacity(object_capacity);
        for _ in 0..object_capacity {
            object_pool.push(Box::new(LruHandle::default()));
        }
        Self {
            capacity,
            lru_usage: 0,
            usage: 0,
            object_pool,
            lru,
            table: LruHandleTable::new(),
        }
    }

    unsafe fn lru_remove(&mut self, e: *mut LruHandle<K, T>) {
        (*(*e).next).prev = (*e).prev;
        (*(*e).prev).next = (*e).next;
        (*e).prev = null_mut();
        (*e).next = null_mut();
        self.lru_usage -= (*e).charge;
    }

    // insert entry in the end of the linked-list
    unsafe fn lru_insert(&mut self, e: *mut LruHandle<K, T>) {
        (*e).next = self.lru.as_mut();
        (*e).prev = self.lru.prev;
        (*(*e).prev).next = e;
        (*(*e).next).prev = e;
        self.lru_usage += (*e).charge;
    }

    unsafe fn evict_from_lru(&mut self, charge: usize, last_reference_list: &mut Vec<T>) {
        while self.usage + charge > self.capacity && !std::ptr::eq(self.lru.next, self.lru.as_mut())
        {
            let old_ptr = self.lru.next;
            self.table.remove((*old_ptr).hash, &(*old_ptr).key);
            assert!((*old_ptr).is_in_cache());
            self.lru_remove(old_ptr);
            (*old_ptr).set_in_cache(false);
            self.usage -= (*old_ptr).charge;
            let mut node = Box::from_raw(old_ptr);
            let data = node.value.take().unwrap();
            last_reference_list.push(data);
            self.recyle_handle_object(node);
        }
    }

    fn recyle_handle_object(&mut self, mut node: Box<LruHandle<K, T>>) {
        if self.object_pool.len() < self.object_pool.capacity() {
            node.next_hash = null_mut();
            node.next = null_mut();
            node.prev = null_mut();
            self.object_pool.push(node);
        }
    }

    unsafe fn insert(
        &mut self,
        key: K,
        hash: u64,
        charge: usize,
        value: T,
        last_reference_list: &mut Vec<T>,
    ) -> *mut LruHandle<K, T> {
        let mut handle = if let Some(mut h) = self.object_pool.pop() {
            h.key = key;
            h.value = Some(value);
            h
        } else {
            Box::new(LruHandle::new(key, Some(value)))
        };
        handle.charge = charge;
        handle.hash = hash;
        handle.refs = 0;
        handle.flags = 0;
        handle.set_in_cache(true);
        self.evict_from_lru(charge, last_reference_list);
        let ptr = Box::into_raw(handle);
        let old = self.table.insert(hash, ptr);
        if !old.is_null() {
            if let Some(data) = self.remove_cache_handle(old) {
                last_reference_list.push(data);
            }
        }
        self.usage += charge;
        (*ptr).add_ref();
        ptr
    }

    unsafe fn release(&mut self, e: *mut LruHandle<K, T>) -> Option<T> {
        if e.is_null() {
            return None;
        }
        let last_reference = (*e).unref();
        if last_reference && (*e).is_in_cache() {
            if self.usage > self.capacity {
                self.table.remove((*e).hash, &(*e).key);
                (*e).set_in_cache(false);
            } else {
                self.lru_insert(e);
                return None;
            }
        }
        if last_reference {
            let mut node = Box::from_raw(e);
            self.usage -= node.charge;
            let data = node.value.take().unwrap();
            self.recyle_handle_object(node);
            Some(data)
        } else {
            None
        }
    }

    unsafe fn lookup(&mut self, hash: u64, key: &K) -> *mut LruHandle<K, T> {
        let e = self.table.lookup(hash, key);
        if !e.is_null() {
            if !(*e).has_refs() {
                self.lru_remove(e);
            }
            (*e).add_ref();
        }
        e
    }

    unsafe fn erase(&mut self, hash: u64, key: &K) -> Option<T> {
        let h = self.table.remove(hash, key);
        if !h.is_null() {
            self.remove_cache_handle(h)
        } else {
            None
        }
    }

    unsafe fn remove_cache_handle(&mut self, h: *mut LruHandle<K, T>) -> Option<T> {
        (*h).set_in_cache(false);
        if !(*h).has_refs() {
            self.lru_remove(h);
            let mut node = Box::from_raw(h);
            node.set_in_cache(false);
            self.usage -= node.charge;
            let data = node.value.take();
            self.recyle_handle_object(node);
            return data;
        }
        None
    }
}

pub struct LruCache<K: PartialEq + Default, T> {
    shards: Vec<Mutex<LruCacheShard<K, T>>>,
}

impl<K: PartialEq + Default, T> LruCache<K, T> {
    pub fn new(num_shard_bits: usize, capacity: usize, object_cache: usize) -> Self {
        let num_shards = 1 << num_shard_bits;
        let mut shards = Vec::with_capacity(num_shards);
        let per_shard = capacity / num_shards;
        let per_shard_object = object_cache / num_shards;
        for _ in 0..num_shards {
            shards.push(Mutex::new(LruCacheShard::new(per_shard, per_shard_object)));
        }
        Self { shards }
    }

    pub fn lookup(self: &Arc<Self>, hash: u64, key: &K) -> Option<CachableEntry<K, T>> {
        let mut shard = self.shards[self.shard(hash)].lock();
        unsafe {
            let ptr = shard.lookup(hash, key);
            if ptr.is_null() {
                return None;
            }
            let entry = CachableEntry {
                cache: self.clone(),
                handle: ptr,
            };
            Some(entry)
        }
    }

    unsafe fn release(&self, handle: *mut LruHandle<K, T>) {
        let data = {
            let mut shard = self.shards[self.shard((*handle).hash)].lock();
            shard.release(handle)
        };
        // do not deallocate data with holding mutex.
        drop(data);
    }

    unsafe fn add_ref(&self, handle: *mut LruHandle<K, T>) {
        let _shard = self.shards[self.shard((*handle).hash)].lock();
        (*handle).refs += 1;
    }

    pub fn insert(
        self: &Arc<Self>,
        key: K,
        hash: u64,
        charge: usize,
        value: T,
    ) -> CachableEntry<K, T> {
        let mut to_delete = vec![];
        let handle = unsafe {
            let mut shard = self.shards[self.shard(hash)].lock();
            let ptr = shard.insert(key, hash, charge, value, &mut to_delete);
            debug_assert!(!ptr.is_null());
            CachableEntry::<K, T> {
                cache: self.clone(),
                handle: ptr,
            }
        };
        to_delete.clear();
        handle
    }

    pub fn erase(&self, hash: u64, key: &K) {
        let data = unsafe {
            let mut shard = self.shards[self.shard(hash)].lock();
            shard.erase(hash, key)
        };
        drop(data);
    }

    pub fn get_memory_usage(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().usage).sum()
    }

    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.shards.len()
    }
}

pub struct CachableEntry<K: PartialEq + Default, T> {
    cache: Arc<LruCache<K, T>>,
    handle: *mut LruHandle<K, T>,
}

unsafe impl<K: PartialEq + Default, T> Send for CachableEntry<K, T> {}
unsafe impl<K: PartialEq + Default, T> Sync for CachableEntry<K, T> {}

impl<K: PartialEq + Default, T> CachableEntry<K, T> {
    pub fn value(&self) -> &T {
        unsafe { (*self.handle).value.as_ref().unwrap() }
    }
}

impl<K: PartialEq + Default, T> Clone for CachableEntry<K, T> {
    fn clone(&self) -> Self {
        unsafe {
            self.cache.add_ref(self.handle);
            Self {
                cache: self.cache.clone(),
                handle: self.handle,
            }
        }
    }
}

impl<K: PartialEq + Default, T> Drop for CachableEntry<K, T> {
    fn drop(&mut self) {
        unsafe {
            self.cache.release(self.handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::Arc;

    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};

    use super::*;
    use crate::hummock::cache::{LruHandle, IN_CACHE};
    use crate::hummock::LruCache;

    pub struct Block {
        pub offset: u64,
        pub sst: u64,
    }

    #[test]
    fn test_cache_handle_basic() {
        println!("not in cache: {}, in cache {}", REVERSE_IN_CACHE, IN_CACHE);
        println!(
            "cache shard size: {}, cache size {}",
            std::mem::size_of::<Mutex<LruCacheShard<u32, String>>>(),
            std::mem::size_of::<LruCache<String, String>>()
        );
        let mut h = Box::new(LruHandle::new(1, Some(2)));
        h.set_in_cache(true);
        assert!(h.is_in_cache());
        h.set_in_cache(false);
        assert!(!h.is_in_cache());
    }

    #[test]
    fn test_cache_shard() {
        let cache = Arc::new(LruCache::<(u64, u64), Block>::new(2, 256, 16));
        assert_eq!(cache.shard(0), 0);
        assert_eq!(cache.shard(1), 1);
        assert_eq!(cache.shard(10), 2);
    }

    #[test]
    fn test_cache_basic() {
        let cache = Arc::new(LruCache::<(u64, u64), Block>::new(2, 256, 16));
        let seed = 10244021u64;
        let mut rng = SmallRng::seed_from_u64(seed);
        for _ in 0..100000 {
            let block_offset = rng.next_u64() % 1024;
            let sst = rng.next_u64() % 1024;
            let mut hasher = DefaultHasher::new();
            sst.hash(&mut hasher);
            block_offset.hash(&mut hasher);
            let h = hasher.finish();
            if let Some(block) = cache.lookup(h, &(sst, block_offset)) {
                assert_eq!(block.value().offset, block_offset);
                drop(block);
                continue;
            }
            cache.insert(
                (sst, block_offset),
                h,
                1,
                Block {
                    offset: block_offset,
                    sst,
                },
            );
        }
        assert_eq!(256, cache.get_memory_usage());
    }

    fn validate_lru_list(cache: &mut LruCacheShard<String, String>, keys: Vec<&str>) {
        unsafe {
            let mut lru: *mut LruHandle<String, String> = cache.lru.as_mut();
            for k in keys {
                lru = (*lru).next;
                assert!(
                    (*lru).key.eq(k),
                    "compare failed: {} vs {}, get value: {:?}",
                    (*lru).key,
                    k,
                    (*lru).value
                );
            }
        }
    }

    fn create_cache(capacity: usize) -> LruCacheShard<String, String> {
        LruCacheShard::new(capacity, capacity)
    }

    fn lookup(cache: &mut LruCacheShard<String, String>, key: &str) -> bool {
        unsafe {
            let h = cache.lookup(0, &key.to_string());
            let exist = !h.is_null();
            if exist {
                assert!((*h).key.eq(key));
                cache.release(h);
            }
            exist
        }
    }

    fn insert(cache: &mut LruCacheShard<String, String>, key: &str, value: &str) {
        let mut free_list = vec![];
        unsafe {
            let handle = cache.insert(
                key.to_string(),
                0,
                value.len(),
                value.to_string(),
                &mut free_list,
            );
            cache.release(handle);
        }
        free_list.clear();
    }

    #[test]
    fn test_basic_lru() {
        let mut cache = create_cache(5);
        let keys = vec!["a", "b", "c", "d", "e"];
        for &k in &keys {
            insert(&mut cache, k, k);
        }
        validate_lru_list(&mut cache, keys);
        for k in ["x", "y", "z"] {
            insert(&mut cache, k, k);
        }
        validate_lru_list(&mut cache, vec!["d", "e", "x", "y", "z"]);
        assert!(!lookup(&mut cache, "b"));
        assert!(lookup(&mut cache, "e"));
        validate_lru_list(&mut cache, vec!["d", "x", "y", "z", "e"]);
        assert!(lookup(&mut cache, "z"));
        validate_lru_list(&mut cache, vec!["d", "x", "y", "e", "z"]);
        unsafe {
            let h = cache.erase(0, &"x".to_string());
            assert!(h.is_some());
            validate_lru_list(&mut cache, vec!["d", "y", "e", "z"]);
        }
        assert!(lookup(&mut cache, "d"));
        validate_lru_list(&mut cache, vec!["y", "e", "z", "d"]);
        insert(&mut cache, "u", "u");
        validate_lru_list(&mut cache, vec!["y", "e", "z", "d", "u"]);
        insert(&mut cache, "v", "v");
        validate_lru_list(&mut cache, vec!["e", "z", "d", "u", "v"]);
    }

    #[test]
    fn test_refrence_and_usage() {
        let mut cache = create_cache(5);
        insert(&mut cache, "k1", "a");
        assert_eq!(cache.usage, 1);
        insert(&mut cache, "k0", "aa");
        assert_eq!(cache.usage, 3);
        insert(&mut cache, "k1", "aa");
        assert_eq!(cache.usage, 4);
        insert(&mut cache, "k2", "aa");
        assert_eq!(cache.usage, 4);
        let mut free_list = vec![];
        validate_lru_list(&mut cache, vec!["k1", "k2"]);
        unsafe {
            let h1 = cache.lookup(0, &"k1".to_string());
            assert!(!h1.is_null());
            let h2 = cache.lookup(0, &"k2".to_string());
            assert!(!h2.is_null());

            let h3 = cache.insert("k3".to_string(), 0, 2, "bb".to_string(), &mut free_list);
            assert_eq!(cache.usage, 6);
            assert!(!h3.is_null());
            let h4 = cache.lookup(0, &"k1".to_string());
            assert!(!h4.is_null());

            cache.release(h1);
            assert_eq!(cache.usage, 6);
            cache.release(h4);
            assert_eq!(cache.usage, 4);

            cache.release(h3);
            cache.release(h2);

            validate_lru_list(&mut cache, vec!["k3", "k2"]);
        }
    }
}
