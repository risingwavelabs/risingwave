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

use std::ptr::null_mut;
use std::sync::Arc;

use spin::Mutex;

const IN_CACHE: u8 = 1;
const REVERSE_IN_CACHE: u8 = !IN_CACHE;

pub struct LRUHandle<K: PartialEq + Default, T> {
    next_hash: *mut LRUHandle<K, T>,
    next: *mut LRUHandle<K, T>,
    prev: *mut LRUHandle<K, T>,
    key: K,
    hash: u64,
    value: Option<T>,
    charge: usize,
    refs: u32,
    flags: u8,
}

impl<K: PartialEq + Default, T> LRUHandle<K, T> {
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

unsafe impl<K: PartialEq + Default, T> Send for LRUHandle<K, T> {}

pub struct LRUHandleTable<K: PartialEq + Default, T> {
    list: Vec<*mut LRUHandle<K, T>>,
    elems: usize,
}

impl<K: PartialEq + Default, T> LRUHandleTable<K, T> {
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
    ) -> (*mut LRUHandle<K, T>, *mut LRUHandle<K, T>) {
        let mut ptr = self.list[idx];
        let mut prev = null_mut();
        while !ptr.is_null() && !(*ptr).key.eq(key) {
            prev = ptr;
            ptr = (*ptr).next_hash;
        }
        (prev, ptr)
    }

    unsafe fn remove(&mut self, hash: u64, key: &K) -> *mut LRUHandle<K, T> {
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

    unsafe fn insert(&mut self, hash: u64, h: *mut LRUHandle<K, T>) -> *mut LRUHandle<K, T> {
        let idx = (hash as usize) & (self.list.len() - 1);
        let (mut prev, ptr) = self.find_pointer(idx, &(*h).key);
        if prev.is_null() {
            let idx = (hash as usize) & (self.list.len() - 1);
            self.list[idx] = h;
        } else {
            (*prev).next_hash = h;
        }
        if !ptr.is_null() && (*ptr).key.eq(&(*h).key) {
            (*h).next_hash = (*ptr).next_hash;
            return ptr;
        } else {
            (*h).next_hash = ptr;
            assert!(ptr.is_null());
        }
        self.elems += 1;
        if self.elems > self.list.len() {
            self.resize();
        }
        null_mut()
    }

    unsafe fn lookup(&self, hash: u64, key: &K) -> *mut LRUHandle<K, T> {
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

pub struct LRUCacheShard<K: PartialEq + Default, T> {
    lru: Box<LRUHandle<K, T>>,
    table: LRUHandleTable<K, T>,
    object_pool: Vec<Box<LRUHandle<K, T>>>,
    lru_usage: usize,
    usage: usize,
    capacity: usize,
}
unsafe impl<K: PartialEq + Default, T> Send for LRUCacheShard<K, T> {}
unsafe impl<K: PartialEq + Default, T> Sync for LRUCacheShard<K, T> {}

impl<K: PartialEq + Default, T> LRUCacheShard<K, T> {
    fn new(capacity: usize, object_capacity: usize) -> Self {
        let mut lru = Box::new(LRUHandle::new(K::default(), None));
        lru.prev = lru.as_mut();
        lru.next = lru.as_mut();
        Self {
            capacity,
            lru_usage: 0,
            usage: 0,
            object_pool: Vec::with_capacity(object_capacity),
            lru,
            table: LRUHandleTable::new(),
        }
    }

    unsafe fn lru_remove(&mut self, e: *mut LRUHandle<K, T>) {
        (*(*e).next).prev = (*e).prev;
        (*(*e).prev).next = (*e).next;
        (*e).prev = null_mut();
        (*e).next = null_mut();
        self.lru_usage -= (*e).charge;
    }

    unsafe fn lru_insert(&mut self, e: *mut LRUHandle<K, T>) {
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
            let tmp = self.table.remove((*old_ptr).hash, &(*old_ptr).key);
            if tmp.is_null() {
                println!("remove hash: {}", (*old_ptr).hash);
            }
            assert!((*old_ptr).is_in_cache());
            assert!(!tmp.is_null());
            self.lru_remove(old_ptr);
            (*old_ptr).set_in_cache(false);
            self.usage -= (*old_ptr).charge;
            let mut node = Box::from_raw(old_ptr);
            let data = node.value.take().unwrap();
            last_reference_list.push(data);
            self.recyle_handle_object(node);
        }
    }

    fn recyle_handle_object(&mut self, mut node: Box<LRUHandle<K, T>>) {
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
    ) -> *mut LRUHandle<K, T> {
        let mut handle = if let Some(mut h) = self.object_pool.pop() {
            h.key = key;
            h.value = Some(value);
            h
        } else {
            Box::new(LRUHandle::new(key, Some(value)))
        };
        handle.charge = charge;
        handle.hash = hash;
        handle.refs = 0;
        handle.flags = 0;
        handle.set_in_cache(true);
        self.evict_from_lru(charge, last_reference_list);
        if self.usage + charge > self.capacity {
            handle.set_in_cache(false);
            let data = handle.value.take().unwrap();
            last_reference_list.push(data);
            self.recyle_handle_object(handle);
            null_mut()
        } else {
            let ptr = Box::into_raw(handle);
            let old = self.table.insert(hash, ptr);
            if !old.is_null() {
                (*old).set_in_cache(false);
                if !(*old).has_refs() {
                    self.lru_remove(old);
                    let mut node = Box::from_raw(old);
                    self.usage -= node.charge;
                    let data = node.value.take().unwrap();
                    last_reference_list.push(data);
                    self.recyle_handle_object(node);
                }
            }
            self.usage += charge;
            (*ptr).add_ref();
            ptr
        }
    }

    unsafe fn release(&mut self, e: *mut LRUHandle<K, T>) -> Option<T> {
        if e.is_null() {
            return None;
        }
        let last_reference = (*e).unref();
        if last_reference && (*e).is_in_cache() {
            if self.usage > self.capacity {
                println!("remove {}", (*e).hash);
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

    unsafe fn lookup(&mut self, hash: u64, key: &K) -> *mut LRUHandle<K, T> {
        let e = self.table.lookup(hash, key);
        if !e.is_null() {
            if !(*e).has_refs() {
                self.lru_remove(e);
            }
            (*e).add_ref();
        }
        e
    }
}

pub struct LRUCache<K: PartialEq + Default, T> {
    num_shard_bits: usize,
    shards: Vec<Mutex<LRUCacheShard<K, T>>>,
}

impl<K: PartialEq + Default, T> LRUCache<K, T> {
    pub fn new(num_shard_bits: usize, capacity: usize, object_cache: usize) -> Self {
        let num_shards = 1 << num_shard_bits;
        let mut shards = Vec::with_capacity(num_shards);
        let per_shard = capacity / num_shards;
        let per_shard_object = object_cache / num_shards;
        for _ in 0..num_shards {
            shards.push(Mutex::new(LRUCacheShard::new(per_shard, per_shard_object)));
        }
        Self {
            shards,
            num_shard_bits,
        }
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

    pub fn release(&self, handle: *mut LRUHandle<K, T>) {
        let data = unsafe {
            let mut shard = self.shards[self.shard((*handle).hash)].lock();
            shard.release(handle)
        };
        // do not deallocate data with holding mutex.
        drop(data);
    }

    pub fn insert(
        self: &Arc<Self>,
        key: K,
        hash: u64,
        charge: usize,
        value: T,
    ) -> Option<CachableEntry<K, T>> {
        let mut to_delete = vec![];
        let handle = unsafe {
            let mut shard = self.shards[self.shard(hash)].lock();
            let ptr = shard.insert(key, hash, charge, value, &mut to_delete);
            if ptr.is_null() {
                None
            } else {
                Some(CachableEntry::<K, T> {
                    cache: self.clone(),
                    handle: ptr,
                })
            }
        };
        to_delete.clear();
        handle
    }

    fn shard(&self, hash: u64) -> usize {
        if self.num_shard_bits > 0 {
            (hash >> (64 - self.num_shard_bits)) as usize
        } else {
            0
        }
    }
}

pub struct CachableEntry<K: PartialEq + Default, T> {
    cache: Arc<LRUCache<K, T>>,
    handle: *mut LRUHandle<K, T>,
}

unsafe impl<K: PartialEq + Default, T> Send for CachableEntry<K, T> {}
unsafe impl<K: PartialEq + Default, T> Sync for CachableEntry<K, T> {}

impl<K: PartialEq + Default, T> CachableEntry<K, T> {
    pub fn value(&self) -> &T {
        unsafe { (*self.handle).value.as_ref().unwrap() }
    }
}

impl<K: PartialEq + Default, T> Drop for CachableEntry<K, T> {
    fn drop(&mut self) {
        self.cache.release(self.handle);
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
    use crate::hummock::cache::{LRUHandle, IN_CACHE};
    use crate::hummock::LRUCache;
    pub struct Block {
        pub offset: u64,
        pub sst: u64,
    }
    #[test]
    fn test_cache_handle_basic() {
        println!("not in cache: {}, in cache {}", REVERSE_IN_CACHE, IN_CACHE);
        let mut h = Box::new(LRUHandle::new(1, Some(2)));
        h.set_in_cache(true);
        assert!(h.is_in_cache());
        h.set_in_cache(false);
        assert!(!h.is_in_cache());
    }

    #[test]
    fn test_cache_basic() {
        let cache = Arc::new(LRUCache::<(u64, u64), Block>::new(2, 256, 16));
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
    }
}
