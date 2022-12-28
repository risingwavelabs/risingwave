use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::{mem, ptr, u32};

use rand::Rng;

use super::arena::Arena;
pub const MAX_HEIGHT: usize = 20;

const HEIGHT_INCREASE: u32 = u32::MAX / 3;

pub trait Key: Send + Ord + Default {}
impl<T: Send + Ord + Default> Key for T {}

pub trait Value: Send + Sync + Default + Eq {}
impl<T: Send + Sync + Default + Eq> Value for T {}

// Uses C layout to make sure tower is at the bottom
#[derive(Debug)]
#[repr(C)]
pub struct Node<K: Key, V: Value> {
    key: K,
    value: V,
    height: usize,
    // PrevList for fast reverse scan.
    prev: AtomicPtr<Node<K, V>>,
    tower: [AtomicPtr<Node<K, V>>; MAX_HEIGHT],
}

impl<K: Key, V: Value> Node<K, V> {
    fn alloc(arena: &Arena, key: K, value: V, height: usize) -> *mut Node<K, V> {
        let size = mem::size_of::<Node<K, V>>();
        // Not all values in Node::tower will be utilized.
        let not_used = (MAX_HEIGHT as usize - height as usize - 1) * mem::size_of::<AtomicUsize>();
        unsafe {
            let node_addr = arena.allocate(size - not_used);
            let node_ptr: *mut Node<K, V> = node_addr as _;
            let node = &mut *node_ptr;
            ptr::write(&mut node.key, key);
            ptr::write(&mut node.value, value);
            node.height = height;
            ptr::write_bytes(node.tower.as_mut_ptr(), 0, height + 1);
            node_ptr
        }
    }

    fn next_offset(&self, height: usize) -> *mut Node<K, V> {
        self.tower[height].load(Ordering::SeqCst)
    }
}

struct SkiplistInner<K: Key, V: Value> {
    height: AtomicUsize,
    head: NonNull<Node<K, V>>,
    arena: Arena,
    allow_concurrent_write: bool,
}

#[derive(Clone)]
pub struct Skiplist<K: Key, V: Value> {
    inner: Arc<SkiplistInner<K, V>>,
}

impl<K: Key, V: Value> Skiplist<K, V> {
    pub fn new(allow_concurrent_write: bool) -> Skiplist<K, V> {
        let arena = Arena::new();
        let head_offset = Node::alloc(&arena, K::default(), V::default(), MAX_HEIGHT - 1);
        let head = unsafe { NonNull::new_unchecked(head_offset) };
        Skiplist {
            inner: Arc::new(SkiplistInner {
                height: AtomicUsize::new(0),
                head,
                arena,
                allow_concurrent_write,
            }),
        }
    }
}

impl<K: Key, V: Value> Drop for SkiplistInner<K, V> {
    fn drop(&mut self) {
        let mut node = self.head.as_ptr();
        loop {
            let next_ptr = unsafe { (&*node).next_offset(0) };
            if !next_ptr.is_null() {
                unsafe {
                    ptr::drop_in_place(node);
                }
                node = next_ptr;
                continue;
            }
            unsafe { ptr::drop_in_place(node) };
            return;
        }
    }
}

impl<K: Key, V: Value> SkiplistInner<K, V> {
    fn random_height(&self) -> usize {
        let mut rng = rand::thread_rng();
        for h in 0..(MAX_HEIGHT - 1) {
            if !rng.gen_ratio(HEIGHT_INCREASE, u32::MAX) {
                return h;
            }
        }
        MAX_HEIGHT - 1
    }

    fn height(&self) -> usize {
        self.height.load(Ordering::SeqCst)
    }

    /// Finds the node near to key.
    ///
    /// If less=true, it finds rightmost node such that node.key < key (if allow_equal=false) or
    /// node.key <= key (if allow_equal=true).
    /// If less=false, it finds leftmost node such that node.key > key (if allow_equal=false) or
    /// node.key >= key (if allow_equal=true).
    ///
    /// Returns the node found. The bool returned is true if the node has key equal to given key.
    unsafe fn find_near(&self, key: &K, less: bool, allow_equal: bool) -> *const Node<K, V> {
        let mut cursor: *const Node<K, V> = self.head.as_ptr();
        let mut level = self.height();
        loop {
            let next_addr = (&*cursor).next_offset(level);
            if next_addr.is_null() {
                if level > 0 {
                    level -= 1;
                    continue;
                }
                if !less || cursor == self.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }
            let next_ptr: *mut Node<K, V> = next_addr as _;
            let next = &*next_ptr;
            let res = key.cmp(&next.key);
            if res == std::cmp::Ordering::Greater {
                cursor = next_ptr;
                continue;
            }
            if res == std::cmp::Ordering::Equal {
                if allow_equal {
                    return next;
                }
                if !less {
                    let offset = next.next_offset(0);
                    return offset;
                }
                if level > 0 {
                    level -= 1;
                    continue;
                }
                if cursor == self.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }
            if level > 0 {
                level -= 1;
                continue;
            }
            if !less {
                return next;
            }
            if cursor == self.head.as_ptr() {
                return ptr::null();
            }
            return cursor;
        }
    }

    /// Returns (nodeBefore, nodeAfter) with nodeBefore.key <= key <= nodeAfter.key.
    ///
    /// The input "before" tells us where to start looking.
    /// If we found a node with the same key, then we return nodeBefore = nodeAfter.
    /// Otherwise, nodeBefore.key < key < nodeAfter.key.
    unsafe fn find_splice_for_level(
        &self,
        key: &K,
        mut before: *mut Node<K, V>,
        level: usize,
    ) -> (*mut Node<K, V>, *mut Node<K, V>) {
        loop {
            let next_ptr = (&*before).next_offset(level);
            if next_ptr.is_null() {
                return (before, ptr::null_mut());
            }
            let next_node = &*next_ptr;
            match key.cmp(&next_node.key) {
                std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                std::cmp::Ordering::Less => return (before, next_ptr),
                _ => before = next_ptr,
            }
        }
    }

    /// Insert the key value pair to skiplist.
    ///
    /// Returns None if the insertion success.
    /// Returns Some(key, vaule) when insertion failed. This happens when the key already exists and
    /// the existed value not equal to the value passed to this function, returns the passed key and
    /// value.
    pub fn put(&self, key: K, value: V) -> Option<(K, V)> {
        let mut list_height = self.height();
        let mut prev = [ptr::null_mut(); MAX_HEIGHT + 1];
        let mut next = [ptr::null_mut(); MAX_HEIGHT + 1];
        prev[list_height + 1] = self.head.as_ptr();
        next[list_height + 1] = ptr::null_mut();
        for i in (0..=list_height).rev() {
            let (p, n) = unsafe { self.find_splice_for_level(&key, prev[i + 1], i) };
            prev[i] = p;
            next[i] = n;
            if p == n {
                unsafe {
                    if (*p).value != value {
                        return Some((key, value));
                    }
                }
                return None;
            }
        }

        let height = self.random_height();
        let node_offset = Node::alloc(&self.arena, key, value, height);
        if self.allow_concurrent_write {
            while height > list_height {
                match self.height.compare_exchange_weak(
                    list_height,
                    height,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(h) => list_height = h,
                }
            }
        } else {
            // There is no need to use CAS for single thread writing.
            if height > list_height {
                self.height.store(height, Ordering::Relaxed);
            }
        }

        let x: &mut Node<K, V> = unsafe { &mut *node_offset };
        for i in 0..=height {
            if self.allow_concurrent_write {
                loop {
                    if prev[i].is_null() {
                        assert!(i > 1);
                        let (p, n) =
                            unsafe { self.find_splice_for_level(&x.key, self.head.as_ptr(), i) };
                        prev[i] = p;
                        next[i] = n;
                        assert_ne!(p, n);
                    }
                    let next_ptr = next[i];
                    x.tower[i].store(next_ptr, Ordering::SeqCst);
                    match unsafe { &*prev[i] }.tower[i].compare_exchange(
                        next_ptr,
                        node_offset,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(_) => {
                            let (p, n) = unsafe { self.find_splice_for_level(&x.key, prev[i], i) };
                            if p == n {
                                assert_eq!(i, 0);
                                if unsafe { &*p }.value != x.value {
                                    let key = mem::replace(&mut x.key, K::default());
                                    let value = mem::replace(&mut x.value, V::default());
                                    return Some((key, value));
                                }
                                unsafe {
                                    ptr::drop_in_place(x);
                                }
                                return None;
                            }
                            prev[i] = p;
                            next[i] = n;
                        }
                    }
                }
            } else {
                // There is no need to use CAS for single thread writing.
                if prev[i].is_null() {
                    assert!(i > 1);
                    let (p, n) =
                        unsafe { self.find_splice_for_level(&x.key, self.head.as_ptr(), i) };
                    prev[i] = p;
                    next[i] = n;
                    assert_ne!(p, n);
                }
                // Construct the PrevList for level 0.
                if i == 0 {
                    let prev_ptr = prev[0];
                    x.prev.store(prev_ptr, Ordering::Relaxed);
                    if !next[i].is_null() {
                        unsafe { &*next[i] }
                            .prev
                            .store(node_offset, Ordering::Release);
                    }
                }
                // Construct the NextList for level i.
                let next_ptr = next[i];
                x.tower[i].store(next_ptr, Ordering::Relaxed);
                unsafe { &*prev[i] }.tower[i].store(node_offset, Ordering::Release);
            }
        }
        None
    }

    // pub fn is_empty(&self) -> bool {
    //     let node = self.head.as_ptr();
    //     let next_offset = unsafe { (&*node).next_offset(0) };
    //     next_offset == 0
    // }
    //
    // pub fn len(&self) -> usize {
    //     let mut node = self.head.as_ptr();
    //     let mut count = 0;
    //     loop {
    //         let next = unsafe { (&*node).next_offset(0) };
    //         if next != 0 {
    //             count += 1;
    //             node = unsafe { self.arena.get_mut(next) };
    //             continue;
    //         }
    //         return count;
    //     }
    // }

    fn find_last(&self) -> *const Node<K, V> {
        let mut node = self.head.as_ptr();
        let mut level = self.height();
        loop {
            let next = unsafe { (&*node).next_offset(level) };
            if !next.is_null() {
                node = next;
                continue;
            }
            if level == 0 {
                if node == self.head.as_ptr() {
                    return ptr::null();
                }
                return node;
            }
            level -= 1;
        }
    }
}
impl<K: Key, V: Value> Skiplist<K, V> {
    pub fn put(&self, key: K, value: V) -> Option<(K, V)> {
        self.inner.put(key, value)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if let Some((_, value)) = self.get_with_key(key) {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_with_key(&self, key: &K) -> Option<(&K, &V)> {
        let node = unsafe { self.inner.find_near(key, false, true) };
        if node.is_null() {
            return None;
        }
        if key.eq(&unsafe { &*node }.key) {
            return Some(unsafe { (&(*node).key, &(*node).value) });
        }
        None
    }

    pub fn iter(&self) -> IterRef<K, V> {
        IterRef {
            list: self.inner.clone(),
            cursor: ptr::null(),
        }
    }

    pub fn mem_size(&self) -> usize {
        unimplemented!("mem_size")
    }
}

unsafe impl<K: Key, V: Value> Send for Node<K, V> {}
unsafe impl<K: Key, V: Value> Sync for Node<K, V> {}
unsafe impl<K: Key, V: Value> Send for SkiplistInner<K, V> {}
unsafe impl<K: Key, V: Value> Sync for SkiplistInner<K, V> {}

pub struct IterRef<K: Key, V: Value> {
    list: Arc<SkiplistInner<K, V>>,
    cursor: *const Node<K, V>,
}

unsafe impl<K: Key, V: Value> Send for IterRef<K, V> {}
unsafe impl<K: Key, V: Value> Sync for IterRef<K, V> {}

impl<K: Key, V: Value> IterRef<K, V> {
    pub fn valid(&self) -> bool {
        !self.cursor.is_null()
    }

    pub fn key(&self) -> &K {
        assert!(self.valid());
        unsafe { &(*self.cursor).key }
    }

    pub fn value(&self) -> &V {
        assert!(self.valid());
        unsafe { &(*self.cursor).value }
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            self.cursor = (&*self.cursor).next_offset(0);
        }
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        if self.list.allow_concurrent_write {
            unsafe {
                self.cursor = self.list.as_ref().find_near(self.key(), true, false);
            }
        } else {
            unsafe {
                let node = (*self.cursor).prev.load(Ordering::Acquire);
                if node != self.list.head.as_ptr() {
                    self.cursor = node;
                } else {
                    self.cursor = ptr::null();
                }
            }
        }
    }

    pub fn seek(&mut self, target: &K) {
        unsafe {
            self.cursor = self.list.find_near(target, false, true);
        }
    }

    pub fn seek_for_prev(&mut self, target: &K) {
        unsafe {
            self.cursor = self.list.as_ref().find_near(target, true, true);
        }
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            self.cursor = (&*self.list.head.as_ptr()).next_offset(0);
        }
    }

    pub fn seek_to_last(&mut self) {
        self.cursor = self.list.as_ref().find_last();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn with_skl_test(allow_concurrent_write: bool, f: impl FnOnce(Skiplist<Bytes, Bytes>)) {
        let list: Skiplist<Bytes, Bytes> = Skiplist::new(allow_concurrent_write);
        f(list);
    }

    fn test_find_near_imp(allow_concurrent_write: bool) {
        with_skl_test(allow_concurrent_write, |list| {
            for i in 0..1000 {
                let key = Bytes::from(format!("{:05}", i * 10 + 5));
                let value = Bytes::from(format!("{:05}", i));
                list.put(key, value);
            }
            let mut cases = vec![
                ("00001", false, false, Some("00005")),
                ("00001", false, true, Some("00005")),
                ("00001", true, false, None),
                ("00001", true, true, None),
                ("00005", false, false, Some("00015")),
                ("00005", false, true, Some("00005")),
                ("00005", true, false, None),
                ("00005", true, true, Some("00005")),
                ("05555", false, false, Some("05565")),
                ("05555", false, true, Some("05555")),
                ("05555", true, false, Some("05545")),
                ("05555", true, true, Some("05555")),
                ("05558", false, false, Some("05565")),
                ("05558", false, true, Some("05565")),
                ("05558", true, false, Some("05555")),
                ("05558", true, true, Some("05555")),
                ("09995", false, false, None),
                ("09995", false, true, Some("09995")),
                ("09995", true, false, Some("09985")),
                ("09995", true, true, Some("09995")),
                ("59995", false, false, None),
                ("59995", false, true, None),
                ("59995", true, false, Some("09995")),
                ("59995", true, true, Some("09995")),
            ];
            for (i, (key, less, allow_equal, exp)) in cases.drain(..).enumerate() {
                let seek_key = Bytes::from(format!("{}", key));
                let res = unsafe { list.inner.find_near(&seek_key, less, allow_equal) };
                if exp.is_none() {
                    assert!(res.is_null(), "{}", i);
                    continue;
                }
                let e = format!("{}", exp.unwrap());
                assert_eq!(&unsafe { &*res }.key, e.as_bytes(), "{}", i);
            }
        });
    }

    #[test]
    fn test_skl_find_near() {
        // test_find_near_imp(true);
        test_find_near_imp(false);
    }
}
