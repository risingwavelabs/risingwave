use std::hash::Hash;
use std::sync::atomic::AtomicUsize;

mod cache;
mod ghost;
mod most;
mod small;

pub trait CacheKey: Eq + Send + Hash + Clone {}
pub trait CacheValue: Send + Clone {}

pub struct CacheItem<K: CacheKey, V: CacheValue> {
    pub key: K,
    pub value: V,
    pub flag: AtomicUsize,
}

impl<K: CacheKey, V: CacheValue> CacheItem<K, V> {
    pub fn new(key: K, value: V, cost: usize) -> Self {
        Self {
            key,
            value,
            flag: AtomicUsize::new(cost << 4),
        }
    }

    #[inline(always)]
    pub fn get_freq(&self) -> usize {
        self.flag.load(std::sync::atomic::Ordering::Acquire) & 3
    }

    #[inline(always)]
    pub fn get_flag(&self) -> usize {
        self.flag.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn inc_freq(&mut self) {
        let mut flag = self.get_flag();
        while (flag & 3) < 3 {
            match self.flag.compare_exchange_weak(
                flag,
                flag + 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(v) => {
                    flag = v;
                }
            }
        }
    }

    pub fn reset_freq(&self) {
        let mut flag = self.get_flag();
        while (flag & 3) > 0 {
            let new_v = flag - (flag & 3);
            match self.flag.compare_exchange_weak(
                flag,
                new_v,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(v) => {
                    flag = v;
                }
            }
        }
    }

    pub fn dec_freq(&self) -> bool {
        let mut flag = self.get_flag();
        while (flag & 3) > 0 {
            match self.flag.compare_exchange_weak(
                flag,
                flag - 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(v) => {
                    flag = v;
                }
            }
        }
        false
    }

    fn kind(&self) -> usize {
        self.get_flag() & 12
    }

    pub fn unmark(&self) {
        let mut flag = self.get_flag();
        while (flag & 12) != 0 {
            match self.flag.compare_exchange_weak(
                flag,
                set_kind(flag, 0),
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(v) => {
                    flag = v;
                }
            }
        }
    }

    pub fn mark_most(&self) -> bool {
        let mut flag = self.get_flag();
        while (flag & 12) != 8 {
            match self.flag.compare_exchange_weak(
                flag,
                set_kind(flag, 8),
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(v) => {
                    flag = v;
                }
            }
        }
        false
    }

    pub fn mark_small(&self) -> bool {
        let mut flag = self.get_flag();
        while (flag & 12) != 4 {
            match self.flag.compare_exchange_weak(
                flag,
                set_kind(flag, 4),
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(v) => {
                    flag = v;
                }
            }
        }
        false
    }

    pub fn mark_ghost(&self) -> bool {
        let mut flag = self.get_flag();
        while (flag & 12) != 12 {
            match self.flag.compare_exchange_weak(
                flag,
                set_kind(flag, 12),
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(v) => {
                    flag = v;
                }
            }
        }
        false
    }

    pub fn cost(&self) -> usize {
        self.flag.load(std::sync::atomic::Ordering::Acquire) >> 4
    }
}

const KIND_MASK: usize = usize::MAX - 12;
fn set_kind(flag: usize, val: usize) -> usize {
    (flag & KIND_MASK) | val
}
