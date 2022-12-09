#![feature(allocator_api)]
#![cfg(loom)]

// use task_stats_alloc::*;
use std::alloc::System;
use std::borrow::BorrowMut;
use std::hint::black_box;
use std::ptr::NonNull;
use std::time::Duration;

use loom::sync::atomic::{fence, AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;
use task_stats_alloc::{allocation_stat, BYTES_ALLOCATED};
use tokio::runtime::Handle;

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct TaskLocalBytesAllocated(Option<NonNull<AtomicUsize>>);

impl Default for TaskLocalBytesAllocated {
    fn default() -> Self {
        Self(Some(
            NonNull::new(Box::leak(Box::new_in(0.into(), System))).unwrap(),
        ))
    }
}

impl TaskLocalBytesAllocated {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an invalid counter.
    pub const fn invalid() -> Self {
        Self(None)
    }

    /// Adds to the current counter.
    #[inline(always)]
    pub(crate) fn add(&self, val: usize) {
        if let Some(bytes) = self.0 {
            let bytes_ref = unsafe { bytes.as_ref() };
            bytes_ref.fetch_add(val, Ordering::Relaxed);
        }
    }

    /// Subtracts from the counter value, and `drop` the counter while the count reaches zero.
    #[inline(always)]
    pub(crate) fn sub(&self, val: usize, atomic: Arc<AtomicUsize>) {
        if let Some(bytes) = self.0 {
            let bytes_ref = unsafe { bytes.as_ref() };
            // Use Release to synchronize with the below deletion.
            let old_bytes = bytes_ref.fetch_sub(val, Ordering::Relaxed);
            // If the counter reaches zero, delete the counter. Note that we've ensured there's no
            // zero deltas in `wrap_layout`, so there'll be no more uses of the counter.
            if old_bytes == val {
                // No fence here. Atomic add to avoid
                atomic.fetch_add(1, Ordering::Relaxed);
                unsafe { Box::from_raw_in(bytes.as_ptr(), System) };
            }
        }
    }

    #[inline(always)]
    pub fn val(&self) -> usize {
        let bytes_ref = self.0.as_ref().expect("bytes is invalid");
        let bytes_ref = unsafe { bytes_ref.as_ref() };
        bytes_ref.load(Ordering::Relaxed)
    }
}

#[test]
fn test_to_avoid_double_drop() {
    loom::model(|| {
        let bytes_num = 3;
        let mut num = Arc::new(TaskLocalBytesAllocated(Some(
            NonNull::new(Box::leak(Box::new_in(bytes_num.into(), System))).unwrap(),
        )));

        // Add the flag value when counter drop so we can observe.
        let flag_num = Arc::new(AtomicUsize::new(0));

        let ths: Vec<_> = (0..bytes_num)
            .map(|_| {
                let num = num.clone();
                let flag_num = flag_num.clone();
                thread::spawn(move || {
                    num.sub(1, flag_num);
                })
            })
            .collect();

        for th in ths {
            th.join().unwrap();
        }

        // Ensure the counter is dropped.
        assert_eq!(flag_num.load(Ordering::Relaxed), 1);
    });
}
