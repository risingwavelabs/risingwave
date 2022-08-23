use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::task_local;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct TaskLocalBytesAllocated(Option<&'static AtomicUsize>);

impl TaskLocalBytesAllocated {
    pub fn new() -> Self {
        Self(Some(Box::leak(Box::new_in(AtomicUsize::new(0), System))))
    }

    pub const fn invalid() -> Self {
        Self(None)
    }

    #[inline(always)]
    pub fn add(&self, val: usize) {
        if let Some(bytes) = self.0 {
            bytes.fetch_add(val, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    pub unsafe fn add_unchecked(&self, val: usize) {
        self.0.unwrap_unchecked().fetch_add(val, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn sub(&self, val: usize) {
        if let Some(bytes) = self.0 {
            bytes.fetch_sub(val, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    pub fn val(&self) -> usize {
        self.0.as_ref().unwrap().load(Ordering::Relaxed)
    }
}

task_local! {
    pub static BYTES_ALLOCATED: TaskLocalBytesAllocated;
}

#[inline(always)]
fn wrap_layout(layout: Layout) -> (Layout, usize) {
    let (wrapped_layout, offset) = Layout::new::<TaskLocalBytesAllocated>()
        .extend(layout)
        .expect("wrapping layout overflow");
    let wrapped_layout = wrapped_layout.pad_to_align();

    (wrapped_layout, offset)
}

pub struct TaskLocalAlloc<A>(pub A);

unsafe impl<A> GlobalAlloc for TaskLocalAlloc<A>
where
    A: GlobalAlloc,
{
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (wrapped_layout, offset) = wrap_layout(layout);

        BYTES_ALLOCATED
            .try_with(|&bytes| {
                bytes.add_unchecked(layout.size());
                let ptr = self.0.alloc(wrapped_layout);
                *ptr.cast() = bytes;
                ptr.wrapping_add(offset)
            })
            .unwrap_or_else(|_| {
                let ptr = self.0.alloc(wrapped_layout);
                *ptr.cast() = TaskLocalBytesAllocated::invalid();
                ptr.wrapping_add(offset)
            })
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let (wrapped_layout, offset) = wrap_layout(layout);
        let ptr = ptr.wrapping_sub(offset);

        let bytes: TaskLocalBytesAllocated = *ptr.cast();
        bytes.sub(layout.size());

        self.0.dealloc(ptr, wrapped_layout);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let (wrapped_layout, offset) = wrap_layout(layout);

        BYTES_ALLOCATED
            .try_with(|&bytes| {
                bytes.add_unchecked(layout.size());
                let ptr = self.0.alloc_zeroed(wrapped_layout);
                *ptr.cast() = bytes;
                ptr.wrapping_add(offset)
            })
            .unwrap_or_else(|_| {
                let ptr = self.0.alloc_zeroed(wrapped_layout);
                *ptr.cast() = TaskLocalBytesAllocated::invalid();
                ptr.wrapping_add(offset)
            })
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (wrapped_layout, offset) = wrap_layout(layout);
        let ptr = ptr.wrapping_sub(offset);

        let bytes: TaskLocalBytesAllocated = *ptr.cast();
        bytes.add(new_size);
        bytes.sub(layout.size());

        let ptr = self.0.realloc(ptr, wrapped_layout, new_size + offset);
        if ptr.is_null() {
            ptr
        } else {
            *ptr.cast() = bytes;
            ptr.wrapping_add(offset)
        }
    }
}
