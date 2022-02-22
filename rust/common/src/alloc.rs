use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;

use crate::error::{ErrorCode, Result};

const ALIGNMENT: usize = 1 << 6;

pub fn alloc_aligned(size: usize) -> Result<NonNull<u8>> {
    let size = if size == 0 { 1 } else { size };
    let layout = Layout::from_size_align(size, ALIGNMENT).unwrap();

    let ptr = unsafe { alloc(layout) };
    NonNull::new(ptr).ok_or_else(|| ErrorCode::MemoryError { layout }.into())
}

pub fn free_aligned(size: usize, ptr: &NonNull<u8>) {
    let size = if size == 0 { 1 } else { size };
    unsafe {
        dealloc(
            ptr.as_ptr(),
            Layout::from_size_align(size, ALIGNMENT).unwrap(),
        )
    }
}
