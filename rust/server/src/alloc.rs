use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::Layout;
use std::ptr::NonNull;

use crate::error::ErrorCode;
use crate::error::Result;

const ALIGNMENT: usize = 1 << 6;

pub(crate) fn alloc_aligned(size: usize) -> Result<NonNull<u8>> {
    let layout = unsafe { Layout::from_size_align_unchecked(size, ALIGNMENT) };

    let ptr = unsafe { alloc(layout) };
    NonNull::new(ptr).ok_or_else(|| ErrorCode::MemoryError { layout }.into())
}

pub(crate) fn free_aligned(size: usize, ptr: &NonNull<u8>) {
    unsafe {
        dealloc(
            ptr.as_ptr(),
            Layout::from_size_align_unchecked(size, ALIGNMENT),
        )
    }
}
