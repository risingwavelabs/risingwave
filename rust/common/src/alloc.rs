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
//
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
