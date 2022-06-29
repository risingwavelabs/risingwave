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

use std::fs::OpenOptions;
use std::marker::PhantomData;
use std::mem::{forget, ManuallyDrop};
use std::os::unix::prelude::{AsRawFd, RawFd};
use std::path::Path;

use bitvec::prelude::*;
use bytes::{Buf, BufMut};
use libc::c_void;
use nix::fcntl::{fallocate, FallocateFlags};
use nix::sys::mman::{mmap, msync, munmap, MapFlags, MsFlags, ProtFlags};

use super::coding::CacheKey;
use super::error::Result;

pub type SlotId = usize;

#[derive(PartialEq, Eq, Debug)]
pub struct BlockLoc {
    /// block index in cache file
    pub bidx: u32,
    /// data len in bytes
    pub len: u32,
}

impl BlockLoc {
    /// block count in cache file
    #[inline(always)]
    pub fn blen(&self, bsz: u32) -> u32 {
        u32_align_up(bsz, self.len)
    }

    #[inline(always)]
    pub fn encoded_len() -> usize {
        8
    }

    #[inline(always)]
    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.bidx);
        buf.put_u32(self.len);
    }

    #[inline(always)]
    fn decode(mut buf: &[u8]) -> Self {
        let bidx = buf.get_u32();
        let len = buf.get_u32();
        Self { bidx, len }
    }

    #[inline(always)]
    fn is_valid(&self) -> bool {
        self.len != 0
    }
}

/// [`MetaFile`] is a memory mapped file to record the locations of the cache file entries.
///
/// The entrie file will be memory mapped to a memory buffer.
pub struct MetaFile<K>
where
    K: CacheKey,
{
    /// Meta file capacity in bytes.
    capacity: usize,
    /// Total slots of the meta file.
    slots: usize,

    /// File descriptor of the meta file.
    fd: RawFd,

    /// Pointer to the memory mapped buffer.
    ptr: *mut u8,
    /// Memory mapped buffer of the entire meta file.
    /// Use `ManuallyDrop` to skip `drop()` call of `Vec`, the mapped memory needs to be
    /// `unmmap(2)` manually when dropping.
    buffer: ManuallyDrop<Vec<u8>>,

    /// Valid slots bitmap, to fasten free slot seeking.
    valid: BitVec,

    _phantom: PhantomData<K>,
}

unsafe impl<K: CacheKey> Send for MetaFile<K> {}
unsafe impl<K: CacheKey> Sync for MetaFile<K> {}

impl<K> MetaFile<K>
where
    K: CacheKey,
{
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        let mut oopts = OpenOptions::new();
        oopts.create(true);
        oopts.write(true);
        oopts.read(true);
        let file = oopts.open(path)?;
        let fd = file.as_raw_fd();
        // Skip `drop()` for `file`. Close it manually after `msync(2)` and `munmap(2)`.
        forget(file);

        fallocate(fd, FallocateFlags::empty(), 0, capacity as i64)?;

        let (ptr, buffer) = unsafe {
            let ptr = mmap(
                std::ptr::null_mut(),
                capacity,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
                fd,
                0,
            )? as *mut u8;
            let buffer = ManuallyDrop::new(Vec::from_raw_parts(ptr, capacity, capacity));
            (ptr, buffer)
        };

        let slots = capacity / Self::slot_info_len();
        let mut valid = bitvec![usize,Lsb0;0;slots];

        let mut cursor = 0;
        for slot in 0..slots {
            valid.set(slot, (&buffer[cursor..cursor + 4]).get_u32() != 0);
            cursor += BlockLoc::encoded_len() + K::encoded_len();
        }

        Ok(Self {
            capacity,
            slots,

            fd,

            ptr,
            buffer,

            valid,

            _phantom: PhantomData,
        })
    }

    pub fn set(&mut self, slot: SlotId, bloc: &BlockLoc, key: &K) {
        debug_assert!(
            slot < self.slots,
            "slot: {}, self.slots: {}",
            slot,
            self.slots
        );
        let mut cursor = Self::slot_info_len() * slot;
        bloc.encode(&mut self.buffer[cursor..cursor + BlockLoc::encoded_len()]);
        cursor += BlockLoc::encoded_len();
        key.encode(&mut self.buffer[cursor..cursor + K::encoded_len()]);
        self.valid.set(slot, bloc.is_valid());
    }

    pub fn get(&self, slot: SlotId) -> Option<(BlockLoc, K)> {
        debug_assert!(
            slot < self.slots,
            "slot: {}, self.slots: {}",
            slot,
            self.slots
        );
        if !*self.valid.get(slot).unwrap() {
            return None;
        }
        let mut cursor = Self::slot_info_len() * slot;
        let bloc = BlockLoc::decode(&self.buffer[cursor..cursor + BlockLoc::encoded_len()]);
        cursor += BlockLoc::encoded_len();
        let key = K::decode(&self.buffer[cursor..cursor + K::encoded_len()]);
        Some((bloc, key))
    }

    fn slot_info_len() -> usize {
        BlockLoc::encoded_len() + K::encoded_len()
    }
}

impl<K> Drop for MetaFile<K>
where
    K: CacheKey,
{
    fn drop(&mut self) {
        unsafe {
            msync(self.ptr as *mut c_void, self.capacity, MsFlags::MS_SYNC).unwrap();
            munmap(self.ptr as *mut c_void, self.capacity).unwrap()
        }
        nix::unistd::close(self.fd).unwrap();
    }
}

#[inline(always)]
fn u32_align_up(align: u32, v: u32) -> u32 {
    (v + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {

    use super::super::test_utils::TestCacheKey;
    use super::*;

    #[test]
    fn test_enc_dec() {
        const SIZE: usize = 4 * 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();

        let mut mf: MetaFile<TestCacheKey> =
            MetaFile::open(dir.path().join("test-meta-001"), SIZE).unwrap();

        let bloc = BlockLoc { bidx: 1, len: 2 };
        let key = TestCacheKey(3);
        mf.set(0, &bloc, &key);
        let (bloc0, key0) = mf.get(0).unwrap();
        assert_eq!(bloc0, bloc);
        assert_eq!(key0, key);
        drop(mf);

        let mf: MetaFile<TestCacheKey> =
            MetaFile::open(dir.path().join("test-meta-001"), SIZE).unwrap();
        let (bloc0, key0) = mf.get(0).unwrap();
        assert_eq!(bloc0, bloc);
        assert_eq!(key0, key);
        drop(mf);
    }
}
