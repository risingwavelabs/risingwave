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

use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::marker::PhantomData;
use std::mem::{forget, ManuallyDrop};
use std::os::unix::prelude::{AsRawFd, RawFd};
use std::path::Path;

use bitvec::prelude::*;
use bytes::{Buf, BufMut};
use libc::c_void;
use nix::fcntl::{fallocate, FallocateFlags};
use nix::sys::mman::{mmap, mremap, msync, munmap, MRemapFlags, MapFlags, MsFlags, ProtFlags};
use nix::sys::stat::fstat;

use super::coding::CacheKey;
use super::error::Result;
use super::{utils, ST_BLOCK_SIZE};

const GROW_UNIT: usize = 1024 * 1024; // 1 MiB

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
        utils::u32::align_up(bsz, self.len)
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
}

/// [`MetaFile`] is a memory mapped file to record the locations of the cache file entries.
///
/// The entrie file will be memory mapped to a memory buffer.
pub struct MetaFile<K>
where
    K: CacheKey,
{
    /// File descriptor of the meta file.
    fd: RawFd,

    /// Pointer to the memory mapped buffer.
    ptr: *mut u8,
    /// Memory mapped buffer of the entire meta file.
    /// Use `ManuallyDrop` to skip `drop()` call of `Vec`, the mapped memory needs to be
    /// `unmmap(2)` manually when dropping.
    buffer: ManuallyDrop<Vec<u8>>,
    /// Meta file size in bytes.
    size: usize,

    /// Free slots list.
    free: VecDeque<usize>,
    /// Valid slots bitmap.
    valid: BitVec,

    _phantom: PhantomData<K>,
}

unsafe impl<K: CacheKey> Send for MetaFile<K> {}
unsafe impl<K: CacheKey> Sync for MetaFile<K> {}

impl<K> MetaFile<K>
where
    K: CacheKey,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut oopts = OpenOptions::new();
        oopts.create(true);
        oopts.write(true);
        oopts.read(true);
        let file = oopts.open(path)?;
        let fd = file.as_raw_fd();
        // Skip `drop()` for `file`. Close it manually after `msync(2)` and `munmap(2)`.
        forget(file);

        let stat = fstat(fd)?;
        let size = if stat.st_blocks == 0 {
            // newly created
            fallocate(fd, FallocateFlags::empty(), 0, GROW_UNIT as i64)?;
            GROW_UNIT
        } else {
            stat.st_blocks as usize * ST_BLOCK_SIZE
        };

        let (ptr, buffer) = unsafe {
            let ptr = mmap(
                std::ptr::null_mut(),
                size,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
                fd,
                0,
            )? as *mut u8;
            let buffer = ManuallyDrop::new(Vec::from_raw_parts(ptr, size, size));
            (ptr, buffer)
        };

        let mut free = VecDeque::new();
        let mut valid = bitvec![usize,Lsb0;0; size / Self::slot_info_len()];
        for slot in 0..size / Self::slot_info_len() {
            // Free if `len == 0`;
            if (&buffer[slot * Self::slot_info_len() + 4..slot * Self::slot_info_len() + 8])
                .get_u32()
                == 0
            {
                free.push_back(slot);
            } else {
                valid.set(slot, true);
            }
        }

        Ok(Self {
            fd,

            ptr,
            buffer,
            size,

            free,
            valid,

            _phantom: PhantomData,
        })
    }

    pub fn insert(&mut self, key: &K, bloc: &BlockLoc) -> Result<usize> {
        assert_ne!(bloc.len, 0);
        if self.free.is_empty() {
            self.grow()?;
        }
        let slot = self.free.pop_front().unwrap();
        self.valid.set(slot, true);

        let mut cursor = Self::slot_info_len() * slot;
        bloc.encode(&mut self.buffer[cursor..cursor + BlockLoc::encoded_len()]);
        cursor += BlockLoc::encoded_len();
        key.encode(&mut self.buffer[cursor..cursor + K::encoded_len()]);

        Ok(slot)
    }

    pub fn free(&mut self, slot: SlotId) -> Option<BlockLoc> {
        debug_assert!(
            (slot + 1) * Self::slot_info_len() <= self.size,
            "slot: {}, offset: {}, size: {}",
            slot,
            slot * Self::slot_info_len(),
            self.size
        );
        if !*self.valid.get(slot).unwrap() {
            return None;
        }
        self.valid.set(slot, false);
        self.free.push_back(slot);

        let bloc = BlockLoc::decode(
            &self.buffer[Self::slot_info_len() * slot
                ..Self::slot_info_len() * slot + BlockLoc::encoded_len()],
        );
        Some(bloc)
    }

    pub fn get(&self, slot: SlotId) -> Option<(BlockLoc, K)> {
        debug_assert!(
            (slot + 1) * Self::slot_info_len() <= self.size,
            "slot: {}, offset: {}, size: {}",
            slot,
            slot * Self::slot_info_len(),
            self.size
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

    pub fn size(&self) -> usize {
        self.size
    }

    #[inline(always)]
    fn slot_info_len() -> usize {
        BlockLoc::encoded_len() + K::encoded_len()
    }

    fn grow(&mut self) -> Result<()> {
        let old_size = self.size;
        let new_size = old_size + GROW_UNIT;

        fallocate(
            self.fd,
            FallocateFlags::empty(),
            old_size as i64,
            GROW_UNIT as i64,
        )?;
        let (ptr, buffer) = unsafe {
            let ptr = mremap(
                self.ptr as *mut c_void,
                old_size,
                new_size,
                MRemapFlags::MREMAP_MAYMOVE,
                None,
            )? as *mut u8;
            let buffer = ManuallyDrop::new(Vec::from_raw_parts(ptr, new_size, new_size));
            (ptr, buffer)
        };

        for slot in (old_size / Self::slot_info_len())..(new_size / Self::slot_info_len()) {
            self.free.push_back(slot);
        }
        self.valid.resize(new_size / Self::slot_info_len(), false);

        self.ptr = ptr;
        self.buffer = buffer;
        self.size = new_size;
        Ok(())
    }
}

impl<K> Drop for MetaFile<K>
where
    K: CacheKey,
{
    fn drop(&mut self) {
        unsafe {
            msync(self.ptr as *mut c_void, self.size, MsFlags::MS_SYNC).unwrap();
            munmap(self.ptr as *mut c_void, self.size).unwrap()
        }
        nix::unistd::close(self.fd).unwrap();
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::super::test_utils::TestCacheKey;
    use super::*;

    #[test]
    fn test_enc_dec() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = HashMap::new();
        let slot_info_len = MetaFile::<TestCacheKey>::slot_info_len();

        let mut mf: MetaFile<TestCacheKey> = MetaFile::open(dir.path().join("test-meta")).unwrap();
        assert_eq!(mf.size(), GROW_UNIT);

        let bloc = BlockLoc { bidx: 1, len: 2 };
        let key = TestCacheKey(3);
        let slot = mf.insert(&key, &bloc).unwrap();
        let (bloc0, key0) = mf.get(slot).unwrap();
        assert_eq!(bloc0, bloc);
        assert_eq!(key0, key);
        drop(mf);

        let mut mf: MetaFile<TestCacheKey> = MetaFile::open(dir.path().join("test-meta")).unwrap();
        let (bloc0, key0) = mf.get(slot).unwrap();
        assert_eq!(bloc0, bloc);
        assert_eq!(key0, key);

        map.insert(slot, (key, bloc));

        for (i, _) in (0..mf.size).step_by(slot_info_len).enumerate() {
            let i = i + 1;
            let key = TestCacheKey(i as u64);
            let bloc = BlockLoc {
                bidx: i as u32 * 2,
                len: i as u32 * 3,
            };
            let slot = mf.insert(&key, &bloc).unwrap();
            map.insert(slot, (key, bloc));
        }
        assert_eq!(mf.size(), GROW_UNIT * 2);
        for (slot, (key, bloc)) in &map {
            let (gbloc, gkey) = mf.get(*slot).unwrap();
            assert_eq!(gbloc, *bloc);
            assert_eq!(gkey, *key);
        }

        for slot in (GROW_UNIT / slot_info_len + 1)..(GROW_UNIT * 2 / slot_info_len) {
            assert_eq!(mf.get(slot), None);
        }

        for (slot, (_key, bloc)) in map.drain() {
            assert_eq!(mf.free(slot), Some(bloc));
            assert_eq!(mf.free(slot), None);
        }

        for slot in 0..(GROW_UNIT * 2 / slot_info_len) {
            assert_eq!(mf.get(slot), None);
        }
        assert_eq!(mf.free.len(), GROW_UNIT * 2 / slot_info_len);
    }
}
