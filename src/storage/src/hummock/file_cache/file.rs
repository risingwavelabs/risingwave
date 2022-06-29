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

use std::fs::{File, OpenOptions};
use std::os::unix::prelude::{AsRawFd, FileExt, OpenOptionsExt, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use nix::fcntl::{fallocate, FallocateFlags};
use nix::sys::stat::fstat;

use super::error::Result;
use super::{asyncify, utils, DioBuffer, DIO_BUFFER_ALLOCATOR, LOGICAL_BLOCK_SIZE};

const ST_BLOCK_SIZE: usize = 512;

#[derive(Clone, Debug)]
pub struct CacheFileOptions {
    pub fs_block_size: usize,
    /// NOTE: `block_size` must be a multiple of `fs_block_size`.
    pub block_size: usize,
    pub fallocate_unit: usize,
}

impl CacheFileOptions {
    fn assert(&self) {
        utils::usize::assert_pow2(LOGICAL_BLOCK_SIZE);
        utils::usize::assert_aligned(LOGICAL_BLOCK_SIZE, self.fs_block_size);
        utils::usize::assert_aligned(self.fs_block_size, self.block_size);
    }
}

struct CacheFileCore {
    file: File,
    len: AtomicUsize,
    capacity: AtomicUsize,
}

#[derive(Clone)]
pub struct CacheFile {
    _fs_block_size: usize,
    block_size: usize,
    fallocate_unit: usize,

    core: Arc<CacheFileCore>,
}

impl std::fmt::Debug for CacheFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheFile").finish()
    }
}

impl CacheFile {
    /// Opens the cache file.
    ///
    /// The underlying file is opened with `O_DIRECT` flag. All I/O requests must be aligned with
    /// the logical block size. Additionally, [`CacheFile`] requires I/O size must be a multipler of
    /// `options.block_size` (which is required to be a multipler of the file system block size).
    /// With this restriction, blocks can be directly reclaimed by the file system after hole
    /// punching.
    pub async fn open(path: impl AsRef<Path>, options: CacheFileOptions) -> Result<Self> {
        options.assert();

        let path = path.as_ref().to_owned();

        let mut oopts = OpenOptions::new();
        oopts.create(true);
        oopts.read(true);
        oopts.write(true);
        oopts.custom_flags(libc::O_DIRECT);

        let (file, len, capacity) = asyncify(move || {
            let file = oopts.open(path)?;
            let fd = file.as_raw_fd();
            let stat = fstat(fd)?;
            if stat.st_blocks == 0 {
                // newly created
                fallocate(
                    fd,
                    FallocateFlags::FALLOC_FL_KEEP_SIZE,
                    0,
                    options.fallocate_unit as i64,
                )?;
                Ok((file, 0, options.fallocate_unit))
            } else {
                // existed
                fallocate(
                    fd,
                    FallocateFlags::FALLOC_FL_KEEP_SIZE,
                    stat.st_size as i64,
                    options.fallocate_unit as i64,
                )?;
                Ok((
                    file,
                    stat.st_size as usize,
                    stat.st_size as usize + options.fallocate_unit,
                ))
            }
        })
        .await?;

        let cache_file = Self {
            _fs_block_size: options.fs_block_size,
            block_size: options.block_size,
            fallocate_unit: options.fallocate_unit,

            core: Arc::new(CacheFileCore {
                file,
                len: AtomicUsize::new(len),
                capacity: AtomicUsize::new(capacity),
            }),
        };

        Ok(cache_file)
    }

    pub async fn append(&self, buf: Arc<DioBuffer>) -> Result<()> {
        utils::usize::debug_assert_aligned(self.block_size, buf.len());

        let core = self.core.clone();
        let fallocate_unit = self.fallocate_unit;

        asyncify(move || {
            let offset = core.len.fetch_add(buf.len(), Ordering::SeqCst);
            let mut capacity = core.capacity.load(Ordering::Acquire);

            // Append the buffer will exceed the cache file allocated capacity, pre-allocate some
            // space for the cache file.
            if offset + buf.len() > capacity {
                loop {
                    match core.capacity.compare_exchange_weak(
                        capacity,
                        capacity + fallocate_unit,
                        Ordering::SeqCst,
                        Ordering::Acquire,
                    ) {
                        // Pre-allocate space in this thread.
                        Ok(_) => {
                            fallocate(
                                core.file.as_raw_fd(),
                                FallocateFlags::FALLOC_FL_KEEP_SIZE,
                                capacity as i64,
                                fallocate_unit as i64,
                            )?;
                            break;
                        }
                        Err(c) => {
                            // The cache file has been pre-allocated by another thread, skip if
                            // pre-allocated space is enough.
                            if offset + buf.len() > c {
                                break;
                            } else {
                                capacity = c;
                            }
                        }
                    }
                }
            }

            core.file.write_all_at(&buf, offset as u64)?;

            Ok(())
        })
        .await
    }

    pub async fn read(&self, offset: u64, len: usize) -> Result<DioBuffer> {
        utils::usize::debug_assert_aligned(self.block_size, len);
        let core = self.core.clone();
        asyncify(move || {
            let mut buf = DioBuffer::with_capacity_in(len, &DIO_BUFFER_ALLOCATOR);
            buf.resize(len, 0);
            core.file.read_exact_at(&mut buf, offset)?;
            Ok(buf)
        })
        .await
    }

    pub async fn sync_all(&self) -> Result<()> {
        let core = self.core.clone();
        asyncify(move || {
            core.file.sync_all()?;
            Ok(())
        })
        .await
    }

    pub async fn sync_data(&self) -> Result<()> {
        let core = self.core.clone();
        asyncify(move || {
            core.file.sync_data()?;
            Ok(())
        })
        .await
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get file length in bytes.
    ///
    /// `len()` stands for the last written byte of the file.
    pub fn len(&self) -> usize {
        self.core.len.load(Ordering::Acquire)
    }

    /// Get file pre-allocated length in bytes.
    ///
    /// `capacity()` stands for the last pre-allocated byte of the file.
    pub fn capacity(&self) -> usize {
        self.core.capacity.load(Ordering::Acquire)
    }

    /// Get file size by `stat.st_blocks * FS_BLOCK_SIZE`.
    ///
    /// `size()` stands for how much space that the file really used.
    ///
    /// `size()` can be different from `len()` because the file is sparse and pre-allocated.
    pub fn size(&self) -> usize {
        fstat(self.fd()).unwrap().st_blocks as usize * ST_BLOCK_SIZE
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }
}

impl CacheFile {
    #[inline(always)]
    fn fd(&self) -> RawFd {
        self.core.file.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<CacheFile>();
    }

    #[tokio::test]
    async fn test_file_cache() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("test-cache-file");
        let options = CacheFileOptions {
            fs_block_size: 4096,
            block_size: 4096,
            fallocate_unit: 64 * 1024 * 1024,
        };
        let cf = CacheFile::open(&path, options.clone()).await.unwrap();
        assert_eq!(cf.block_size, 4096);
        assert_eq!(cf.len(), 0);
        assert_eq!(cf.size(), 64 * 1024 * 1024);

        let mut wbuf = DioBuffer::with_capacity_in(4096, &DIO_BUFFER_ALLOCATOR);
        wbuf.extend_from_slice(&[b'x'; 4096]);
        let wbuf = Arc::new(wbuf);

        cf.append(wbuf.clone()).await.unwrap();
        assert_eq!(cf.len(), 4096);
        assert_eq!(cf.size(), 64 * 1024 * 1024);

        let rbuf = cf.read(0, 4096).await.unwrap();
        assert_eq!(&rbuf, wbuf.as_ref());

        drop(cf);

        let cf = CacheFile::open(&path, options).await.unwrap();
        assert_eq!(cf.block_size, 4096);
        assert_eq!(cf.len(), 4096);
        assert_eq!(cf.size(), 4096 + 64 * 1024 * 1024);
    }
}
