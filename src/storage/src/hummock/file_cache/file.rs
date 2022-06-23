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
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Buf;
use nix::fcntl::{fallocate, FallocateFlags};
use nix::sys::stat::fstat;
use nix::unistd::ftruncate;

use super::error::{Error, Result};
use super::{asyncify, DioBuffer, DIO_BUFFER_ALLOCATOR, LOGICAL_BLOCK_SIZE};

const ST_BLOCK_SIZE: usize = 512;

const MAGIC: &[u8] = b"hummock-cache-file";
const VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct CacheFileOptions {
    pub dir: String,
    pub id: u64,

    pub fs_block_size: usize,
    /// NOTE: `block_size` must be a multiple of `fs_block_size`.
    pub block_size: usize,
    pub meta_blocks: usize,
    pub fallocate_unit: usize,
}

impl CacheFileOptions {
    fn assert(&self) {
        assert_pow2(LOGICAL_BLOCK_SIZE);
        assert_alignment(LOGICAL_BLOCK_SIZE, self.fs_block_size);
        assert_alignment(self.fs_block_size, self.block_size);
    }
}

struct CacheFileCore {
    file: std::fs::File,
    len: AtomicUsize,
    capacity: AtomicUsize,
}

/// # Format
///
/// ```plain
/// header block (1 bs, < logical block size used)
///
/// | MAGIC | version | block size | meta blocks |
///
/// meta blocks ({meta blocks} bs)
///
/// | slot 0 index | slot 1 index |   ...   | padding | (1 bs)
/// | slot i index | slot i + 1 index | ... | padding | (1 bs)
/// ...
///
/// data blocks
///
/// | slot 0 data | slot 1 data | ... |
/// ```
#[derive(Clone)]
pub struct CacheFile {
    dir: String,
    id: u64,

    pub fs_block_size: usize,
    pub block_size: usize,
    pub meta_blocks: usize,
    pub fallocate_unit: usize,

    core: Arc<CacheFileCore>,
}

impl std::fmt::Debug for CacheFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheFile")
            .field(
                "path",
                &PathBuf::from(self.dir.as_str()).join(filename(self.id)),
            )
            .finish()
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
    ///
    /// Steps:
    ///
    /// 1. open the underlying file
    /// 2. (a) write header block if newly created
    ///    (b) read header block if exists
    /// 3. read meta blocks to [`DioBuffer`] (TODO)
    /// 4. pre-allocate space
    pub async fn open(options: CacheFileOptions) -> Result<Self> {
        options.assert();

        // 1.
        let path = PathBuf::from(options.dir.as_str()).join(filename(options.id));
        let mut oopts = OpenOptions::new();
        oopts.create(true);
        oopts.read(true);
        oopts.write(true);
        oopts.custom_flags(libc::O_DIRECT);

        let (file, block_size, meta_blocks, len, capacity, _buffer) = asyncify(move || {
            let file = oopts.open(path)?;
            let fd = file.as_raw_fd();
            let stat = fstat(fd)?;
            if stat.st_blocks == 0 {
                // 2a.
                write_header(&file, options.block_size, options.meta_blocks)?;
                // 3.
                let meta_len = options.block_size * options.meta_blocks;
                let mut buffer = DioBuffer::with_capacity_in(meta_len, &DIO_BUFFER_ALLOCATOR);
                buffer.resize(meta_len, 0);
                ftruncate(fd, (options.block_size * (1 + options.meta_blocks)) as i64)?;
                // 4.
                fallocate(
                    fd,
                    FallocateFlags::FALLOC_FL_KEEP_SIZE,
                    0,
                    options.fallocate_unit as i64,
                )?;
                Ok((
                    file,
                    options.block_size,
                    options.meta_blocks,
                    (options.block_size * (1 + options.meta_blocks)) as usize,
                    options.fallocate_unit,
                    buffer,
                ))
            } else {
                // 2b.
                let (block_size, meta_blocks) = read_header(&file)?;
                // 3.
                let meta_len = options.block_size * options.meta_blocks;
                let mut buffer =
                    DioBuffer::with_capacity_in(block_size * meta_blocks, &DIO_BUFFER_ALLOCATOR);
                buffer.resize(meta_len, 0);
                file.read_exact_at(&mut buffer, block_size as u64)?;
                // 4.
                fallocate(
                    fd,
                    FallocateFlags::FALLOC_FL_KEEP_SIZE,
                    stat.st_size as i64,
                    options.fallocate_unit as i64,
                )?;
                Ok((
                    file,
                    block_size,
                    meta_blocks,
                    stat.st_size as usize,
                    stat.st_size as usize + options.fallocate_unit,
                    buffer,
                ))
            }
        })
        .await?;

        Ok(Self {
            dir: options.dir,
            id: options.id,

            fs_block_size: options.fs_block_size,
            block_size,
            meta_blocks,
            fallocate_unit: options.fallocate_unit,

            core: Arc::new(CacheFileCore {
                file,
                len: AtomicUsize::new(len),
                capacity: AtomicUsize::new(capacity),
            }),
        })
    }

    pub async fn append(&self) -> Result<()> {
        todo!()
    }

    pub async fn write(&self) -> Result<()> {
        todo!()
    }

    pub async fn read(&self) -> Result<()> {
        todo!()
    }

    pub async fn flush(&self) -> Result<()> {
        todo!()
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

    pub fn meta_blocks(&self) -> usize {
        self.meta_blocks
    }
}

impl CacheFile {
    #[inline(always)]
    fn fd(&self) -> RawFd {
        self.core.file.as_raw_fd()
    }
}

#[inline(always)]
fn filename(id: u64) -> String {
    format!("cf-{:020}", id)
}

fn write_header(file: &File, block_size: usize, meta_blocks: usize) -> Result<()> {
    let mut buf: DioBuffer = Vec::with_capacity_in(LOGICAL_BLOCK_SIZE, &DIO_BUFFER_ALLOCATOR);

    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_be_bytes());
    buf.extend_from_slice(&block_size.to_be_bytes());
    buf.extend_from_slice(&meta_blocks.to_be_bytes());
    buf.resize(LOGICAL_BLOCK_SIZE, 0);

    file.write_all_at(&buf, 0)?;
    Ok(())
}

fn read_header(file: &File) -> Result<(usize, usize)> {
    let mut buf: DioBuffer = Vec::with_capacity_in(LOGICAL_BLOCK_SIZE, &DIO_BUFFER_ALLOCATOR);
    buf.resize(LOGICAL_BLOCK_SIZE, 0);
    file.read_exact_at(&mut buf, 0)?;
    let mut cursor = 0;

    cursor += MAGIC.len();
    let magic = &buf[cursor - MAGIC.len()..cursor];
    if magic != MAGIC {
        return Err(Error::Other(format!(
            "magic mismatch, expected: {:?}, got: {:?}",
            MAGIC, magic
        )));
    }

    cursor += 4;
    let version = (&buf[cursor - 4..cursor]).get_u32();
    if version != VERSION {
        return Err(Error::Other(format!("unsupported version: {}", version)));
    }

    cursor += 8;
    let block_size = (&buf[cursor - 8..cursor]).get_u64() as usize;

    cursor += 8;
    let meta_blocks = (&buf[cursor - 8..cursor]).get_u64() as usize;

    Ok((block_size, meta_blocks))
}

#[inline(always)]
fn assert_pow2(v: usize) {
    assert_eq!(v & (v - 1), 0);
}

#[inline(always)]
fn assert_alignment(align: usize, v: usize) {
    assert_eq!(v & (align - 1), 0, "align: {}, v: {}", align, v);
}

#[inline(always)]
fn _align_up(align: usize, v: usize) -> usize {
    (v + align - 1) & !(align - 1)
}

#[inline(always)]
fn _align_down(align: usize, v: usize) -> usize {
    v & !(align - 1)
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
        let options = CacheFileOptions {
            dir: tempdir.path().to_str().unwrap().to_string(),
            id: 1,

            fs_block_size: 4096,
            block_size: 4096,
            meta_blocks: 64,
            fallocate_unit: 64 * 1024 * 1024,
        };
        let cf = CacheFile::open(options.clone()).await.unwrap();
        assert_eq!(cf.block_size, 4096);
        assert_eq!(cf.meta_blocks, 64);
        assert_eq!(cf.len(), 4096 * 65);
        assert_eq!(cf.size(), 64 * 1024 * 1024);
        drop(cf);

        let cf = CacheFile::open(options).await.unwrap();
        assert_eq!(cf.block_size, 4096);
        assert_eq!(cf.meta_blocks, 64);
        assert_eq!(cf.len(), 4096 * 65);
        assert_eq!(cf.size(), 64 * 1024 * 1024 + 4096 * 65);
    }
}
