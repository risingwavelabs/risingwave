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

use std::path::PathBuf;
use std::sync::Arc;

use nix::sys::statfs::{statfs, FsType as NixFsType, EXT4_SUPER_MAGIC};
use risingwave_common::cache::{LruCache, LruCacheEventListener};

use super::coding::CacheKey;
use super::error::{Error, Result};
use super::file::{CacheFile, CacheFileOptions};
use super::meta::{BlockLoc, MetaFile, SlotId};

const META_FILE_FILENAME: &str = "meta";
const CACHE_FILE_FILENAME: &str = "cache";

#[derive(Clone, Copy, Debug)]
pub enum FsType {
    Ext4,
    Xfs,
}

pub struct StoreOptions {
    pub dir: String,
    pub slots: usize,
    pub capacity: usize,
    pub cache_file_fallocate_unit: usize,
}

pub struct Store<K>
where
    K: CacheKey,
{
    _dir: String,
    _capacity: usize,

    _fs_type: FsType,
    _fs_block_size: usize,

    _mf: MetaFile<K>,
    _cf: CacheFile,
}

impl<K> Store<K>
where
    K: CacheKey,
{
    pub async fn open(options: StoreOptions) -> Result<Self> {
        if !PathBuf::from(options.dir.as_str()).exists() {
            std::fs::create_dir_all(options.dir.as_str())?;
        }

        // Get file system type and block size by `statfs(2)`.
        let fs_stat = statfs(options.dir.as_str())?;
        let fs_type = match fs_stat.filesystem_type() {
            EXT4_SUPER_MAGIC => FsType::Ext4,
            // FYI: https://github.com/nix-rust/nix/issues/1742
            NixFsType(libc::XFS_SUPER_MAGIC) => FsType::Xfs,
            nix_fs_type => return Err(Error::UnsupportedFilesystem(nix_fs_type.0)),
        };
        let fs_block_size = fs_stat.block_size() as usize;

        let mf_capacity = options.slots * (K::encoded_len() + BlockLoc::encoded_len());
        let cf_opts = CacheFileOptions {
            fs_block_size,
            // TODO: Make it configurable.
            block_size: fs_block_size,
            fallocate_unit: options.cache_file_fallocate_unit,
        };

        let mf = MetaFile::open(
            PathBuf::from(&options.dir).join(META_FILE_FILENAME),
            mf_capacity,
        )?;

        let cf = CacheFile::open(
            PathBuf::from(&options.dir).join(CACHE_FILE_FILENAME),
            cf_opts,
        )
        .await?;

        Ok(Self {
            _dir: options.dir,
            _capacity: options.capacity,

            _fs_type: fs_type,
            _fs_block_size: fs_block_size,

            _mf: mf,
            _cf: cf,
        })
    }

    pub async fn restore(&self, _indices: &LruCache<K, SlotId>) -> Result<()> {
        // TODO: Impl me!!!
        Ok(())
    }

    pub fn insert(&self) -> Result<()> {
        todo!()
    }

    pub fn erase(&self) -> Result<()> {
        todo!()
    }
}

impl<K> LruCacheEventListener for Store<K>
where
    K: CacheKey,
{
    type K = K;
    type T = SlotId;

    fn on_evict(&self, _key: &Self::K, _value: &Self::T) {
        // TODO: Trigger invalidate cache slot.
    }

    fn on_erase(&self, _key: &Self::K, _value: &Self::T) {
        // TODO: Trigger invalidate cache slot.
    }
}

pub type StoreRef<K> = Arc<Store<K>>;

#[cfg(test)]
mod tests {

    use super::super::test_utils::TestCacheKey;
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<StoreRef<TestCacheKey>>();
    }
}
