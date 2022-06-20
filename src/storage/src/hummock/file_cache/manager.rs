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

use super::error::{Error, Result};
use super::filter::Filter;

#[derive(Clone, Copy, Debug)]
pub enum FsType {
    Ext4,
    Xfs,
}

pub struct FileCacheManagerOptions {
    pub dir: String,
    pub filters: Vec<Arc<dyn Filter>>,
}

#[derive(Clone)]
pub struct FileCacheManager {
    _dir: String,

    _fs_type: FsType,
    _fs_block_size: usize,

    _filters: Vec<Arc<dyn Filter>>,
}

impl FileCacheManager {
    pub async fn open(options: FileCacheManagerOptions) -> Result<Self> {
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

        Ok(Self {
            _dir: options.dir,

            _fs_type: fs_type,
            _fs_block_size: fs_block_size,

            _filters: options.filters,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<FileCacheManager>();
    }

    #[tokio::test]
    async fn test_file_cache_manager() {
        let ci: bool = std::env::var("RISINGWAVE_CI")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .expect("env $RISINGWAVE_CI must be 'true' or 'false'");

        let tempdir = if ci {
            tempfile::Builder::new().tempdir_in("/risingwave").unwrap()
        } else {
            tempfile::tempdir().unwrap()
        };

        let options = FileCacheManagerOptions {
            dir: tempdir.path().to_str().unwrap().to_string(),
            filters: vec![],
        };
        let _manager = FileCacheManager::open(options).await.unwrap();
    }
}
