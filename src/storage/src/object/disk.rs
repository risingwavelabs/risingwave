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

use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::UnboundedSender;

use crate::object::{
    BlockLocation, BoxedObjectUploader, ObjectError, ObjectMetadata, ObjectResult, ObjectStore,
    ObjectUploader, SstableDataStream,
};

pub(super) mod utils {
    use std::fs::Metadata;
    use std::path::Path;

    use tokio::fs::{create_dir_all, File, OpenOptions};

    use crate::object::{ObjectError, ObjectResult};

    pub async fn ensure_file_dir_exists(path: &Path) -> ObjectResult<()> {
        if let Some(dir) = path.parent() {
            // `create_dir_all` will not error even if the directory already exists.
            create_dir_all(dir).await.map_err(|e| {
                ObjectError::internal(format!(
                    "unable to create dir: {:?}. Err: {:?}",
                    dir.to_str(),
                    e
                ))
            })?;
        }
        Ok(())
    }

    pub async fn open_file(
        path: &Path,
        enable_read: bool,
        enable_write: bool,
        create_new: bool,
    ) -> ObjectResult<File> {
        ensure_file_dir_exists(path).await?;
        OpenOptions::new()
            .read(enable_read)
            .write(enable_write)
            .create_new(create_new)
            .open(path)
            .await
            .map_err(|err| {
                ObjectError::internal(format!(
                    "Failed to open file {:?}. Err: {:?}",
                    path.to_str(),
                    err
                ))
            })
    }

    pub async fn get_metadata(file: &File) -> ObjectResult<Metadata> {
        file.metadata()
            .await
            .map_err(|err| ObjectError::internal(format!("Failed to get metadata. Err: {:?}", err)))
    }
}

pub struct FileSstableDataStream {
    file: File,
    buffer: Bytes,
}

#[async_trait::async_trait]
impl SstableDataStream for FileSstableDataStream {
    async fn reset(&mut self) -> ObjectResult<()> {
        self.file
            .seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|err| {
                ObjectError::internal(format!("Failed to seek to start. Err: {:?}", err))
            })?;
        Ok(())
    }

    async fn read(&mut self, size: usize) -> ObjectResult<&[u8]> {
        let mut buffer = vec![0; size];
        self.file
            .read_exact(&mut buffer)
            .await
            .map_err(|err| ObjectError::internal(format!("Failed to read. Err: {:?}", err)))?;
        self.buffer = buffer.into();
        Ok(&self.buffer)
    }
}

struct LocalDiskUploader {
    file: File,
    path: PathBuf,
}

#[async_trait::async_trait]
impl ObjectUploader for LocalDiskUploader {
    async fn upload(&mut self, data: &[u8]) -> ObjectResult<()> {
        self.file.write_all(data).await.map_err(|err| {
            ObjectError::internal(format!(
                "Failed to write data to {:?}. Err: {:?}",
                self.path, err
            ))
        })
    }

    async fn finish(mut self: Box<LocalDiskUploader>) -> ObjectResult<()> {
        self.file.flush().await.map_err(|e| {
            ObjectError::internal(format!("failed to flush to {:?}, err: {:?}", self.path, e))
        })
    }

    async fn finish_with_data(mut self: Box<Self>) -> ObjectResult<Box<dyn SstableDataStream>> {
        self.file.flush().await.map_err(|e| {
            ObjectError::internal(format!("failed to flush to {:?}, err: {:?}", self.path, e))
        })?;
        Ok(Box::new(FileSstableDataStream {
            file: utils::open_file(&self.path, true, false, false).await?,
            buffer: vec![].into(),
        }))
    }
}

pub struct LocalDiskObjectStore {
    path_prefix: String,
    compactor_shutdown_sender: parking_lot::Mutex<Option<UnboundedSender<()>>>,
}

impl LocalDiskObjectStore {
    pub fn new(path_prefix: &str) -> LocalDiskObjectStore {
        LocalDiskObjectStore {
            path_prefix: path_prefix.to_string(),
            compactor_shutdown_sender: parking_lot::Mutex::new(None),
        }
    }

    pub fn new_file_path(&self, path: &str) -> ObjectResult<PathBuf> {
        if path.starts_with('/') {
            return Err(ObjectError::internal("path should not start with /"));
        };
        let mut ret = PathBuf::from(&self.path_prefix);
        ret.push(path);
        Ok(ret)
    }

    pub fn set_compactor_shutdown_sender(&self, shutdown_sender: UnboundedSender<()>) {
        *self.compactor_shutdown_sender.lock() = Some(shutdown_sender);
    }
}

impl Drop for LocalDiskObjectStore {
    fn drop(&mut self) {
        if let Some(sender) = self.compactor_shutdown_sender.lock().take() {
            let _ = sender.send(());
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for LocalDiskObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        let mut file =
            utils::open_file(self.new_file_path(path)?.as_path(), false, true, true).await?;
        file.write_all(&obj).await.map_err(|e| {
            ObjectError::internal(format!("failed to write {}, err: {:?}", path, e))
        })?;
        file.flush().await.map_err(|e| {
            ObjectError::internal(format!("failed to flush {}, err: {:?}", path, e))
        })?;
        Ok(())
    }

    async fn get_upload_handle(&self, path: &str) -> ObjectResult<BoxedObjectUploader> {
        let path = self.new_file_path(path)?;
        let file = utils::open_file(path.as_path(), false, true, true)
            .await
            .map_err(|e| {
                ObjectError::internal(format!("failed to open {:?}, err: {:?}", path, e))
            })?;
        Ok(Box::new(LocalDiskUploader { file, path }))
    }

    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        let mut file =
            utils::open_file(self.new_file_path(path)?.as_path(), true, false, false).await?;
        match block_loc {
            Some(block_loc) => Ok(self.readv(path, vec![block_loc]).await?.pop().unwrap()),
            None => {
                let metadata = utils::get_metadata(&file).await?;
                let mut buf = Vec::with_capacity(metadata.len() as usize);
                file.read_to_end(&mut buf).await.map_err(|e| {
                    ObjectError::internal(format!(
                        "failed to read the whole file {}, err: {:?}",
                        path, e
                    ))
                })?;
                Ok(Bytes::from(buf))
            }
        }
    }

    async fn readv(&self, path: &str, block_locs: Vec<BlockLocation>) -> ObjectResult<Vec<Bytes>> {
        let mut file =
            utils::open_file(self.new_file_path(path)?.as_path(), true, false, false).await?;
        let metadata = utils::get_metadata(&file).await?;
        for block_loc in &block_locs {
            if block_loc.offset + block_loc.size > metadata.len() as usize {
                return Err(ObjectError::internal(format!(
                    "block location {:?} is out of bounds for file of len {}",
                    block_loc,
                    metadata.len()
                )));
            }
        }
        let mut ret = Vec::with_capacity(block_locs.len());
        // TODO: may want to sort and reorder the location to improve serial read.
        for block_loc in &block_locs {
            let mut buf = vec![0; block_loc.size as usize];
            file.seek(std::io::SeekFrom::Start(block_loc.offset as u64))
                .await
                .map_err(|err| {
                    ObjectError::internal(format!(
                        "Failed to seek to offset {}. Err: {:?}",
                        block_loc.offset, err
                    ))
                })?;
            let read_size = file.read_exact(&mut buf).await.map_err(|err| {
                ObjectError::internal(format!(
                    "Failed to read from offset {}. Err: {:?}",
                    block_loc.offset, err
                ))
            })?;

            if read_size != buf.len() {
                return Err(ObjectError::internal(format!(
                    "Failed to read from offset {}. Read {} bytes, expected {}",
                    block_loc.offset,
                    read_size,
                    buf.len()
                )));
            }
            ret.push(Bytes::from(buf));
        }
        Ok(ret)
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        let file =
            utils::open_file(self.new_file_path(path)?.as_path(), true, false, false).await?;
        let metadata = utils::get_metadata(&file).await?;
        Ok(ObjectMetadata {
            total_size: metadata.len() as usize,
        })
    }

    async fn delete(&self, path: &str) -> ObjectResult<()> {
        tokio::fs::remove_file(self.new_file_path(path)?.as_path())
            .await
            .map_err(|e| {
                ObjectError::internal(format!("failed to delete {}, err: {:?}", path, e))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Read;
    use std::path::PathBuf;

    use bytes::Bytes;
    use itertools::Itertools;
    use tempfile::TempDir;

    use crate::object::disk::LocalDiskObjectStore;
    use crate::object::{BlockLocation, ObjectStore};

    fn gen_test_payload() -> Vec<u8> {
        let mut ret = Vec::new();
        for i in 0..100000 {
            ret.extend(format!("{:05}", i).as_bytes());
        }
        ret
    }

    fn check_payload(payload: &[u8], path: &str) {
        let mut file = OpenOptions::new().read(true).open(path).unwrap();
        assert_eq!(payload.len(), file.metadata().unwrap().len() as usize);
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert_eq!(payload, &buf[..]);
    }

    #[tokio::test]
    async fn test_simple_upload() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);

        let mut path = PathBuf::from(test_root_path);
        path.push("test.obj");
        check_payload(&payload, path.to_str().unwrap());
    }

    #[tokio::test]
    async fn test_multi_level_dir_upload() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("1/2/test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("1/2/test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);

        let mut path = PathBuf::from(test_root_path);
        path.push("1/2/test.obj");
        check_payload(&payload, path.to_str().unwrap());
    }

    #[tokio::test]
    async fn test_read_all() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);
        let read_data = store.read("test.obj", None).await.unwrap();
        assert_eq!(payload, &read_data[..]);
    }

    #[tokio::test]
    async fn test_read_partial() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);
        let read_data = store
            .read(
                "test.obj",
                Some(BlockLocation {
                    offset: 10000,
                    size: 1000,
                }),
            )
            .await
            .unwrap();
        assert_eq!(&payload[10000..11000], &read_data[..]);
    }

    #[tokio::test]
    async fn test_read_multi_block() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let metadata = store.metadata("test.obj").await.unwrap();
        assert_eq!(payload.len(), metadata.total_size);
        let test_loc = vec![(0, 1000), (10000, 1000), (20000, 1000)];
        let read_data = store
            .readv(
                "test.obj",
                test_loc
                    .iter()
                    .map(|(offset, size)| BlockLocation {
                        offset: *offset,
                        size: *size,
                    })
                    .collect_vec(),
            )
            .await
            .unwrap();
        assert_eq!(test_loc.len(), read_data.len());
        for (i, (offset, size)) in test_loc.iter().enumerate() {
            assert_eq!(&payload[*offset..(*offset + *size)], &read_data[i][..]);
        }
    }

    #[tokio::test]
    async fn test_delete() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        let mut path = PathBuf::from(test_root_path);
        path.push("test.obj");
        assert!(path.exists());
        store.delete("test.obj").await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn test_read_not_exists() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);

        assert!(store.read("non-exist.obj", None).await.is_err());
    }

    #[tokio::test]
    async fn test_read_out_of_range() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        store
            .upload("test.obj", Bytes::from(payload.clone()))
            .await
            .unwrap();
        assert_eq!(payload.len(), 500000);
        assert!(store
            .read(
                "test.obj",
                Some(BlockLocation {
                    offset: 499999,
                    size: 1,
                })
            )
            .await
            .is_ok());
        assert!(store
            .read(
                "test.obj",
                Some(BlockLocation {
                    offset: 499999,
                    size: 2,
                })
            )
            .await
            .is_err());
        assert!(store
            .readv(
                "test.obj",
                vec![
                    BlockLocation {
                        offset: 499999,
                        size: 2,
                    },
                    BlockLocation {
                        offset: 10000,
                        size: 2,
                    }
                ]
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_invalid_path() {
        let test_dir = TempDir::new().unwrap();
        let test_root_path = test_dir.path().to_str().unwrap();
        let store = LocalDiskObjectStore::new(test_root_path);
        let payload = gen_test_payload();
        // path is not allowed to be started with '/'
        assert!(store
            .upload("/test.obj", Bytes::from(payload))
            .await
            .is_err());
    }
}
