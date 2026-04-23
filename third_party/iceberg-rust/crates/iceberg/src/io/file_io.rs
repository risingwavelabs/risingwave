// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use opendal::Operator;
use url::Url;

use super::storage::{OpenDalStorage, Storage};
use crate::{Error, ErrorKind, Result};

/// Configuration property for setting the chunk size for IO write operations.
///
/// This is useful for FileIO operations which may use multipart uploads (e.g. for S3)
/// where consistent chunk sizes of a certain size may be more optimal. Some services
/// like Cloudlare R2 requires all chunk sizes to be consistent except for the last.
pub const IO_CHUNK_SIZE: &str = "io.write.chunk-size";

/// Configuration property for setting the timeout duration for IO operations.
///
/// This timeout is applied to individual operations like read, write, delete, etc.
/// Value should be in seconds. If not set, uses OpenDAL's default timeout.
pub const IO_TIMEOUT_SECONDS: &str = "io.timeout";

/// Configuration property for setting the maximum number of retries for IO operations.
///
/// This controls how many times an operation will be retried upon failure.
/// If not set, uses OpenDAL's default retry count.
pub const IO_MAX_RETRIES: &str = "io.max-retries";

/// Configuration property for setting the minimum retry delay in milliseconds.
///
/// This controls the minimum delay between retry attempts.
/// If not set, uses OpenDAL's default minimum delay.
pub const IO_RETRY_MIN_DELAY_MS: &str = "io.retry.min-delay-ms";

/// Configuration property for setting the maximum retry delay in milliseconds.
///
/// This controls the maximum delay between retry attempts.
/// If not set, uses OpenDAL's default maximum delay.
pub const IO_RETRY_MAX_DELAY_MS: &str = "io.retry.max-delay-ms";

/// FileIO implementation, used to manipulate files in underlying storage.
///
/// # Note
///
/// All path passed to `FileIO` must be absolute path starting with scheme string used to construct `FileIO`.
/// For example, if you construct `FileIO` with `s3a` scheme, then all path passed to `FileIO` must start with `s3a://`.
///
/// Supported storages:
///
/// | Storage            | Feature Flag      | Expected Path Format             | Schemes                       |
/// |--------------------|-------------------|----------------------------------| ------------------------------|
/// | Local file system  | `storage-fs`      | `file`                           | `file://path/to/file`         |
/// | Memory             | `storage-memory`  | `memory`                         | `memory://path/to/file`       |
/// | S3                 | `storage-s3`      | `s3`, `s3a`                      | `s3://<bucket>/path/to/file`  |
/// | GCS                | `storage-gcs`     | `gs`, `gcs`                      | `gs://<bucket>/path/to/file`  |
/// | OSS                | `storage-oss`     | `oss`                            | `oss://<bucket>/path/to/file` |
/// | Azure Datalake     | `storage-azdls`   | `abfs`, `abfss`, `wasb`, `wasbs` | `abfs://<filesystem>@<account>.dfs.core.windows.net/path/to/file` or `wasb://<container>@<account>.blob.core.windows.net/path/to/file` |
/// | AZBLOB             | `storage-azblob`  | `azblob`                         | `azblob://<container>/path/to/file` |
#[derive(Clone, Debug)]
pub struct FileIO {
    builder: FileIOBuilder,

    inner: Arc<OpenDalStorage>,
}

impl FileIO {
    /// Convert FileIO into [`FileIOBuilder`] which used to build this FileIO.
    ///
    /// This function is useful when you want serialize and deserialize FileIO across
    /// distributed systems.
    pub fn into_builder(self) -> FileIOBuilder {
        self.builder
    }

    /// Try to infer file io scheme from path. See [`FileIO`] for supported schemes.
    ///
    /// - If it's a valid url, for example `s3://bucket/a`, url scheme will be used, and the rest of the url will be ignored.
    /// - If it's not a valid url, will try to detect if it's a file path.
    ///
    /// Otherwise will return parsing error.
    pub fn from_path(path: impl AsRef<str>) -> crate::Result<FileIOBuilder> {
        let url = Url::parse(path.as_ref())
            .map_err(Error::from)
            .or_else(|e| {
                Url::from_file_path(path.as_ref()).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Input is neither a valid url nor path",
                    )
                    .with_context("input", path.as_ref().to_string())
                    .with_source(e)
                })
            })?;

        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        let (op, relative_path) = self
            .inner
            .create_operator_with_config(&path, &self.builder.props)?;
        Ok(op.delete(relative_path).await?)
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Behavior
    ///
    /// - If the path is a file or not exist, this function will be no-op.
    /// - If the path is a empty directory, this function will remove the directory itself.
    /// - If the path is a non-empty directory, this function will remove the directory and all nested files and directories.
    pub async fn delete_prefix(&self, path: impl AsRef<str>) -> Result<()> {
        let (op, relative_path) = self
            .inner
            .create_operator_with_config(&path, &self.builder.props)?;
        let path = if relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };
        Ok(op.remove_all(&path).await?)
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        let (op, relative_path) = self
            .inner
            .create_operator_with_config(&path, &self.builder.props)?;
        Ok(op.exists(relative_path).await?)
    }

    /// Lists files and directories under the given path as a stream.
    ///
    /// # Arguments
    ///
    /// * `path`: Absolute path starting with scheme string used to construct [`FileIO`].
    /// * `recursive`: If `true`, list all files recursively; otherwise list only direct children.
    pub async fn list(
        &self,
        path: impl AsRef<str>,
        recursive: bool,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        let path_str: Arc<str> = Arc::from(path.as_ref());
        let (op, relative_path) = self
            .inner
            .create_operator_with_config(&path_str, &self.builder.props)?;

        // Calculate the absolute prefix: path_str without the relative_path suffix.
        let absolute_prefix: Arc<str> =
            Arc::from(&path_str[..path_str.len() - relative_path.len()]);

        // Ensure path ends with '/' for directory listing
        let list_path = if relative_path.is_empty() || relative_path.ends_with('/') {
            relative_path.to_string()
        } else {
            format!("{relative_path}/")
        };

        // OpenDAL's lister_with returns a Lister that implements Stream
        let lister = op.lister_with(&list_path).recursive(recursive).await?;

        // Transform the OpenDAL Entry stream into our ListEntry stream
        let stream = lister
            .map(move |entry_result| {
                entry_result
                    .map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "Failed to list entry")
                            .with_context("path", path_str.to_string())
                            .with_source(e)
                    })
                    .map(|entry| {
                        let meta = entry.metadata();
                        ListEntry {
                            path: format!("{}{}", absolute_prefix, entry.path()),
                            metadata: FileMetadata {
                                size: meta.content_length(),
                                last_modified_ms: meta.last_modified().and_then(|dt| {
                                    let system_time: std::time::SystemTime = dt.into();
                                    system_time
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .ok()
                                        .map(|d| d.as_millis() as i64)
                                }),
                                is_dir: meta.is_dir(),
                            },
                        }
                    })
            })
            .boxed();

        Ok(stream)
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        self.inner.new_input(path.as_ref())
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        let (op, relative_path) = self
            .inner
            .create_operator_with_config(&path, &self.builder.props)?;
        let path = path.as_ref().to_string();
        let relative_path_pos = path.len() - relative_path.len();

        // ADLS requires append mode for writes
        #[cfg(feature = "storage-azdls")]
        let append_file = matches!(self.inner.as_ref(), OpenDalStorage::Azdls { .. });
        #[cfg(not(feature = "storage-azdls"))]
        let append_file = false;

        Ok(OutputFile::new_with_op(
            self.inner.clone(),
            path,
            op,
            relative_path_pos,
            self.get_write_chunk_size()?,
            append_file,
        ))
    }

    fn get_write_chunk_size(&self) -> Result<Option<usize>> {
        match self.builder.props.get(IO_CHUNK_SIZE) {
            Some(chunk_size) => {
                let parsed_chunk_size = chunk_size.parse::<usize>().map_err(|_err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid {IO_CHUNK_SIZE}: Cannot parse to unsigned integer."),
                    )
                })?;
                Ok(Some(parsed_chunk_size))
            }
            _ => Ok(None),
        }
    }
}

/// Container for storing type-safe extensions used to configure underlying FileIO behavior.
#[derive(Clone, Debug, Default)]
pub struct Extensions(HashMap<TypeId, Arc<dyn Any + Send + Sync>>);

impl Extensions {
    /// Add an extension.
    pub fn add<T: Any + Send + Sync>(&mut self, ext: T) {
        self.0.insert(TypeId::of::<T>(), Arc::new(ext));
    }

    /// Extends the current set of extensions with another set of extensions.
    pub fn extend(&mut self, extensions: Extensions) {
        self.0.extend(extensions.0);
    }

    /// Fetch an extension.
    pub fn get<T>(&self) -> Option<Arc<T>>
    where T: 'static + Send + Sync + Clone {
        let type_id = TypeId::of::<T>();
        self.0
            .get(&type_id)
            .and_then(|arc_any| Arc::clone(arc_any).downcast::<T>().ok())
    }
}

/// Builder for [`FileIO`].
#[derive(Clone, Debug)]
pub struct FileIOBuilder {
    /// This is used to infer scheme of operator.
    ///
    /// If this is `None`, then [`FileIOBuilder::build`](FileIOBuilder::build) will build a local file io.
    scheme_str: Option<String>,
    /// Arguments for operator.
    props: HashMap<String, String>,
    /// Optional extensions to configure the underlying FileIO behavior.
    extensions: Extensions,
}

impl FileIOBuilder {
    /// Creates a new builder with scheme.
    /// See [`FileIO`] for supported schemes.
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
            extensions: Extensions::default(),
        }
    }

    /// Creates a new builder for local file io.
    pub fn new_fs_io() -> Self {
        Self {
            scheme_str: None,
            props: HashMap::default(),
            extensions: Extensions::default(),
        }
    }

    /// Fetch the scheme string.
    ///
    /// The scheme_str will be empty if it's None.
    pub fn into_parts(self) -> (String, HashMap<String, String>, Extensions) {
        (
            self.scheme_str.unwrap_or_default(),
            self.props,
            self.extensions,
        )
    }

    /// Add argument for operator.
    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    /// Add argument for operator.
    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    /// Add an extension to the file IO builder.
    pub fn with_extension<T: Any + Send + Sync>(mut self, ext: T) -> Self {
        self.extensions.add(ext);
        self
    }

    /// Adds multiple extensions to the file IO builder.
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions.extend(extensions);
        self
    }

    /// Fetch an extension from the file IO builder.
    pub fn extension<T>(&self) -> Option<Arc<T>>
    where T: 'static + Send + Sync + Clone {
        self.extensions.get::<T>()
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> Result<FileIO> {
        let storage = OpenDalStorage::build(self.clone())?;
        Ok(FileIO {
            builder: self,
            inner: Arc::new(storage),
        })
    }
}

/// The struct that represents the metadata of a file.
pub struct FileMetadata {
    /// The size of the file.
    pub size: u64,
    /// The last modified time in milliseconds since Unix epoch.
    pub last_modified_ms: Option<i64>,
    /// Whether this entry represents a directory.
    pub is_dir: bool,
}

/// A single entry returned by listing operations.
pub struct ListEntry {
    /// Absolute path that can be directly passed back to [`FileIO`] operations.
    pub path: String,
    /// Metadata associated with the path.
    pub metadata: FileMetadata,
}

/// Trait for reading file.
///
/// # TODO
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileRead: Send + Sync + Unpin + 'static {
    /// Read file content with given range.
    ///
    /// TODO: we can support reading non-contiguous bytes in the future.
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes>;
}

/// Input file is used for reading from files.
#[derive(Debug)]
pub struct InputFile {
    storage: Arc<dyn Storage>,
    // Absolute path of file.
    path: String,
}

impl InputFile {
    /// Creates a new input file.
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self { storage, path }
    }

    /// Absolute path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    pub async fn exists(&self) -> crate::Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Fetch and returns metadata of file.
    pub async fn metadata(&self) -> crate::Result<FileMetadata> {
        self.storage.metadata(&self.path).await
    }

    /// Read and returns whole content of file.
    ///
    /// For continuous reading, use [`Self::reader`] instead.
    pub async fn read(&self) -> crate::Result<Bytes> {
        self.storage.read(&self.path).await
    }

    /// Creates [`FileRead`] for continuous reading.
    ///
    /// For one-time reading, use [`Self::read`] instead.
    pub async fn reader(&self) -> crate::Result<Box<dyn FileRead>> {
        self.storage.reader(&self.path).await
    }
}

/// Trait for writing file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    /// Write bytes to file.
    ///
    /// TODO: we can support writing non-contiguous bytes in the future.
    async fn write(&mut self, bs: Bytes) -> crate::Result<()>;

    /// Close file.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> crate::Result<()>;
}

/// Output file is used for writing to files..
#[derive(Debug)]
pub struct OutputFile {
    storage: Arc<dyn Storage>,
    // Absolute path of file.
    path: String,
    // Relative path of file to uri, starts at [`relative_path_pos`]
    relative_path_pos: Option<usize>,
    // Optional direct operator for configuring writer behavior.
    op: Option<Operator>,
    // Chunk size for write operations to ensure consistent size of multipart chunks
    chunk_size: Option<usize>,
    // Whether to use append mode for writes (required for some storage backends like AZDLS)
    append_file: bool,
}

impl OutputFile {
    /// Creates a new output file.
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self {
        Self {
            storage,
            path,
            relative_path_pos: None,
            op: None,
            chunk_size: None,
            append_file: false,
        }
    }

    pub(crate) fn new_with_op(
        storage: Arc<dyn Storage>,
        path: String,
        op: Operator,
        relative_path_pos: usize,
        chunk_size: Option<usize>,
        append_file: bool,
    ) -> Self {
        Self {
            storage,
            path,
            relative_path_pos: Some(relative_path_pos),
            op: Some(op),
            chunk_size,
            append_file,
        }
    }

    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Checks if file exists.
    pub async fn exists(&self) -> Result<bool> {
        self.storage.exists(&self.path).await
    }

    /// Deletes file.
    ///
    /// If the file does not exist, it will not return error.
    pub async fn delete(&self) -> Result<()> {
        self.storage.delete(&self.path).await
    }

    /// Converts into [`InputFile`].
    pub fn to_input_file(self) -> InputFile {
        InputFile {
            storage: self.storage,
            path: self.path,
        }
    }

    /// Create a new output file with given bytes.
    ///
    /// # Notes
    ///
    /// Calling `write` will overwrite the file if it exists.
    /// For continuous writing, use [`Self::writer`].
    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        let mut writer = self.writer().await?;
        writer.write(bs).await?;
        writer.close().await
    }

    /// Creates output file for continuous writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        if let (Some(op), Some(relative_path_pos)) = (&self.op, self.relative_path_pos) {
            let mut writer = op
                .writer_with(&self.path[relative_path_pos..])
                .append(self.append_file);
            if let Some(chunk_size) = self.chunk_size {
                writer = writer.chunk(chunk_size);
            }
            return Ok(Box::new(writer.await?));
        }

        self.storage.writer(&self.path).await
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::Path;

    use bytes::Bytes;
    use futures::io::AllowStdIo;
    use futures::{AsyncReadExt, TryStreamExt};
    use tempfile::TempDir;

    use super::{FileIO, FileIOBuilder, ListEntry};
    use crate::io::IO_CHUNK_SIZE;

    fn create_local_file_io() -> FileIO {
        FileIOBuilder::new_fs_io().build().unwrap()
    }

    fn write_to_file<P: AsRef<Path>>(s: &str, path: P) {
        create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        let mut f = File::create(path).unwrap();
        write!(f, "{s}").unwrap();
    }

    async fn read_from_file<P: AsRef<Path>>(path: P) -> String {
        let mut f = AllowStdIo::new(File::open(path).unwrap());
        let mut s = String::new();
        f.read_to_string(&mut s).await.unwrap();
        s
    }

    #[tokio::test]
    async fn test_local_input_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);
        write_to_file(content, &full_path);

        let file_io = create_local_file_io();
        let input_file = file_io.new_input(&full_path).unwrap();

        assert!(input_file.exists().await.unwrap());
        // Remove heading slash
        assert_eq!(&full_path, input_file.location());
        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[tokio::test]
    async fn test_delete_local_file() {
        let tmp_dir = TempDir::new().unwrap();

        let a_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), "a.txt");
        let sub_dir_path = format!("{}/sub", tmp_dir.path().to_str().unwrap());
        let b_path = format!("{}/{}", sub_dir_path, "b.txt");
        let c_path = format!("{}/{}", sub_dir_path, "c.txt");
        write_to_file("Iceberg loves rust.", &a_path);
        write_to_file("Iceberg loves rust.", &b_path);
        write_to_file("Iceberg loves rust.", &c_path);

        let file_io = create_local_file_io();
        assert!(file_io.exists(&a_path).await.unwrap());

        // Remove a file should be no-op.
        file_io.delete_prefix(&a_path).await.unwrap();
        assert!(file_io.exists(&a_path).await.unwrap());

        // Remove a not exist dir should be no-op.
        file_io.delete_prefix("not_exists/").await.unwrap();

        // Remove a dir should remove all files in it.
        file_io.delete_prefix(&sub_dir_path).await.unwrap();
        assert!(!file_io.exists(&b_path).await.unwrap());
        assert!(!file_io.exists(&c_path).await.unwrap());
        assert!(file_io.exists(&a_path).await.unwrap());

        file_io.delete(&a_path).await.unwrap();
        assert!(!file_io.exists(&a_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_non_exist_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        assert!(!file_io.exists(&full_path).await.unwrap());
        assert!(file_io.delete(&full_path).await.is_ok());
        assert!(file_io.delete_prefix(&full_path).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_output_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        let output_file = file_io.new_output(&full_path).unwrap();

        assert!(!output_file.exists().await.unwrap());
        {
            output_file.write(content.into()).await.unwrap();
        }

        assert_eq!(&full_path, output_file.location());

        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[test]
    fn test_create_file_from_path() {
        let io = FileIO::from_path("/tmp/a").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("file:/tmp/b").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("file:///tmp/c").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("s3://bucket/a").unwrap();
        assert_eq!("s3", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("tmp/||c");
        assert!(io.is_err());
    }

    #[tokio::test]
    async fn test_memory_io() {
        let io = FileIOBuilder::new("memory").build().unwrap();

        let path = format!("{}/1.txt", TempDir::new().unwrap().path().to_str().unwrap());

        let output_file = io.new_output(&path).unwrap();
        output_file.write("test".into()).await.unwrap();

        assert!(io.exists(&path.clone()).await.unwrap());
        let input_file = io.new_input(&path).unwrap();
        let content = input_file.read().await.unwrap();
        assert_eq!(content, Bytes::from("test"));

        io.delete(&path).await.unwrap();
        assert!(!io.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_set_chunk_size() {
        let io = FileIOBuilder::new("memory")
            .with_prop(IO_CHUNK_SIZE, 32 * 1024 * 1024)
            .build()
            .unwrap();

        let path = format!("{}/1.txt", TempDir::new().unwrap().path().to_str().unwrap());
        let output_file = io.new_output(&path).unwrap();
        assert_eq!(Some(32 * 1024 * 1024), output_file.chunk_size);
    }

    #[tokio::test]
    async fn test_list() {
        let tmp_dir = TempDir::new().unwrap();
        let root = tmp_dir.path().join("root");
        let sub_dir = root.join("subdir");
        create_dir_all(&sub_dir).unwrap();

        // Create files at different levels
        let root_file = root.join("root_file.txt");
        let nested_file = sub_dir.join("nested_file.txt");
        write_to_file("root", &root_file);
        write_to_file("nested", &nested_file);

        let file_io = create_local_file_io();
        let root_path = root.to_str().unwrap();

        // Non-recursive: should only see direct children
        let entries: Vec<ListEntry> = file_io
            .list(root_path, false)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();

        assert!(paths.iter().any(|p| p.ends_with("root_file.txt")));
        assert!(!paths.iter().any(|p| p.ends_with("nested_file.txt")));

        // Recursive: should see all files with correct absolute paths
        let entries: Vec<ListEntry> = file_io
            .list(root_path, true)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();

        // Verify absolute path format (can be passed back to FileIO)
        let expected_nested = format!("{root_path}/subdir/nested_file.txt");
        assert!(paths.contains(&expected_nested.as_str()));
        assert!(paths.iter().any(|p| p.ends_with("root_file.txt")));
    }

    #[tokio::test]
    async fn test_list_with_file_scheme() {
        let tmp_dir = TempDir::new().unwrap();
        let data_dir = tmp_dir.path().join("data");
        create_dir_all(&data_dir).unwrap();
        write_to_file("test", data_dir.join("file.parquet"));

        let file_io = create_local_file_io();
        let path = format!("file://{}", tmp_dir.path().to_str().unwrap());

        let entries: Vec<ListEntry> = file_io
            .list(&path, true)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();

        // Returned paths preserve the file:/ scheme prefix
        assert!(paths.iter().all(|p| p.starts_with("file:/")));
        assert!(paths.iter().any(|p| p.ends_with("file.parquet")));
    }
}
