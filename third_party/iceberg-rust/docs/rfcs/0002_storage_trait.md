<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Making Storage a Trait

## Background

### Existing Implementation

The existing code implements storage functionality through a concrete `Storage` enum that handles different storage backends (S3, local filesystem, GCS, etc.). This implementation is tightly coupled with OpenDAL as the underlying storage layer. The `FileIO` struct wraps this `Storage` enum and provides a high-level API for file operations.

```rust
// Current: Concrete enum with variants for each backend
pub(crate) enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory(Operator),
    #[cfg(feature = "storage-fs")]
    LocalFs,
    #[cfg(feature = "storage-s3")]
    S3 {
        configured_scheme: String,
        config: Arc<S3Config>,
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs { config: Arc<GcsConfig> },
    // ... other variants
}
```

Current structure:

- **FileIO:** Main interface for file operations, wraps `Arc<Storage>`
- **Storage:** Enum with variants for different storage backends
- **InputFile / OutputFile:** Concrete structs that hold an `Operator` and path

### Problem Statement

The original design has several limitations:

- **Tight Coupling** – All storage logic depends on OpenDAL, limiting flexibility. Users cannot easily opt in for other storage implementations like `object_store`
- **Customization Barriers** – Users cannot easily add custom behaviors or optimizations
- **No Extensibility** – Adding new backends requires modifying the core enum in the `iceberg` crate

As discussed in Issue #1314, making Storage a trait would allow pluggable storage and better integration with existing systems.

---

## High-Level Architecture

The new design introduces a trait-based storage abstraction with a factory pattern for creating storage instances. This enables pluggable storage backends while maintaining a clean separation between the core Iceberg library and storage implementations.

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User Application                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 Catalog                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  CatalogBuilder::with_storage_factory(storage_factory)              │    │
│  │  - Accepts optional StorageFactory injection                        │    │
│  │  - Falls back to LocalFsStorageFactory if not provided              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FileIO / FileIOBuilder                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  FileIOBuilder:                                                     │    │
│  │  - factory: Arc<dyn StorageFactory>                                 │    │
│  │  - config: StorageConfig                                            │    │
│  │  - Methods: new(), with_prop(), with_props(), config(), build()     │    │
│  │                                                                     │    │
│  │  FileIO:                                                            │    │
│  │  - config: StorageConfig (properties only, no scheme)               │    │
│  │  - factory: Arc<dyn StorageFactory>                                 │    │
│  │  - storage: OnceCell<Arc<dyn Storage>> (lazy initialization)        │    │
│  │  - Methods: new_with_memory(), new_with_fs(), into_builder()        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Methods: new_input(), new_output(), delete(), exists(), delete_prefix()    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
┌───────────────────────────────┐     ┌───────────────────────────────────────┐
│        StorageFactory         │     │              Storage                   │
│  (trait in iceberg crate)     │     │        (trait in iceberg crate)        │
│                               │     │                                        │
│  fn build(&self, config)      │────▶│  async fn exists(&self, path)          │
│     -> Arc<dyn Storage>       │     │  async fn read(&self, path)            │
│                               │     │  async fn write(&self, path, bytes)    │
│                               │     │  async fn delete(&self, path)          │
│                               │     │  fn new_input(&self, path)             │
│                               │     │  fn new_output(&self, path)            │
└───────────────────────────────┘     └───────────────────────────────────────┘
            │                                           ▲
            │                                           │
            ▼                                           │
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Storage Implementations                               │
│                                                                              │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐  │
│  │   MemoryStorage     │  │   LocalFsStorage    │  │   OpenDalStorage    │  │
│  │   (iceberg crate)   │  │   (iceberg crate)   │  │ (iceberg-storage-   │  │
│  │                     │  │                     │  │      opendal)       │  │
│  │  - In-memory HashMap│  │  - std::fs ops      │  │  - S3, GCS, Azure   │  │
│  │  - For testing      │  │  - For local files  │  │  - OSS, filesystem  │  │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           FileIO Creation Flow                                │
└──────────────────────────────────────────────────────────────────────────────┘

  User Code                 FileIOBuilder                  StorageFactory
      │                          │                              │
      │  FileIOBuilder::new()    │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │  .with_prop()            │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │  .build()                │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │◀─────────────────────────│                              │
      │  FileIO                  │                              │
      │                          │                              │
      │  new_input(path)         │                              │
      │─────────────────────────▶│                              │
      │                          │  (lazy) factory.build()      │
      │                          │─────────────────────────────▶│
      │                          │                              │
      │                          │◀─────────────────────────────│
      │                          │  Arc<dyn Storage>            │
      │                          │                              │
      │◀─────────────────────────│                              │
      │  InputFile               │                              │


┌──────────────────────────────────────────────────────────────────────────────┐
│                        Catalog with FileIO Injection                          │
└──────────────────────────────────────────────────────────────────────────────┘

  User Code                  CatalogBuilder                  Catalog
      │                          │                              │
      │  ::default()             │                              │
      │─────────────────────────▶│                              │
      │                          │                              │
      │  .with_storage_factory(factory)  │                              │
      │─────────────────────────────────▶│                              │
      │                          │                              │
      │  .load(name, props)      │                              │
      │─────────────────────────▶│                              │
      │                          │  new(config, Some(file_io))  │
      │                          │─────────────────────────────▶│
      │                          │                              │
      │◀─────────────────────────│◀─────────────────────────────│
      │  Catalog                 │                              │
```

### Crate Structure

```
crates/
├── iceberg/                         # Core Iceberg functionality
│   └── src/
│       └── io/
│           ├── mod.rs               # Re-exports
│           ├── storage.rs           # Storage + StorageFactory traits
│           ├── file_io.rs           # FileIO, InputFile, OutputFile
│           ├── config/              # StorageConfig and backend configs
│           │   ├── mod.rs           # StorageConfig
│           │   ├── s3.rs            # S3Config constants
│           │   ├── gcs.rs           # GcsConfig constants
│           │   ├── oss.rs           # OssConfig constants
│           │   └── azdls.rs         # AzdlsConfig constants
│           ├── memory.rs            # MemoryStorage (built-in)
│           └── local_fs.rs          # LocalFsStorage (built-in)
│
├── storage/
│   └── opendal/                     # OpenDAL-based implementations
│       └── src/
│           ├── lib.rs               # Re-exports
│           ├── storage.rs           # OpenDalStorage + OpenDalStorageFactory
│           ├── storage_s3.rs        # S3 support
│           ├── storage_gcs.rs       # GCS support
│           ├── storage_oss.rs       # OSS support
│           ├── storage_azdls.rs     # Azure support
│           └── storage_fs.rs        # Filesystem support
│
└── catalog/                         # Catalog implementations
    ├── rest/                        # Uses with_storage_factory injection
    ├── glue/                        # Uses with_storage_factory injection
    ├── hms/                         # Uses with_storage_factory injection
    ├── s3tables/                    # Uses with_storage_factory injection
    └── sql/                         # Uses with_storage_factory injection
```

---

## Design Phase 1: Storage Trait and Core Types

Phase 1 focuses on converting Storage from an enum to a trait, introducing `StorageFactory` and `StorageConfig`, and updating `FileIO`, `InputFile`, and `OutputFile` to use the trait-based abstraction.

### Storage Trait

The `Storage` trait is defined in the `iceberg` crate and provides the interface for all storage operations. It uses `typetag` for serialization support across process boundaries.

```rust
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: Debug + Send + Sync {
    /// Check if a file exists at the given path
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Get metadata from an input path
    async fn metadata(&self, path: &str) -> Result<FileMetadata>;

    /// Read bytes from a path
    async fn read(&self, path: &str) -> Result<Bytes>;

    /// Get FileRead from a path
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>>;

    /// Write bytes to an output path
    async fn write(&self, path: &str, bs: Bytes) -> Result<()>;

    /// Get FileWrite from a path
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    /// Delete a file at the given path
    async fn delete(&self, path: &str) -> Result<()>;

    /// Delete all files with the given prefix
    async fn delete_prefix(&self, path: &str) -> Result<()>;

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}
```

### StorageFactory Trait

The `StorageFactory` trait creates `Storage` instances from configuration. This enables lazy initialization and custom storage injection.

```rust
#[typetag::serde(tag = "type")]
pub trait StorageFactory: Debug + Send + Sync {
    /// Build a new Storage instance from the given configuration.
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>>;
}
```

### StorageConfig

`StorageConfig` is a pure property container without scheme. The storage type is determined by explicit factory selection:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    /// Configuration properties for the storage backend
    props: HashMap<String, String>,
}

impl StorageConfig {
    pub fn new() -> Self;
    pub fn from_props(props: HashMap<String, String>) -> Self;
    pub fn props(&self) -> &HashMap<String, String>;
    pub fn get(&self, key: &str) -> Option<&String>;
    pub fn with_prop(self, key: impl Into<String>, value: impl Into<String>) -> Self;
    pub fn with_props(self, props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self;
}
```

#### Backend-Specific Configuration Types

In addition to `StorageConfig`, we provide typed configuration structs for each storage backend.
These can be constructed from `StorageConfig` using `TryFrom` and provide a structured way to access backend-specific settings:

- `S3Config` - Amazon S3 configuration
- `GcsConfig` - Google Cloud Storage configuration
- `OssConfig` - Alibaba Cloud OSS configuration
- `AzdlsConfig` - Azure Data Lake Storage configuration

Example of `S3Config`:

```rust
#[derive(Clone, Debug, Default, Serialize, Deserialize, TypedBuilder)]
pub struct S3Config {
    #[builder(default, setter(strip_option, into))]
    endpoint: Option<String>,
    #[builder(default, setter(strip_option, into))]
    access_key_id: Option<String>,
    #[builder(default, setter(strip_option, into))]
    secret_access_key: Option<String>,
    #[builder(default, setter(strip_option, into))]
    region: Option<String>,
    #[builder(default)]
    allow_anonymous: bool,
    // ... other S3-specific fields
}

impl S3Config {
    /// Returns the S3 endpoint URL.
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    // ... other getter methods
}

// Fallible conversion from StorageConfig
impl TryFrom<&StorageConfig> for S3Config {
    type Error = iceberg::Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {/* ... */}
}
```

Usage with the builder pattern:

```rust
let s3_config = S3Config::builder()
    .region("us-east-1")
    .access_key_id("my-access-key")
    .secret_access_key("my-secret-key")
    .build();

assert_eq!(s3_config.region(), Some("us-east-1"));
```

These typed configs are used internally by storage implementations (e.g., `OpenDalStorage`) to
parse properties from `StorageConfig` into strongly-typed configuration.

### FileIO and FileIOBuilder

`FileIO` is redesigned to use lazy storage initialization with a factory pattern. Configuration is done via `FileIOBuilder`:

```rust
#[derive(Clone)]
pub struct FileIO {
    /// Storage configuration containing properties
    config: StorageConfig,
    /// Factory for creating storage instances
    factory: Arc<dyn StorageFactory>,
    /// Cached storage instance (lazily initialized)
    storage: Arc<OnceCell<Arc<dyn Storage>>>,
}

impl FileIO {
    /// Create a new FileIO backed by in-memory storage.
    pub fn new_with_memory() -> Self;

    /// Create a new FileIO backed by local filesystem storage.
    pub fn new_with_fs() -> Self;

    /// Convert this FileIO into a FileIOBuilder for modification.
    pub fn into_builder(self) -> FileIOBuilder;

    // File operations delegate to the lazily-initialized storage
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()>;
    pub async fn delete_prefix(&self, path: impl AsRef<str>) -> Result<()>;
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool>;
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile>;
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile>;
}

/// Builder for creating FileIO instances.
pub struct FileIOBuilder {
    factory: Arc<dyn StorageFactory>,
    config: StorageConfig,
}

impl FileIOBuilder {
    /// Create a new FileIOBuilder with the given storage factory.
    pub fn new(factory: Arc<dyn StorageFactory>) -> Self;

    /// Add a configuration property.
    pub fn with_prop(self, key: impl Into<String>, value: impl Into<String>) -> Self;

    /// Add multiple configuration properties.
    pub fn with_props(self, props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self;

    /// Get the storage configuration.
    pub fn config(&self) -> &StorageConfig;

    /// Build the FileIO instance.
    pub fn build(self) -> Result<FileIO>;
}
```

Key changes from the old design:
- `FileIOBuilder` is used for configuration with explicit factory injection
- `FileIO` has convenience constructors (`new_with_memory()`, `new_with_fs()`) for common cases
- Removed `Extensions` - custom behavior is now provided via `StorageFactory`
- Storage is lazily initialized on first use via `OnceCell`

### InputFile and OutputFile Changes

`InputFile` and `OutputFile` now hold a reference to `Arc<dyn Storage>` instead of an `Operator`:

```rust
pub struct InputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl InputFile {
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self;
    pub fn location(&self) -> &str;
    pub async fn exists(&self) -> Result<bool>;
    pub async fn metadata(&self) -> Result<FileMetadata>;
    pub async fn read(&self) -> Result<Bytes>;
    pub async fn reader(&self) -> Result<Box<dyn FileRead>>;
}

pub struct OutputFile {
    storage: Arc<dyn Storage>,
    path: String,
}

impl OutputFile {
    pub fn new(storage: Arc<dyn Storage>, path: String) -> Self;
    pub fn location(&self) -> &str;
    pub async fn exists(&self) -> Result<bool>;
    pub async fn delete(&self) -> Result<()>;
    pub fn to_input_file(self) -> InputFile;
    pub async fn write(&self, bs: Bytes) -> Result<()>;
    pub async fn writer(&self) -> Result<Box<dyn FileWrite>>;
}
```

### Built-in Storage Implementations

The `iceberg` crate includes two built-in storage implementations for testing and basic use cases:

#### MemoryStorage

In-memory storage using a thread-safe `HashMap`, primarily for testing:

```rust
#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    data: Arc<RwLock<HashMap<String, Bytes>>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryStorageFactory;

#[typetag::serde]
impl StorageFactory for MemoryStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        if config.scheme() != "memory" {
            return Err(/* error */);
        }
        Ok(Arc::new(MemoryStorage::new()))
    }
}
```

#### LocalFsStorage

Local filesystem storage using standard Rust `std::fs` operations:

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalFsStorage;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LocalFsStorageFactory;

#[typetag::serde]
impl StorageFactory for LocalFsStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        if config.scheme() != "file" {
            return Err(/* error */);
        }
        Ok(Arc::new(LocalFsStorage::new()))
    }
}
```

### CatalogBuilder Changes

The `CatalogBuilder` trait is extended with `with_storage_factory()` to allow StorageFactory injection:

```rust
pub trait CatalogBuilder: Default + Debug + Send + Sync {
    type C: Catalog;

    /// Set a custom StorageFactory to use for storage operations.
    ///
    /// When a StorageFactory is provided, the catalog will use it to build FileIO
    /// instances for all storage operations instead of using the default factory.
    fn with_storage_factory(self, storage_factory: Arc<dyn StorageFactory>) -> Self;

    /// Create a new catalog instance.
    fn load(
        self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send;
}
```

Catalog implementations store the optional `StorageFactory` and use it when provided:

```rust
pub struct GlueCatalogBuilder {
    config: GlueCatalogConfig,
    storage_factory: Option<Arc<dyn StorageFactory>>,  // New field
}

impl CatalogBuilder for GlueCatalogBuilder {
    fn with_storage_factory(mut self, storage_factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(storage_factory);
        self
    }

    // In load():
    // Use provided StorageFactory or LocalFsStorageFactory as fallback
    let factory = storage_factory.unwrap_or_else(|| Arc::new(LocalFsStorageFactory));
    let file_io = FileIOBuilder::new(factory)
        .with_props(file_io_props)
        .build()?;
}
```

---

## Design Part 2: Separate Storage Crate

Phase 2 moves concrete OpenDAL-based implementations to a separate crate (`iceberg-storage-opendal`).

### iceberg-storage-opendal Crate

This crate provides OpenDAL-based storage implementations for cloud storage backends:

```rust
// crates/storage/opendal/src/storage.rs

/// Explicit storage factory variants for OpenDAL-based backends.
///
/// Each variant represents a specific storage backend. Path scheme
/// validation is handled by the underlying Storage implementation
/// when operations are performed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpenDalStorageFactory {
    /// Local filesystem storage factory.
    #[cfg(feature = "storage-fs")]
    Fs,

    /// Amazon S3 storage factory.
    #[cfg(feature = "storage-s3")]
    S3,

    /// Google Cloud Storage factory.
    #[cfg(feature = "storage-gcs")]
    Gcs,

    /// Alibaba Cloud OSS storage factory.
    #[cfg(feature = "storage-oss")]
    Oss,

    /// Azure Data Lake Storage factory.
    #[cfg(feature = "storage-azdls")]
    Azdls,
}

#[typetag::serde]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let storage = match self {
            #[cfg(feature = "storage-fs")]
            Self::Fs => OpenDalStorage::LocalFs,

            #[cfg(feature = "storage-s3")]
            Self::S3 => {
                let iceberg_s3_config = S3Config::try_from(config)?;
                let opendal_s3_config = s3_config_to_opendal(&iceberg_s3_config);
                OpenDalStorage::S3 {
                    configured_scheme: "s3".to_string(),
                    config: opendal_s3_config.into(),
                    customized_credential_load: None,
                }
            }
            // ... other variants
        };
        Ok(Arc::new(storage))
    }
}

/// Unified OpenDAL-based storage implementation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpenDalStorage {
    #[cfg(feature = "storage-fs")]
    LocalFs,

    #[cfg(feature = "storage-s3")]
    S3 {
        configured_scheme: String,
        config: Arc<S3Config>,
        #[serde(skip)]
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },

    #[cfg(feature = "storage-gcs")]
    Gcs { config: Arc<GcsConfig> },

    #[cfg(feature = "storage-oss")]
    Oss { config: Arc<OssConfig> },

    #[cfg(feature = "storage-azdls")]
    Azdls {
        configured_scheme: AzureStorageScheme,
        config: Arc<AzdlsConfig>,
    },
}

impl OpenDalStorage {
    /// Creates operator from path.
    fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)>;
}

#[async_trait]
#[typetag::serde]
impl Storage for OpenDalStorage {
    // Delegates all operations to the appropriate OpenDAL operator
}
```

Feature flags in `iceberg-storage-opendal`:
- `storage-s3` : Enables S3 storage backend
- `storage-gcs`: Enables Google Cloud Storage backend
- `storage-oss`: Enables Alibaba Cloud OSS backend
- `storage-azdls`: Enables Azure Data Lake Storage backend
- `storage-fs`: Enables OpenDAL filesystem backend
- `storage-all`: Enables all storage backends

---

## Example Usage

### Basic Usage with Memory Storage (Testing)

```rust
use iceberg::io::FileIO;

// Create in-memory FileIO for testing
let file_io = FileIO::new_with_memory();

// Write and read files
let output = file_io.new_output("memory://test/file.txt")?;
output.write("Hello, World!".into()).await?;

let input = file_io.new_input("memory://test/file.txt")?;
let content = input.read().await?;
assert_eq!(content, bytes::Bytes::from("Hello, World!"));
```

### Using OpenDAL Storage Factory

```rust
use std::sync::Arc;
use iceberg::io::FileIOBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

// Create FileIO with explicit S3 factory
let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3))
    .with_prop("s3.region", "us-east-1")
    .with_prop("s3.access-key-id", "my-access-key")
    .with_prop("s3.secret-access-key", "my-secret-key")
    .build()?;

// Use the FileIO
let input = file_io.new_input("s3://my-bucket/warehouse/table/metadata.json")?;
let metadata = input.read().await?;
```

### Using Catalogs with Custom Storage

When using a catalog without injecting a custom `StorageFactory`, the catalog falls back to
`LocalFsStorageFactory`. For cloud storage, inject the appropriate factory:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use iceberg::CatalogBuilder;
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

// Inject S3 storage factory for cloud storage support
let catalog = GlueCatalogBuilder::default()
    .with_storage_factory(Arc::new(OpenDalStorageFactory::S3))
    .load("my_catalog", HashMap::from([
        ("warehouse".to_string(), "s3://my-bucket/warehouse".to_string()),
        ("s3.region".to_string(), "us-east-1".to_string()),
    ]))
    .await?;

// Load and scan a table - storage is handled automatically
let table = catalog.load_table(&TableIdent::from_strs(["db", "my_table"])?).await?;
let scan = table.scan().build()?;
```

### Injecting Custom StorageFactory into Catalogs

For advanced use cases, you can inject a custom `StorageFactory` with specific storage configuration:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use iceberg::CatalogBuilder;
use iceberg::io::FileIOBuilder;
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

// Create a custom StorageFactory
let storage_factory = Arc::new(OpenDalStorageFactory::S3);

// Inject StorageFactory into catalog
let catalog = GlueCatalogBuilder::default()
    .with_storage_factory(storage_factory)
    .load("my_catalog", HashMap::from([
        ("warehouse".to_string(), "s3://my-bucket/warehouse".to_string()),
        ("s3.region".to_string(), "us-east-1".to_string()),
    ]))
    .await?;
```

### Implementing Custom Storage

To implement a custom storage backend, implement the `Storage` trait with `#[typetag::serde]`:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use iceberg::io::{Storage, StorageFactory, StorageConfig, InputFile, OutputFile};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MyCustomStorage { /* fields */ }

#[async_trait]
#[typetag::serde]
impl Storage for MyCustomStorage {
    // Implement all required methods: exists, metadata, read, reader,
    // write, writer, delete, delete_prefix, new_input, new_output
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MyCustomStorageFactory;

#[typetag::serde]
impl StorageFactory for MyCustomStorageFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(MyCustomStorage { /* ... */ }))
    }
}
```

### Routing to Multiple Storage Backends

To use different storage implementations for different schemes (e.g., a native S3 client
for S3 and OpenDAL for other schemes), implement routing at the `Storage` level:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use iceberg::io::{Storage, StorageFactory, StorageConfig, InputFile, OutputFile};

/// A storage that routes to different backends based on path scheme
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RoutingStorage {
    s3_storage: NativeS3Storage,      // Custom S3 implementation
    opendal_storage: OpenDalStorage,  // OpenDAL for other schemes
}

#[async_trait]
#[typetag::serde]
impl Storage for RoutingStorage {
    async fn read(&self, path: &str) -> iceberg::Result<bytes::Bytes> {
        if path.starts_with("s3://") || path.starts_with("s3a://") {
            self.s3_storage.read(path).await
        } else {
            self.opendal_storage.read(path).await
        }
    }

    // Route other methods similarly...
}

/// Factory that creates RoutingStorage
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RoutingStorageFactory;

#[typetag::serde]
impl StorageFactory for RoutingStorageFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(RoutingStorage {
            s3_storage: NativeS3Storage::new(config)?,
            opendal_storage: OpenDalStorage::build_from_config(config)?,
        }))
    }
}
```

---


## Implementation Plan

### Phase 1: Storage Trait
- Define `Storage` trait in `iceberg` crate
- Define `StorageFactory` trait in `iceberg` crate
- Introduce `StorageConfig` for configuration properties
- Update `FileIO` to use lazy storage initialization with factory pattern
- Update `InputFile`/`OutputFile` to use `Arc<dyn Storage>`
- Implement `MemoryStorage` and `LocalFsStorage` in `iceberg` crate
- Add `with_storage_factory()` to `CatalogBuilder` trait
- Update all catalog implementations to support StorageFactory injection
- Improve naming: Storage handles locations rather than paths

### Phase 2: Separate Storage Crate
- Create `iceberg-storage-opendal` crate with `OpenDalStorage` and `OpenDalStorageFactory`
- Move S3, GCS, OSS, Azure implementations to `iceberg-storage-opendal`
- Remove storage feature flags from `iceberg` crate

### Future Work
- Implement ResolvingStorage backed by OpenDal
- Add `object_store`-based storage implementations
- Consider introducing `IoErrorKind` for storage-specific error handling
- Introduce custom key values in StorageConfigs
