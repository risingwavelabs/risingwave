// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Segment bundle and segment meta encoding, and the read-only directory serving a bundle.
//!
//! Bundle layout (all integers little-endian):
//!
//! ```text
//! | file 0 | file 1 | ... | file table | footer |
//!
//! file table: num_files: u32, then per file: name_len: u16, name, offset: u64, len: u64
//! footer:     table_offset: u64, version: u32, magic: u32, checksum: u64
//! ```
//!
//! File names are Tantivy's file names (`<segment uuid>.<extension>`), so a bundle can only be
//! opened with its own segment meta. Each file is stored exactly as Tantivy wrote it, including
//! Tantivy's own footer. The checksum is the xxhash64 of every byte before it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    FileHandle, FileSlice, OwnedBytes, RamDirectory, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::index::{SegmentComponent, SegmentId, SegmentMeta};
use tantivy::{Directory, IndexMeta};
use xxhash_rust::xxh64;

use super::{FULL_TEXT_FORMAT_VERSION, full_text_error};
use crate::hummock::{HummockError, HummockResult};

const BUNDLE_MAGIC: u32 = 0x5257_4642; // "RWFB"
const BUNDLE_FOOTER_LEN: usize = 8 + 4 + 4 + 8;

/// Opstamp used to name deletion vector files. Deletion vectors are stored outside the bundle,
/// so the name only has to be consistent within one opened segment.
pub(super) const DELETE_OPSTAMP: u64 = 0;

pub(super) fn encode_bundle(files: &[(String, OwnedBytes)]) -> Bytes {
    let data_len: usize = files.iter().map(|(_, data)| data.len()).sum();
    let table_len: usize = 4 + files
        .iter()
        .map(|(name, _)| 2 + name.len() + 16)
        .sum::<usize>();
    let mut buf = BytesMut::with_capacity(data_len + table_len + BUNDLE_FOOTER_LEN);
    let mut entries = Vec::with_capacity(files.len());
    for (name, data) in files {
        entries.push((name, buf.len() as u64, data.len() as u64));
        buf.put_slice(data.as_slice());
    }
    let table_offset = buf.len() as u64;
    buf.put_u32_le(entries.len() as u32);
    for (name, offset, len) in entries {
        buf.put_u16_le(name.len() as u16);
        buf.put_slice(name.as_bytes());
        buf.put_u64_le(offset);
        buf.put_u64_le(len);
    }
    buf.put_u64_le(table_offset);
    buf.put_u32_le(FULL_TEXT_FORMAT_VERSION);
    buf.put_u32_le(BUNDLE_MAGIC);
    let checksum = xxh64::xxh64(&buf, 0);
    buf.put_u64_le(checksum);
    buf.freeze()
}

/// Verifies a bundle and returns its files, keyed by name.
pub(super) fn decode_bundle(bundle: Bytes) -> HummockResult<HashMap<String, FileSlice>> {
    let len = bundle.len();
    if len < BUNDLE_FOOTER_LEN {
        return Err(HummockError::decode_error(format!(
            "full-text segment bundle too short: {len} bytes"
        )));
    }
    let mut footer = &bundle[len - BUNDLE_FOOTER_LEN..];
    let table_offset = footer.get_u64_le();
    let version = footer.get_u32_le();
    let magic = footer.get_u32_le();
    let expected_checksum = footer.get_u64_le();
    if magic != BUNDLE_MAGIC {
        return Err(HummockError::magic_mismatch(BUNDLE_MAGIC, magic));
    }
    if version != FULL_TEXT_FORMAT_VERSION {
        return Err(HummockError::invalid_format_version(version));
    }
    let checksum = xxh64::xxh64(&bundle[..len - 8], 0);
    if checksum != expected_checksum {
        return Err(HummockError::checksum_mismatch(expected_checksum, checksum));
    }

    let table_end = len - BUNDLE_FOOTER_LEN;
    let corrupted = || HummockError::decode_error("corrupted full-text segment bundle file table");
    let table_offset = usize::try_from(table_offset).map_err(|_| corrupted())?;
    if table_offset > table_end {
        return Err(corrupted());
    }
    let mut table = &bundle[table_offset..table_end];
    if table.remaining() < 4 {
        return Err(corrupted());
    }
    let num_files = table.get_u32_le() as usize;
    let mut entries = Vec::with_capacity(num_files);
    for _ in 0..num_files {
        if table.remaining() < 2 {
            return Err(corrupted());
        }
        let name_len = table.get_u16_le() as usize;
        if table.remaining() < name_len + 16 {
            return Err(corrupted());
        }
        let name = String::from_utf8(table[..name_len].to_vec()).map_err(|_| corrupted())?;
        table.advance(name_len);
        let offset = table.get_u64_le() as usize;
        let file_len = table.get_u64_le() as usize;
        if offset
            .checked_add(file_len)
            .is_none_or(|end| end > table_offset)
        {
            return Err(corrupted());
        }
        entries.push((name, offset, file_len));
    }

    // Converting to `Vec` is zero-copy when the `Bytes` is uniquely owned, and copies otherwise.
    let data = OwnedBytes::new(Vec::<u8>::from(bundle));
    let mut files = HashMap::with_capacity(entries.len());
    for (name, offset, file_len) in entries {
        let slice = FileSlice::new(Arc::new(data.slice(offset..offset + file_len)));
        if files.insert(name, slice).is_some() {
            return Err(corrupted());
        }
    }
    Ok(files)
}

/// Collects the files of a freshly written segment from the directory it was written to.
pub(super) fn collect_segment_files(
    directory: &RamDirectory,
    index_meta: &IndexMeta,
    segment_meta: &SegmentMeta,
) -> HummockResult<Vec<(String, OwnedBytes)>> {
    let prefix = format!("{}.", segment_meta.id().uuid_string());
    let delete_path = segment_meta.relative_path(SegmentComponent::Delete);
    let temp_store_path = segment_meta.relative_path(SegmentComponent::TempStore);
    let mut files = Vec::new();
    for path in index_meta.list_segment_files() {
        if !directory.exists(&path).map_err(full_text_error)? {
            continue;
        }
        if path == delete_path || path == temp_store_path {
            return Err(HummockError::other(format!(
                "unexpected file {} in a new full-text segment",
                path.display()
            )));
        }
        let name = path
            .to_str()
            .filter(|path| path.starts_with(&prefix))
            .ok_or_else(|| {
                HummockError::other(format!(
                    "unexpected file name {} in a full-text segment",
                    path.display()
                ))
            })?
            .to_owned();
        // Read the raw file, including Tantivy's footer, so that it can be served back as is.
        let data = directory
            .open_read(&path)
            .map_err(full_text_error)?
            .read_bytes()
            .map_err(full_text_error)?;
        files.push((name, data));
    }
    files.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(files)
}

/// What is needed to open a segment besides its bundle.
pub(super) struct DecodedSegmentMeta {
    pub segment_id: SegmentId,
    pub max_doc: u32,
}

/// Layout: `version: u32, max_doc: u32, segment uuid as a 32-char hex string`.
pub(super) fn encode_segment_meta(segment_id: SegmentId, max_doc: u32) -> Bytes {
    let uuid = segment_id.uuid_string();
    let mut buf = BytesMut::with_capacity(8 + uuid.len());
    buf.put_u32_le(FULL_TEXT_FORMAT_VERSION);
    buf.put_u32_le(max_doc);
    buf.put_slice(uuid.as_bytes());
    buf.freeze()
}

pub(super) fn decode_segment_meta(mut data: &[u8]) -> HummockResult<DecodedSegmentMeta> {
    if data.len() < 8 {
        return Err(HummockError::decode_error(
            "full-text segment meta too short",
        ));
    }
    let version = data.get_u32_le();
    if version != FULL_TEXT_FORMAT_VERSION {
        return Err(HummockError::invalid_format_version(version));
    }
    let max_doc = data.get_u32_le();
    let segment_id = std::str::from_utf8(data)
        .ok()
        .and_then(|uuid| SegmentId::from_uuid_string(uuid).ok())
        .ok_or_else(|| {
            HummockError::decode_error("invalid segment id in full-text segment meta")
        })?;
    Ok(DecodedSegmentMeta {
        segment_id,
        max_doc,
    })
}

/// A Tantivy directory serving the read-only files of one bundle, without copying them.
///
/// Tantivy still writes a few small files when an index is created on top of it (`meta.json`,
/// `.managed.json`) and the deletion vector is placed next to the bundle files. Those go to an
/// in-memory overlay.
#[derive(Clone)]
pub(super) struct BundleDirectory {
    files: Arc<HashMap<PathBuf, FileSlice>>,
    overlay: RamDirectory,
}

impl BundleDirectory {
    pub fn new(files: HashMap<String, FileSlice>) -> Self {
        let files = files
            .into_iter()
            .map(|(name, slice)| (PathBuf::from(name), slice))
            .collect();
        Self {
            files: Arc::new(files),
            overlay: RamDirectory::create(),
        }
    }

    pub fn overlay(&self) -> &RamDirectory {
        &self.overlay
    }

    fn read_only_error(path: &Path) -> io::Error {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} is a read-only bundle file", path.display()),
        )
    }
}

impl fmt::Debug for BundleDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BundleDirectory")
            .field("files", &self.files.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        match self.files.get(path) {
            Some(slice) => Ok(Arc::new(slice.clone())),
            None => self.overlay.get_file_handle(path),
        }
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        match self.files.get(path) {
            Some(slice) => Ok(slice.clone()),
            None => self.overlay.open_read(path),
        }
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        if self.files.contains_key(path) {
            return Err(DeleteError::IoError {
                io_error: Arc::new(Self::read_only_error(path)),
                filepath: path.to_path_buf(),
            });
        }
        self.overlay.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.files.contains_key(path) || self.overlay.exists(path)?)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        if self.files.contains_key(path) {
            return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
        }
        self.overlay.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        match self.files.get(path) {
            Some(slice) => slice
                .read_bytes()
                .map(|bytes| bytes.as_slice().to_vec())
                .map_err(|io_error| OpenReadError::IoError {
                    io_error: Arc::new(io_error),
                    filepath: path.to_path_buf(),
                }),
            None => self.overlay.atomic_read(path),
        }
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        if self.files.contains_key(path) {
            return Err(Self::read_only_error(path));
        }
        self.overlay.atomic_write(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.overlay.watch(watch_callback)
    }
}
