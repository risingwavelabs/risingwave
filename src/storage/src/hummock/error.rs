// Copyright 2025 RisingWave Labs
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

use risingwave_common::catalog::TableId;
use risingwave_object_store::object::ObjectError;
use thiserror::Error;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::error::RecvError;

// TODO(error-handling): should prefer use error types than strings.
#[derive(Error, thiserror_ext::ReportDebug, thiserror_ext::Arc)]
#[thiserror_ext(newtype(name = HummockError, backtrace))]
pub enum HummockErrorInner {
    #[error("Magic number mismatch: expected {expected}, found: {found}")]
    MagicMismatch { expected: u32, found: u32 },
    #[error("Invalid format version: {0}")]
    InvalidFormatVersion(u32),
    #[error("Checksum mismatch: expected {expected}, found: {found}")]
    ChecksumMismatch { expected: u64, found: u64 },
    #[error("Invalid block")]
    InvalidBlock,
    #[error("Encode error: {0}")]
    EncodeError(String),
    #[error("Decode error: {0}")]
    DecodeError(String),
    #[error("ObjectStore failed with IO error: {0}")]
    ObjectIoError(
        #[from]
        #[backtrace]
        ObjectError,
    ),
    #[error("Meta error: {0}")]
    MetaError(String),
    #[error("SharedBuffer error: {0}")]
    SharedBufferError(String),
    #[error("Wait epoch error: {0}")]
    WaitEpoch(String),
    #[error("Next epoch error: {0}")]
    NextEpoch(String),
    #[error("Barrier read is unavailable for now. Likely the cluster is recovering")]
    ReadCurrentEpoch,
    #[error("Expired Epoch: watermark {safe_epoch}, epoch {epoch}")]
    ExpiredEpoch {
        table_id: u32,
        safe_epoch: u64,
        epoch: u64,
    },
    #[error("CompactionExecutor error: {0}")]
    CompactionExecutor(String),
    #[error("FileCache error: {0}")]
    FileCache(String),
    #[error("SstObjectIdTracker error: {0}")]
    SstObjectIdTrackerError(String),
    #[error("CompactionGroup error: {0}")]
    CompactionGroupError(String),
    #[error("SstableUpload error: {0}")]
    SstableUploadError(String),
    #[error("Read backup error: {0}")]
    ReadBackupError(String),
    #[error("Foyer error: {0}")]
    FoyerError(anyhow::Error),
    #[error("Other error: {0}")]
    Other(String),
}

impl HummockError {
    pub fn invalid_format_version(v: u32) -> HummockError {
        HummockErrorInner::InvalidFormatVersion(v).into()
    }

    pub fn invalid_block() -> HummockError {
        HummockErrorInner::InvalidBlock.into()
    }

    pub fn encode_error(error: impl ToString) -> HummockError {
        HummockErrorInner::EncodeError(error.to_string()).into()
    }

    pub fn decode_error(error: impl ToString) -> HummockError {
        HummockErrorInner::DecodeError(error.to_string()).into()
    }

    pub fn magic_mismatch(expected: u32, found: u32) -> HummockError {
        HummockErrorInner::MagicMismatch { expected, found }.into()
    }

    pub fn checksum_mismatch(expected: u64, found: u64) -> HummockError {
        HummockErrorInner::ChecksumMismatch { expected, found }.into()
    }

    pub fn meta_error(error: impl ToString) -> HummockError {
        HummockErrorInner::MetaError(error.to_string()).into()
    }

    pub fn shared_buffer_error(error: impl ToString) -> HummockError {
        HummockErrorInner::SharedBufferError(error.to_string()).into()
    }

    pub fn wait_epoch(error: impl ToString) -> HummockError {
        HummockErrorInner::WaitEpoch(error.to_string()).into()
    }

    pub fn next_epoch(error: impl ToString) -> HummockError {
        HummockErrorInner::NextEpoch(error.to_string()).into()
    }

    pub fn read_current_epoch() -> HummockError {
        HummockErrorInner::ReadCurrentEpoch.into()
    }

    pub fn expired_epoch(table_id: TableId, safe_epoch: u64, epoch: u64) -> HummockError {
        HummockErrorInner::ExpiredEpoch {
            table_id: table_id.table_id,
            safe_epoch,
            epoch,
        }
        .into()
    }

    pub fn is_expired_epoch(&self) -> bool {
        matches!(self.inner(), HummockErrorInner::ExpiredEpoch { .. })
    }

    pub fn is_meta_error(&self) -> bool {
        matches!(self.inner(), HummockErrorInner::MetaError(..))
    }

    pub fn is_object_error(&self) -> bool {
        matches!(self.inner(), HummockErrorInner::ObjectIoError { .. })
    }

    pub fn compaction_executor(error: impl ToString) -> HummockError {
        HummockErrorInner::CompactionExecutor(error.to_string()).into()
    }

    pub fn sst_object_id_tracker_error(error: impl ToString) -> HummockError {
        HummockErrorInner::SstObjectIdTrackerError(error.to_string()).into()
    }

    pub fn compaction_group_error(error: impl ToString) -> HummockError {
        HummockErrorInner::CompactionGroupError(error.to_string()).into()
    }

    pub fn file_cache(error: impl ToString) -> HummockError {
        HummockErrorInner::FileCache(error.to_string()).into()
    }

    pub fn sstable_upload_error(error: impl ToString) -> HummockError {
        HummockErrorInner::SstableUploadError(error.to_string()).into()
    }

    pub fn read_backup_error(error: impl ToString) -> HummockError {
        HummockErrorInner::ReadBackupError(error.to_string()).into()
    }

    pub fn foyer_error(error: anyhow::Error) -> HummockError {
        HummockErrorInner::FoyerError(error).into()
    }

    pub fn other(error: impl ToString) -> HummockError {
        HummockErrorInner::Other(error.to_string()).into()
    }
}

impl From<prost::DecodeError> for HummockError {
    fn from(error: prost::DecodeError) -> Self {
        HummockErrorInner::DecodeError(error.to_report_string()).into()
    }
}

impl From<RecvError> for HummockError {
    fn from(error: RecvError) -> Self {
        ObjectError::from(error).into()
    }
}

pub type HummockResult<T> = std::result::Result<T, HummockError>;
