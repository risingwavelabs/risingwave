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

use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;

/// The subsections `[storage.object_store]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreConfig {
    // alias is for backward compatibility
    #[serde(
        default = "default::object_store_config::set_atomic_write_dir",
        alias = "object_store_set_atomic_write_dir"
    )]
    pub set_atomic_write_dir: bool,

    /// Retry and timeout configuration
    /// Description retry strategy driven by exponential back-off
    /// Exposes the timeout and retries of each Object store interface. Therefore, the total timeout for each interface is determined based on the interface's timeout/retry configuration and the exponential back-off policy.
    #[serde(default)]
    pub retry: ObjectStoreRetryConfig,

    /// Some special configuration of S3 Backend
    #[serde(default)]
    pub s3: S3ObjectStoreConfig,

    // TODO: the following field will be deprecated after opendal is stabilized
    #[serde(default = "default::object_store_config::opendal_upload_concurrency")]
    pub opendal_upload_concurrency: usize,

    // TODO: the following field will be deprecated after opendal is stabilized
    #[serde(default)]
    pub opendal_writer_abort_on_err: bool,

    #[serde(default = "default::object_store_config::upload_part_size")]
    pub upload_part_size: usize,
}

impl ObjectStoreConfig {
    pub fn set_atomic_write_dir(&mut self) {
        self.set_atomic_write_dir = true;
    }
}

/// The subsections `[storage.object_store.s3]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct S3ObjectStoreConfig {
    // alias is for backward compatibility
    #[serde(
        default = "default::object_store_config::s3::keepalive_ms",
        alias = "object_store_keepalive_ms"
    )]
    pub keepalive_ms: Option<u64>,
    #[serde(
        default = "default::object_store_config::s3::recv_buffer_size",
        alias = "object_store_recv_buffer_size"
    )]
    pub recv_buffer_size: Option<usize>,
    #[serde(
        default = "default::object_store_config::s3::send_buffer_size",
        alias = "object_store_send_buffer_size"
    )]
    pub send_buffer_size: Option<usize>,
    #[serde(
        default = "default::object_store_config::s3::nodelay",
        alias = "object_store_nodelay"
    )]
    pub nodelay: Option<bool>,
    /// For backwards compatibility, users should use `S3ObjectStoreDeveloperConfig` instead.
    #[serde(default = "default::object_store_config::s3::developer::retry_unknown_service_error")]
    pub retry_unknown_service_error: bool,
    #[serde(default = "default::object_store_config::s3::identity_resolution_timeout_s")]
    pub identity_resolution_timeout_s: u64,
    #[serde(default)]
    pub developer: S3ObjectStoreDeveloperConfig,
}

/// The subsections `[storage.object_store.s3.developer]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct S3ObjectStoreDeveloperConfig {
    /// Whether to retry s3 sdk error from which no error metadata is provided.
    #[serde(
        default = "default::object_store_config::s3::developer::retry_unknown_service_error",
        alias = "object_store_retry_unknown_service_error"
    )]
    pub retry_unknown_service_error: bool,
    /// An array of error codes that should be retried.
    /// e.g. `["SlowDown", "TooManyRequests"]`
    #[serde(
        default = "default::object_store_config::s3::developer::retryable_service_error_codes",
        alias = "object_store_retryable_service_error_codes"
    )]
    pub retryable_service_error_codes: Vec<String>,

    // TODO: deprecate this config when we are completely deprecate aws sdk.
    #[serde(default = "default::object_store_config::s3::developer::use_opendal")]
    pub use_opendal: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreRetryConfig {
    // A retry strategy driven by exponential back-off.
    // The retry strategy is used for all object store operations.
    /// Given a base duration for retry strategy in milliseconds.
    #[serde(default = "default::object_store_config::object_store_req_backoff_interval_ms")]
    pub req_backoff_interval_ms: u64,

    /// The max delay interval for the retry strategy. No retry delay will be longer than this `Duration`.
    #[serde(default = "default::object_store_config::object_store_req_backoff_max_delay_ms")]
    pub req_backoff_max_delay_ms: u64,

    /// A multiplicative factor that will be applied to the exponential back-off retry delay.
    #[serde(default = "default::object_store_config::object_store_req_backoff_factor")]
    pub req_backoff_factor: u64,

    /// Maximum timeout for `upload` operation
    #[serde(default = "default::object_store_config::object_store_upload_attempt_timeout_ms")]
    pub upload_attempt_timeout_ms: u64,

    /// Total counts of `upload` operation retries
    #[serde(default = "default::object_store_config::object_store_upload_retry_attempts")]
    pub upload_retry_attempts: usize,

    /// Maximum timeout for `streaming_upload_init` and `streaming_upload`
    #[serde(
        default = "default::object_store_config::object_store_streaming_upload_attempt_timeout_ms"
    )]
    pub streaming_upload_attempt_timeout_ms: u64,

    /// Total counts of `streaming_upload` operation retries
    #[serde(
        default = "default::object_store_config::object_store_streaming_upload_retry_attempts"
    )]
    pub streaming_upload_retry_attempts: usize,

    /// Maximum timeout for `read` operation
    #[serde(default = "default::object_store_config::object_store_read_attempt_timeout_ms")]
    pub read_attempt_timeout_ms: u64,

    /// Total counts of `read` operation retries
    #[serde(default = "default::object_store_config::object_store_read_retry_attempts")]
    pub read_retry_attempts: usize,

    /// Maximum timeout for `streaming_read_init` and `streaming_read` operation
    #[serde(
        default = "default::object_store_config::object_store_streaming_read_attempt_timeout_ms"
    )]
    pub streaming_read_attempt_timeout_ms: u64,

    /// Total counts of `streaming_read operation` retries
    #[serde(default = "default::object_store_config::object_store_streaming_read_retry_attempts")]
    pub streaming_read_retry_attempts: usize,

    /// Maximum timeout for `metadata` operation
    #[serde(default = "default::object_store_config::object_store_metadata_attempt_timeout_ms")]
    pub metadata_attempt_timeout_ms: u64,

    /// Total counts of `metadata` operation retries
    #[serde(default = "default::object_store_config::object_store_metadata_retry_attempts")]
    pub metadata_retry_attempts: usize,

    /// Maximum timeout for `delete` operation
    #[serde(default = "default::object_store_config::object_store_delete_attempt_timeout_ms")]
    pub delete_attempt_timeout_ms: u64,

    /// Total counts of `delete` operation retries
    #[serde(default = "default::object_store_config::object_store_delete_retry_attempts")]
    pub delete_retry_attempts: usize,

    /// Maximum timeout for `delete_object` operation
    #[serde(
        default = "default::object_store_config::object_store_delete_objects_attempt_timeout_ms"
    )]
    pub delete_objects_attempt_timeout_ms: u64,

    /// Total counts of `delete_object` operation retries
    #[serde(default = "default::object_store_config::object_store_delete_objects_retry_attempts")]
    pub delete_objects_retry_attempts: usize,

    /// Maximum timeout for `list` operation
    #[serde(default = "default::object_store_config::object_store_list_attempt_timeout_ms")]
    pub list_attempt_timeout_ms: u64,

    /// Total counts of `list` operation retries
    #[serde(default = "default::object_store_config::object_store_list_retry_attempts")]
    pub list_retry_attempts: usize,
}

mod default {
    pub mod object_store_config {
        pub fn set_atomic_write_dir() -> bool { false }
        pub fn object_store_req_backoff_interval_ms() -> u64 { 1000 }
        pub fn object_store_req_backoff_max_delay_ms() -> u64 { 10000 }
        pub fn object_store_req_backoff_factor() -> u64 { 2 }
        pub fn object_store_upload_attempt_timeout_ms() -> u64 { 8000 }
        pub fn object_store_upload_retry_attempts() -> usize { 3 }
        pub fn object_store_streaming_upload_attempt_timeout_ms() -> u64 { 480000 }
        pub fn object_store_streaming_upload_retry_attempts() -> usize { 3 }
        pub fn object_store_read_attempt_timeout_ms() -> u64 { 8000 }
        pub fn object_store_read_retry_attempts() -> usize { 3 }
        pub fn object_store_streaming_read_attempt_timeout_ms() -> u64 { 480000 }
        pub fn object_store_streaming_read_retry_attempts() -> usize { 3 }
        pub fn object_store_metadata_attempt_timeout_ms() -> u64 { 8000 }
        pub fn object_store_metadata_retry_attempts() -> usize { 3 }
        pub fn object_store_delete_attempt_timeout_ms() -> u64 { 5000 }
        pub fn object_store_delete_retry_attempts() -> usize { 3 }
        pub fn object_store_delete_objects_attempt_timeout_ms() -> u64 { 5000 }
        pub fn object_store_delete_objects_retry_attempts() -> usize { 3 }
        pub fn object_store_list_attempt_timeout_ms() -> u64 { 8000 }
        pub fn object_store_list_retry_attempts() -> usize { 3 }
        pub fn opendal_upload_concurrency() -> usize { 8 }
        pub fn upload_part_size() -> usize { 16 * 1024 * 1024 }

        pub mod s3 {
            pub fn keepalive_ms() -> Option<u64> { None }
            pub fn recv_buffer_size() -> Option<usize> { None }
            pub fn send_buffer_size() -> Option<usize> { None }
            pub fn nodelay() -> Option<bool> { None }
            pub fn identity_resolution_timeout_s() -> u64 { 5 }

            pub mod developer {
                pub fn retry_unknown_service_error() -> bool { false }
                pub fn retryable_service_error_codes() -> Vec<String> { vec![] }
                pub fn use_opendal() -> bool { true }
            }
        }
    }
}