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

use std::io;

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStreamError;
use aws_smithy_types::body::SdkBody;
use risingwave_common::error::BoxedError;
use thiserror::Error;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::error::RecvError;

#[derive(Error, thiserror_ext::ReportDebug, thiserror_ext::Box, thiserror_ext::Construct)]
#[thiserror_ext(newtype(name = ObjectError, backtrace))]
pub enum ObjectErrorInner {
    #[error("s3 error: {inner}")]
    S3 {
        // TODO: remove this after switch s3 backend to opendal
        should_retry: bool,
        #[source]
        inner: BoxedError,
    },
    #[error("disk error: {msg}")]
    Disk {
        msg: String,
        #[source]
        inner: io::Error,
    },
    #[error(transparent)]
    Opendal(#[from] opendal::Error),
    #[error(transparent)]
    Mem(#[from] crate::object::mem::Error),
    #[error("Internal error: {0}")]
    #[construct(skip)]
    Internal(String),
    #[cfg(madsim)]
    #[error(transparent)]
    Sim(#[from] crate::object::sim::SimError),

    #[error("Timeout error: {0}")]
    Timeout(String),
}

impl ObjectError {
    pub fn internal(msg: impl ToString) -> Self {
        ObjectErrorInner::Internal(msg.to_string()).into()
    }

    /// Tells whether the error indicates the target object is not found.
    pub fn is_object_not_found_error(&self) -> bool {
        match self.inner() {
            ObjectErrorInner::S3 {
                inner,
                should_retry: _,
            } => {
                if let Some(aws_smithy_runtime_api::client::result::SdkError::ServiceError(err)) =
                    inner.downcast_ref::<aws_smithy_runtime_api::client::result::SdkError<
                        GetObjectError,
                        aws_smithy_runtime_api::http::Response<SdkBody>,
                    >>()
                {
                    return matches!(err.err(), GetObjectError::NoSuchKey(_));
                }
                if let Some(aws_smithy_runtime_api::client::result::SdkError::ServiceError(err)) =
                    inner.downcast_ref::<aws_smithy_runtime_api::client::result::SdkError<
                        HeadObjectError,
                        aws_smithy_runtime_api::http::Response<SdkBody>,
                    >>()
                {
                    return matches!(err.err(), HeadObjectError::NotFound(_));
                }
            }
            ObjectErrorInner::Opendal(e) => {
                return matches!(e.kind(), opendal::ErrorKind::NotFound);
            }
            ObjectErrorInner::Disk { msg: _msg, inner } => {
                return matches!(inner.kind(), io::ErrorKind::NotFound);
            }
            ObjectErrorInner::Mem(e) => {
                return e.is_object_not_found_error();
            }
            #[cfg(madsim)]
            ObjectErrorInner::Sim(e) => {
                return e.is_object_not_found_error();
            }
            _ => {}
        };
        false
    }

    pub fn should_retry(&self, retry_opendal_s3_unknown_error: bool) -> bool {
        match self.inner() {
            ObjectErrorInner::S3 {
                inner: _,
                should_retry,
            } => *should_retry,

            ObjectErrorInner::Opendal(e) => {
                e.is_temporary()
                    || (retry_opendal_s3_unknown_error
                        && e.kind() == opendal::ErrorKind::Unexpected)
            }

            ObjectErrorInner::Timeout(_) => true,

            _ => false,
        }
    }
}

impl<E, R> From<aws_smithy_runtime_api::client::result::SdkError<E, R>> for ObjectError
where
    E: std::error::Error + Sync + Send + 'static,
    R: Send + Sync + 'static + std::fmt::Debug,
{
    fn from(e: aws_smithy_runtime_api::client::result::SdkError<E, R>) -> Self {
        ObjectErrorInner::S3 {
            inner: e.into(),
            should_retry: false,
        }
        .into()
    }
}

impl From<RecvError> for ObjectError {
    fn from(e: RecvError) -> Self {
        ObjectErrorInner::Internal(e.to_report_string()).into()
    }
}

impl From<ByteStreamError> for ObjectError {
    fn from(e: ByteStreamError) -> Self {
        ObjectErrorInner::S3 {
            inner: e.into(),
            should_retry: true,
        }
        .into()
    }
}

#[cfg(madsim)]
impl From<std::io::Error> for ObjectError {
    fn from(e: std::io::Error) -> Self {
        ObjectErrorInner::Internal(e.to_string()).into()
    }
}

pub type ObjectResult<T> = std::result::Result<T, ObjectError>;
