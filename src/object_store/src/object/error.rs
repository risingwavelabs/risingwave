// Copyright 2023 RisingWave Labs
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

use std::backtrace::Backtrace;
use std::io;
use std::marker::{Send, Sync};

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStreamError;
use risingwave_common::error::BoxedError;
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;

use crate::object::Error;

#[derive(Error, Debug)]
enum ObjectErrorInner {
    #[error(transparent)]
    S3(BoxedError),
    #[error("disk error: {msg}")]
    Disk {
        msg: String,
        #[source]
        inner: io::Error,
    },
    #[error(transparent)]
    Opendal(opendal::Error),
    #[error(transparent)]
    Mem(crate::object::mem::Error),
    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Error)]
#[error("{inner}")]
pub struct ObjectError {
    #[from]
    inner: ObjectErrorInner,

    backtrace: Backtrace,
}

impl std::fmt::Debug for ObjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = (&self.inner as &dyn Error).request_ref::<Backtrace>() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(f, "  backtrace of `ObjectError`:\n{}", self.backtrace)?;
        }
        Ok(())
    }
}

impl ObjectError {
    pub fn internal(msg: impl ToString) -> Self {
        ObjectErrorInner::Internal(msg.to_string()).into()
    }

    pub fn disk(msg: String, err: io::Error) -> Self {
        ObjectErrorInner::Disk { msg, inner: err }.into()
    }

    /// Tells whether the error indicates the target object is not found.
    pub fn is_object_not_found_error(&self) -> bool {
        match &self.inner {
            ObjectErrorInner::S3(e) => {
                if let Some(aws_smithy_http::result::SdkError::ServiceError(err)) =
                    e.downcast_ref::<aws_smithy_http::result::SdkError<GetObjectError>>()
                {
                    return matches!(err.err(), GetObjectError::NoSuchKey(_));
                }
                if let Some(aws_smithy_http::result::SdkError::ServiceError(err)) =
                    e.downcast_ref::<aws_smithy_http::result::SdkError<HeadObjectError>>()
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
            _ => {}
        };
        false
    }
}

impl<E> From<aws_sdk_s3::error::SdkError<E>> for ObjectError
where
    E: std::error::Error + Sync + Send + 'static,
{
    fn from(e: aws_sdk_s3::error::SdkError<E>) -> Self {
        ObjectErrorInner::S3(e.into()).into()
    }
}

// impl From<aws_smithy_http::byte_stream::Error> for ObjectError {
//     fn from(e: aws_smithy_http::byte_stream::Error) -> Self {
//         ObjectErrorInner::S3(e.into()).into()
//     }
// }
impl From<opendal::Error> for ObjectError {
    fn from(e: opendal::Error) -> Self {
        ObjectErrorInner::Opendal(e).into()
    }
}

impl From<RecvError> for ObjectError {
    fn from(e: RecvError) -> Self {
        ObjectErrorInner::Internal(e.to_string()).into()
    }
}

impl From<ByteStreamError> for ObjectError {
    fn from(e: ByteStreamError) -> Self {
        ObjectErrorInner::Internal(e.to_string()).into()
    }
}

impl From<crate::object::mem::Error> for ObjectError {
    fn from(e: Error) -> Self {
        ObjectErrorInner::Mem(e).into()
    }
}

pub type ObjectResult<T> = std::result::Result<T, ObjectError>;
