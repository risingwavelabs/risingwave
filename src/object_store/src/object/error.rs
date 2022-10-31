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

use std::backtrace::Backtrace;
use std::io;
use std::marker::{Send, Sync};

use risingwave_common::error::BoxedError;
use thiserror::Error;

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

    pub fn s3(err: impl Into<BoxedError>) -> Self {
        ObjectErrorInner::S3(err.into()).into()
    }
}

impl<E> From<aws_sdk_s3::types::SdkError<E>> for ObjectError
where
    E: std::error::Error + Sync + Send + 'static,
{
    fn from(e: aws_sdk_s3::types::SdkError<E>) -> Self {
        ObjectErrorInner::S3(e.into()).into()
    }
}

impl From<aws_smithy_http::byte_stream::Error> for ObjectError {
    fn from(e: aws_smithy_http::byte_stream::Error) -> Self {
        ObjectErrorInner::S3(e.into()).into()
    }
}

pub type ObjectResult<T> = std::result::Result<T, ObjectError>;
