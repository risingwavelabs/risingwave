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
use std::marker::{Send, Sync};

use risingwave_common::error::BoxedError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ObjectError {
    #[error(transparent)]
    S3(BoxedError),

    #[error("Internal error: {msg}")]
    Internal {
        msg: String,
        #[backtrace]
        backtrace: Backtrace,
    },
}

impl ObjectError {
    pub fn internal(msg: impl ToString) -> Self {
        Self::Internal {
            msg: msg.to_string(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl<E> From<aws_smithy_http::result::SdkError<E>> for ObjectError
where
    E: std::error::Error + Sync + Send + 'static,
{
    fn from(e: aws_smithy_http::result::SdkError<E>) -> Self {
        Self::S3(e.into())
    }
}

impl From<aws_smithy_http::byte_stream::Error> for ObjectError {
    fn from(e: aws_smithy_http::byte_stream::Error) -> Self {
        Self::S3(e.into())
    }
}

pub type ObjectResult<T> = std::result::Result<T, ObjectError>;
