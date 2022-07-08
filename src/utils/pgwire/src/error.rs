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

use std::io::Error as IoError;

use thiserror::Error;

use crate::pg_server::BoxedError;

/// Error type used in pgwire crates.
#[derive(Error, Debug)]
pub enum PsqlError {
    #[error("Encode error {0}.")]
    CancelError(String),

    #[error("{0}")]
    UnrecognizedParamError(String),

    #[error("{0}")]
    IoError(IoError),

    // The difference between IoError and ReadMsgIoError is that ReadMsgIoError needed to report
    // to users but IoError does not.
    #[error("{0}")]
    ReadMsgIoError(IoError),

    // InternalError return from frontend(comes from internal syste)m, it's wrapper of RwError.
    #[error("{0}")]
    InternalError(BoxedError),
}

impl PsqlError {
    /// Construct a Cancel error. Used when Ctrl-c a processing query. Similar to PG.
    pub fn cancel() -> Self {
        PsqlError::CancelError("ERROR:  canceling statement due to user request".to_string())
    }

    pub fn unrecognized_param(param: &str) -> Self {
        PsqlError::UnrecognizedParamError(format!("ERROR: unrecognized parameter {}", param))
    }
}
