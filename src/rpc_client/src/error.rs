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

use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

pub type Result<T, E = RpcError> = std::result::Result<T, E>;

// Re-export these types as they're commonly used together with `RpcError`.
pub use risingwave_error::tonic::{ToTonicStatus, TonicStatusWrapper};

#[derive(Error, Debug)]
pub enum RpcError {
    #[error(transparent)]
    TransportError(Box<tonic::transport::Error>),

    #[error(transparent)]
    GrpcStatus(Box<TonicStatusWrapper>),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
}

static_assertions::const_assert_eq!(std::mem::size_of::<RpcError>(), 16);

impl From<tonic::transport::Error> for RpcError {
    fn from(e: tonic::transport::Error) -> Self {
        RpcError::TransportError(Box::new(e))
    }
}

impl From<tonic::Status> for RpcError {
    fn from(s: tonic::Status) -> Self {
        RpcError::GrpcStatus(Box::new(TonicStatusWrapper::new(s)))
    }
}

impl From<RpcError> for RwError {
    fn from(r: RpcError) -> Self {
        match r {
            RpcError::GrpcStatus(status) => TonicStatusWrapper::into(*status),
            _ => ErrorCode::RpcError(r.into()).into(),
        }
    }
}
