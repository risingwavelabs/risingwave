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

use std::sync::Arc;

pub use anyhow::anyhow;
use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

pub type Result<T, E = RpcError> = std::result::Result<T, E>;

#[easy_ext::ext(ToTonicStatus)]
impl<T> T
where
    T: ?Sized + std::error::Error,
{
    pub fn to_status(&self, code: tonic::Code) -> tonic::Status {
        // TODO: disallowed methods clippy.
        match self.source() {
            Some(source) => {
                let source = serde_error::Error::new(source);
                let details = bincode::serialize(&source).unwrap_or_default();

                let mut status =
                    tonic::Status::with_details(code, self.to_string(), details.into());
                status.set_source(Arc::new(source));
                status
            }

            None => tonic::Status::new(code, self.to_string()),
        }
    }
}

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("Transport error: {0}")]
    TransportError(#[source] Box<tonic::transport::Error>),

    #[error("gRPC error ({}): {}", .0.code(), .0.message())]
    GrpcStatus(#[source] Box<tonic::Status>),

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
    fn from(mut s: tonic::Status) -> Self {
        if let Ok(e) = bincode::deserialize::<serde_error::Error>(s.details()) {
            s.set_source(Arc::new(e));
        }
        RpcError::GrpcStatus(Box::new(s))
    }
}

impl From<RpcError> for RwError {
    fn from(r: RpcError) -> Self {
        use tonic::Code;

        // TODO(error-handling): `message().to_string()` loses the source chain.
        match r {
            RpcError::GrpcStatus(status) => match status.code() {
                Code::InvalidArgument => {
                    ErrorCode::InvalidParameterValue(status.message().to_string()).into()
                }
                Code::NotFound | Code::AlreadyExists => {
                    ErrorCode::CatalogError(status.into()).into()
                }
                Code::PermissionDenied => {
                    ErrorCode::PermissionDenied(status.message().to_string()).into()
                }
                Code::Cancelled => ErrorCode::SchedulerError(status.into()).into(),

                _ => ErrorCode::RpcError(status.into()).into(),
            },

            _ => ErrorCode::RpcError(r.into()).into(),
        }
    }
}
