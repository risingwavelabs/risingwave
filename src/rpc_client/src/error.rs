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
    /// Convert the error to [`tonic::Status`] with the given [`tonic::Code`].
    ///
    /// The source chain is preserved by pairing with [`RpcError`].
    // TODO(error-handling): disallow constructing `tonic::Status` directly with `new` by clippy.
    pub fn to_status(&self, code: tonic::Code) -> tonic::Status {
        // Embed the whole error (`self`) and its source chain into the details field.
        // At the same time, set the message field to the error message of `self` (without source chain).
        // The redundancy of the current error's message is intentional.
        let source = serde_error::Error::new(self);
        let details = bincode::serialize(&source).unwrap_or_default();

        let mut status = tonic::Status::with_details(code, self.to_string(), details.into());
        // Set the source of `tonic::Status`, though it's not likely to be used.
        // This is only available before serializing to the wire. That's why we need to manually embed it
        // into the `details` field.
        status.set_source(Arc::new(source));
        status
    }
}

/// A wrapper of [`tonic::Status`] that provides better error message.
#[derive(Debug)]
pub struct TonicStatusWrapper(pub tonic::Status);

impl std::fmt::Display for TonicStatusWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gRPC error ({}): {}", self.0.code(), self.0.message())
    }
}
impl std::error::Error for TonicStatusWrapper {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Delegate to `self.0` as if there's no new layer introduced.
        self.0.source()
    }
}

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
    fn from(mut s: tonic::Status) -> Self {
        // Try to extract the source chain from the `details` field. See `ToTonicStatus::to_status` above.
        if let Ok(e) = bincode::deserialize::<serde_error::Error>(s.details()) {
            s.set_source(Arc::new(e));
        }
        RpcError::GrpcStatus(Box::new(TonicStatusWrapper(s)))
    }
}

impl From<RpcError> for RwError {
    fn from(r: RpcError) -> Self {
        use tonic::Code;

        match r {
            // TODO(error-handling): `message().to_string()` loses the source chain.
            RpcError::GrpcStatus(status) => {
                let code = status.as_ref().0.code();
                let message = status.as_ref().0.message();

                match code {
                    Code::InvalidArgument => {
                        ErrorCode::InvalidParameterValue(message.to_string()).into()
                    }
                    Code::NotFound | Code::AlreadyExists => {
                        ErrorCode::CatalogError(status.into()).into()
                    }
                    Code::PermissionDenied => {
                        ErrorCode::PermissionDenied(message.to_string()).into()
                    }
                    Code::Cancelled => ErrorCode::SchedulerError(status.into()).into(),

                    _ => ErrorCode::RpcError(status.into()).into(),
                }
            }

            _ => ErrorCode::RpcError(r.into()).into(),
        }
    }
}
