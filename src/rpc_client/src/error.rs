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

use risingwave_common::util::meta_addr::MetaAddressStrategyParseError;
use risingwave_error::tonic::TonicStatusWrapperExt as _;
use thiserror::Error;
use thiserror_ext::Construct;

pub type Result<T, E = RpcError> = std::result::Result<T, E>;

// Re-export these types as they're commonly used together with `RpcError`.
pub use risingwave_error::tonic::{ToTonicStatus, TonicStatusWrapper};

#[derive(Error, Debug, Construct)]
pub enum RpcError {
    #[error(transparent)]
    TransportError(Box<tonic::transport::Error>),

    #[error(transparent)]
    GrpcStatus(
        #[from]
        // Typically it does not have a backtrace,
        // but this is to let `thiserror` generate `provide` implementation to make `Extra` work.
        // See `risingwave_error::tonic::extra`.
        #[backtrace]
        Box<TonicStatusWrapper>,
    ),

    #[error(transparent)]
    MetaAddressParse(#[from] MetaAddressStrategyParseError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
}

// TODO: use `thiserror_ext::Box`
static_assertions::const_assert_eq!(std::mem::size_of::<RpcError>(), 32);

impl From<tonic::transport::Error> for RpcError {
    fn from(e: tonic::transport::Error) -> Self {
        RpcError::TransportError(Box::new(e))
    }
}

/// Intentionally not implemented to enforce using `RpcError::from_xxx_status`, so that
/// the service name can always be included in the error message.
impl !From<tonic::Status> for RpcError {}

macro_rules! impl_from_status {
    ($($service:ident),* $(,)?) => {
        paste::paste! {
            impl RpcError {
                $(
                    #[doc = "Convert a gRPC status from " $service " service into an [`RpcError`]."]
                    pub fn [<from_ $service _status>](s: tonic::Status) -> Self {
                        Box::new(s.with_client_side_service_name(stringify!($service))).into()
                    }
                )*
            }
        }
    };
}

impl_from_status!(stream, batch, meta, compute, compactor, connector, frontend);

impl RpcError {
    /// Returns `true` if the error is a connection error. Typically used to determine if
    /// the error is transient and can be retried.
    pub fn is_connection_error(&self) -> bool {
        match self {
            RpcError::TransportError(_) => true,
            RpcError::GrpcStatus(status) => matches!(
                status.inner().code(),
                tonic::Code::Unavailable // server not started
                 | tonic::Code::Unknown // could be transport error
                 | tonic::Code::Unimplemented // meta leader service not started
            ),
            RpcError::MetaAddressParse(_) => false,
            RpcError::Internal(anyhow) => anyhow
                .downcast_ref::<Self>() // this skips all contexts attached to the error
                .is_some_and(Self::is_connection_error),
        }
    }
}
