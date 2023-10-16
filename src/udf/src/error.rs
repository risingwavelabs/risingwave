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

use arrow_flight::error::FlightError;

/// A specialized `Result` type for UDF operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The error type for UDF operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to connect to UDF service: {0}")]
    Connect(#[from] tonic::transport::Error),

    #[error("failed to send requests to UDF service: {0}")]
    Tonic(#[from] Box<tonic::Status>),

    #[error("failed to call UDF: {0}")]
    Flight(#[from] Box<FlightError>),

    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("UDF unsupported: {0}")]
    Unsupported(String),

    #[error("UDF service returned no data")]
    NoReturned,

    #[error("Flight service error: {0}")]
    ServiceError(String),
}

static_assertions::const_assert_eq!(std::mem::size_of::<Error>(), 40);

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::from(Box::new(status))
    }
}

impl From<FlightError> for Error {
    fn from(error: FlightError) -> Self {
        Error::from(Box::new(error))
    }
}
