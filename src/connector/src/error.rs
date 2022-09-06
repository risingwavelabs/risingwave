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

use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("Parse error: {0}")]
    Parse(&'static str),

    #[error("Invalid parameter {name}: {reason}")]
    InvalidParam { name: &'static str, reason: String },

    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<ConnectorError> for RwError {
    fn from(s: ConnectorError) -> Self {
        ErrorCode::ConnectorError(Box::new(s)).into()
    }
}
