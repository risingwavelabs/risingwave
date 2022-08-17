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
use std::num::ParseIntError;
use std::sync::Arc;

use rdkafka::error::KafkaError;
use risingwave_common::array::ArrayError;
use risingwave_pb::ProstFieldNotFound;

pub type SourceResult<T> = std::result::Result<T, SourceError>;

#[derive(thiserror::Error, Debug)]
enum SourceErrorInner {
    #[error("Send error: {0}")]
    SendError(String),
    #[error("Sdk error: {0}")]
    SdkError(String),
    #[error("FileSystemOpt error: {0}")]
    FileSystemOptError(String),
    #[error("Kafka error: {0}")]
    KafkaError(KafkaError),
    #[error("Pulsar error: {0}")]
    PulsarError(pulsar::Error),
    #[error("ProstDecode error: {0}")]
    ProstDecodeError(prost::DecodeError),
    #[error("Array error: {0}")]
    ArrayError(ArrayError),
    #[error("IO error: {0}")]
    IoError(std::io::Error),
    #[error("Json error: {0}")]
    JsonError(serde_json::Error),
    #[error("ParseInt error: {0}")]
    ParseIntError(ParseIntError),
    #[error("ProstFieldNotFound error: {0}")]
    ProstFieldNotFoundError(String),
    #[error(transparent)]
    Internal(anyhow::Error),
    #[error(transparent)]
    BaseSourceError(String),
}

impl From<SourceErrorInner> for SourceError {
    fn from(inner: SourceErrorInner) -> Self {
        Self {
            inner: Arc::new(inner),
            backtrace: Arc::new(Backtrace::capture()),
        }
    }
}

#[derive(thiserror::Error, Clone)]
#[error("{inner}")]
pub struct SourceError {
    inner: Arc<SourceErrorInner>,
    backtrace: Arc<Backtrace>,
}

impl std::fmt::Debug for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = self.inner.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(f, "  backtrace of `MetaError`:\n{}", self.backtrace)?;
        }
        Ok(())
    }
}

impl SourceError {
    pub fn into_source_error(s: String) -> Self {
        SourceErrorInner::SourceError(s).into()
    }

    pub fn send_error(s: String) -> Self {
        SourceErrorInner::SendError(s).into()
    }

    pub fn sdk_error(s: String) -> Self {
        SourceErrorInner::SdkError(s).into()
    }

    pub fn filesystem_opt_error(s: String) -> Self {
        SourceErrorInner::FileSystemOptError(s).into()
    }
}

impl From<KafkaError> for SourceError {
    fn from(e: KafkaError) -> Self {
        SourceErrorInner::KafkaError(e).into()
    }
}

impl From<ArrayError> for SourceError {
    fn from(e: ArrayError) -> Self {
        SourceErrorInner::ArrayError(e).into()
    }
}

impl From<ProstFieldNotFound> for SourceError {
    fn from(e: ProstFieldNotFound) -> Self {
        SourceErrorInner::ProstFieldNotFoundError(e.0.to_string()).into()
    }
}

impl From<serde_json::Error> for SourceError {
    fn from(e: serde_json::Error) -> Self {
        SourceErrorInner::JsonError(e).into()
    }
}

impl From<ParseIntError> for SourceError {
    fn from(e: ParseIntError) -> Self {
        SourceErrorInner::ParseIntError(e).into()
    }
}

impl From<prost::DecodeError> for SourceError {
    fn from(e: prost::DecodeError) -> Self {
        SourceErrorInner::ProstDecodeError(e).into()
    }
}

impl From<anyhow::Error> for SourceError {
    fn from(a: anyhow::Error) -> Self {
        SourceErrorInner::Internal(a).into()
    }
}

impl From<globset::Error> for SourceError {
    fn from(a: globset::Error) -> Self {
        SourceErrorInner::Internal(a.into()).into()
    }
}

impl From<pulsar::Error> for SourceError {
    fn from(a: pulsar::Error) -> Self {
        SourceErrorInner::PulsarError(a).into()
    }
}

impl From<std::io::Error> for SourceError {
    fn from(a: std::io::Error) -> Self {
        SourceErrorInner::IoError(a).into()
    }
}
