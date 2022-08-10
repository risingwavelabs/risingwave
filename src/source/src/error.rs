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
use std::sync::Arc;

pub type SourceResult<T> = std::result::Result<T, SourceError>;

#[derive(thiserror::Error, Debug)]
enum SourceErrorInner {
    #[error("SourceReader error: {0}")]
    SourceReaderError(String),
    #[error(transparent)]
    Internal(anyhow::Error),
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
impl From<anyhow::Error> for SourceError {
    fn from(a: anyhow::Error) -> Self {
        SourceErrorInner::Internal(a).into()
    }
}