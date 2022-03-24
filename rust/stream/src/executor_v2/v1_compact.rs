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

use std::fmt;

use async_trait::async_trait;
use futures::StreamExt;
pub use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

pub use super::{BoxedMessageStream, ExecutorV1, Message, PkIndices, PkIndicesRef};

/// The struct wraps a [`BoxedMessageStream`] and implements the interface of [`ExecutorV1`].
///
/// With this wrapper, we can migrate our executors from v1 to v2 step by step.
pub struct StreamExecutorV1 {
    /// The wrapped stream.
    pub(super) stream: BoxedMessageStream,

    pub(super) schema: Schema,
    pub(super) pk_indices: PkIndices,
    pub(super) identity: String,
}

impl fmt::Debug for StreamExecutorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamExecutor")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .field("identity", &self.identity)
            .finish()
    }
}

#[async_trait]
impl ExecutorV1 for StreamExecutorV1 {
    async fn next(&mut self) -> Result<Message> {
        self.stream.next().await.unwrap()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn logical_operator_info(&self) -> &str {
        // FIXME: use identity temporally.
        &self.identity
    }
}
