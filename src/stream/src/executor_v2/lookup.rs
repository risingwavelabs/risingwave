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

use std::pin::Pin;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_storage::StateStore;

use crate::executor_v2::{Barrier, BoxedMessageStream, Executor, PkIndices, PkIndicesRef};

mod sides;
use self::sides::*;
mod impl_;

#[cfg(test)]
mod tests;

type BoxedArrangeStream = Pin<Box<dyn Stream<Item = Result<ArrangeMessage>> + Send>>;

/// `LookupExecutor` takes one input stream and one arrangement. It joins the input stream with the
/// arrangement. Currently, it only supports inner join. See `LookupExecutorParams` for more
/// information.
///
/// The output schema is `| stream columns | arrangement columns |`.
pub struct LookupExecutor<S: StateStore> {
    /// the data types of the formed new columns
    output_data_types: Vec<DataType>,

    /// The schema of the lookup executor
    schema: Schema,

    /// The primary key indices of the schema
    pk_indices: PkIndices,

    /// The join side of the arrangement
    arrangement: ArrangeJoinSide<S>,

    /// The join side of the stream
    stream: StreamJoinSide,

    /// The combined input from arrangement and stream
    input: Option<BoxedArrangeStream>,

    /// The last received barrier.
    last_barrier: Option<Barrier>,
}

#[async_trait]
impl<S: StateStore> Executor for LookupExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "<unknown>"
    }
}
