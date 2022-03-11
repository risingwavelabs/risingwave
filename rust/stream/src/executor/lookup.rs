use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_storage::StateStore;

mod sides;
use self::sides::*;
mod impl_;

#[cfg(test)]
mod tests;

use super::{Barrier, Executor, Message, PkIndices, PkIndicesRef};

pub type BoxedArrangeStream = Pin<Box<dyn Stream<Item = Result<ArrangeMessage>> + Send>>;

/// `LookupExecutor` takes one input stream and one arrangement. It joins the input stream with the
/// arrangement. Currently, it only supports inner join. See [`LookupExecutorParams`] for more
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
    input: BoxedArrangeStream,

    /// The last received barrier.
    last_barrier: Option<Barrier>,
}

impl<S: StateStore> std::fmt::Debug for LookupExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupExecutor")
            .field("output_data_types", &self.output_data_types)
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .field("arrangement", &self.arrangement)
            .field("stream", &self.stream)
            .field("last_barrier", &self.last_barrier)
            .finish()
    }
}

#[async_trait]
impl<S: StateStore> Executor for LookupExecutor<S> {
    async fn next(&mut self) -> Result<Message> {
        loop {
            if let Some(msg) = self.next_inner().await? {
                return Ok(msg);
            }
        }
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

    fn logical_operator_info(&self) -> &str {
        "LookupExecutor"
    }

    fn clear_cache(&mut self) -> Result<()> {
        Ok(())
    }

    fn reset(&mut self, _epoch: u64) {
        self.last_barrier = None;
    }
}
