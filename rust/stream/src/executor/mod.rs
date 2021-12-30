use std::collections::HashMap;

pub use actor::Actor;
pub use aggregation::*;
use async_trait::async_trait;
pub use dispatch::*;
pub use filter::*;
pub use hash_agg::*;
pub use hash_join::*;
pub use managed_state::aggregation::{OrderedArraysSerializer, OrderedRowsSerializer};
pub use merge::*;
pub use mview::*;
pub use project::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayImpl, DataChunk, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::types::DataTypeKind;
use risingwave_pb::data::stream_message::StreamMessage;
use risingwave_pb::data::{Barrier as ProstBarrier, StreamMessage as ProstStreamMessage};
use risingwave_pb::stream_service::ActorInfo;
pub use simple_agg::*;
use smallvec::SmallVec;
pub use stream_source::*;
pub use top_n::*;
pub use top_n_appendonly::*;

mod actor;
mod aggregation;
mod barrier_align;
mod dispatch;
mod filter;
mod hash_agg;
mod hash_join;
mod managed_state;
mod merge;
mod mview;
mod project;
mod simple_agg;
mod stream_source;
mod top_n;
mod top_n_appendonly;

#[cfg(test)]
mod integration_tests;

#[cfg(test)]
mod test_utils;

pub trait ExprFn = Fn(&DataChunk) -> Result<Bitmap> + Send + Sync + 'static;

#[derive(Debug, Clone)]
pub enum Mutation {
    Nothing,
    Stop,
    UpdateOutputs(HashMap<u32, Vec<ActorInfo>>),
}
impl Default for Mutation {
    fn default() -> Self {
        Mutation::Nothing
    }
}

#[derive(Default, Debug, Clone)]
pub struct Barrier {
    pub epoch: u64,
    pub mutation: Mutation,
}

impl Mutation {
    fn is_stop(&self) -> bool {
        matches!(self, Mutation::Stop)
    }
}

impl Barrier {
    fn to_protobuf(&self) -> ProstBarrier {
        let Barrier { epoch, mutation }: Barrier = self.clone();
        let stop = matches!(mutation, Mutation::Nothing);
        ProstBarrier { epoch, stop }
    }

    fn from_protobuf(prost: &ProstBarrier) -> Self {
        let ProstBarrier { epoch, stop } = *prost;
        Barrier {
            epoch,
            mutation: if stop {
                Mutation::Stop
            } else {
                Mutation::Nothing
            },
        }
    }
}

#[derive(Debug)]
pub enum Message {
    Chunk(StreamChunk),
    Barrier(Barrier),
}

impl Message {
    /// Return true if the message is a stop barrier, meaning the stream
    /// will not continue, false otherwise.
    pub fn is_terminate(&self) -> bool {
        matches!(
            self,
            Message::Barrier(Barrier {
                epoch: _,
                mutation: Mutation::Stop,
            })
        )
    }

    pub fn to_protobuf(&self) -> Result<ProstStreamMessage> {
        let prost = match self {
            Self::Chunk(stream_chunk) => {
                let prost_stream_chunk = stream_chunk.to_protobuf()?;
                StreamMessage::StreamChunk(prost_stream_chunk)
            }
            Self::Barrier(barrier) => StreamMessage::Barrier(barrier.clone().to_protobuf()),
        };
        let prost_stream_msg = ProstStreamMessage {
            stream_message: Some(prost),
        };
        Ok(prost_stream_msg)
    }

    pub fn from_protobuf(prost: ProstStreamMessage) -> Result<Self> {
        let res = match prost.get_stream_message() {
            StreamMessage::StreamChunk(stream_chunk) => {
                Message::Chunk(StreamChunk::from_protobuf(stream_chunk)?)
            }
            StreamMessage::Barrier(epoch) => Message::Barrier(Barrier {
                epoch: epoch.get_epoch(),
                ..Barrier::default()
            }),
        };
        Ok(res)
    }
}

/// `Executor` supports handling of control messages.
#[async_trait]
pub trait Executor: Send + 'static {
    async fn next(&mut self) -> Result<Message>;

    /// Return the schema of the OUTPUT of the executor.
    fn schema(&self) -> &Schema;

    /// Return the primary key indices of the OUTPUT of the executor.
    /// Schema is used by both OLAP and streaming, therefore
    /// pk indices are maintained independently.
    fn pk_indices(&self) -> PkIndicesRef;

    fn pk_data_type_kinds(&self) -> PkDataTypeKinds {
        let schema = self.schema();
        self.pk_indices()
            .iter()
            .map(|idx| schema.fields[*idx].data_type.data_type_kind())
            .collect()
    }
}

pub type PkIndices = Vec<usize>;
pub type PkIndicesRef<'a> = &'a [usize];
pub type PkDataTypeKinds = SmallVec<[DataTypeKind; 1]>;

// Get inputs by given `pk_indices` from `columns`.
pub fn pk_input_arrays<'a>(pk_indices: PkIndicesRef, columns: &'a [Column]) -> Vec<&'a ArrayImpl> {
    pk_indices
        .iter()
        .map(|pk_idx| columns[*pk_idx].array_ref())
        .collect()
}

/// `SimpleExecutor` accepts a single chunk as input.
pub trait SimpleExecutor: Executor {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message>;
    fn input(&mut self) -> &mut dyn Executor;
}

/// Most executors don't care about the control messages, and therefore
/// this method provides a default implementation helper for them.
async fn simple_executor_next<E: SimpleExecutor>(executor: &mut E) -> Result<Message> {
    match executor.input().next().await {
        Ok(message) => match message {
            Message::Chunk(chunk) => executor.consume_chunk(chunk),
            Message::Barrier(_) => Ok(message),
        },
        Err(e) => Err(e),
    }
}

/// `StreamConsumer` is the last step in a fragment
#[async_trait]
pub trait StreamConsumer: Send + 'static {
    /// Run next stream chunk. returns whether the stream is terminated
    async fn next(&mut self) -> Result<bool>;
}
