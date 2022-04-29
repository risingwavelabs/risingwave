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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

pub use batch_query::*;
pub use chain::*;
pub use filter::*;
pub use global_simple_agg::*;
pub use hash_agg::*;
pub use local_simple_agg::*;
pub use merge::*;
pub use mview::*;
pub use project::*;
pub use top_n::*;
pub use top_n_appendonly::*;

use crate::executor_v2::{
    BoxedExecutor, HashJoinExecutorBuilder, HopWindowExecutorBuilder, LookupExecutorBuilder,
    LookupUnionExecutorBuilder, SourceExecutorBuilder, UnionExecutorBuilder,
};
use crate::task::{
    ActorId, DispatcherId, ExecutorParams, LocalStreamManagerCore, ENABLE_BARRIER_AGGREGATION,
};

mod batch_query;
mod chain;
mod filter;
mod global_simple_agg;
mod hash_agg;
mod local_simple_agg;
mod merge;
mod mview;
mod project;
mod top_n;
mod top_n_appendonly;

use enum_as_inner::EnumAsInner;
use futures::Stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayImpl, ArrayRef, DataChunk, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::data::barrier::Mutation as ProstMutation;
use risingwave_pb::data::stream_message::StreamMessage;
use risingwave_pb::data::{
    AddMutation, Barrier as ProstBarrier, DispatcherMutation, Epoch as ProstEpoch, NothingMutation,
    StopMutation, StreamMessage as ProstStreamMessage, UpdateMutation,
};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;
use smallvec::SmallVec;
use tracing::trace_span;

pub const INVALID_EPOCH: u64 = 0;

pub trait ExprFn = Fn(&DataChunk) -> Result<Bitmap> + Send + Sync + 'static;

/// Boxed stream of [`StreamMessage`].
pub type BoxedExecutorV1Stream = Pin<Box<dyn Stream<Item = Result<Message>> + Send>>;

#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    Stop(HashSet<ActorId>),
    UpdateOutputs(HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>),
    AddOutput(HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Epoch {
    pub curr: u64,
    pub prev: u64,
}

impl Epoch {
    pub fn new(curr: u64, prev: u64) -> Self {
        assert!(curr > prev);
        Self { curr, prev }
    }

    pub fn inc(&self) -> Self {
        Self {
            curr: self.curr + 1,
            prev: self.prev + 1,
        }
    }

    pub fn new_test_epoch(curr: u64) -> Self {
        assert!(curr > 0);
        Self::new(curr, curr - 1)
    }
}

impl Default for Epoch {
    fn default() -> Self {
        Self {
            curr: 1,
            prev: INVALID_EPOCH,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Barrier {
    pub epoch: Epoch,
    pub mutation: Option<Arc<Mutation>>,
    pub span: tracing::Span,
}

impl Default for Barrier {
    fn default() -> Self {
        Self {
            span: tracing::Span::none(),
            epoch: Epoch::default(),
            mutation: None,
        }
    }
}

impl Barrier {
    /// Create a plain barrier.
    pub fn new_test_barrier(epoch: u64) -> Self {
        Self {
            epoch: Epoch::new_test_epoch(epoch),
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_mutation(self, mutation: Mutation) -> Self {
        Self {
            mutation: Some(Arc::new(mutation)),
            ..self
        }
    }

    #[must_use]
    pub fn with_stop(self) -> Self {
        self.with_mutation(Mutation::Stop(HashSet::default()))
    }

    // TODO: The barrier should always contain trace info after we migrated barrier generation to
    // meta service.
    #[must_use]
    pub fn with_span(self, span: tracing::span::Span) -> Self {
        Self { span, ..self }
    }

    pub fn is_to_stop_actor(&self, actor_id: ActorId) -> bool {
        matches!(self.mutation.as_deref(), Some(Mutation::Stop(actors)) if actors.contains(&actor_id))
    }

    pub fn is_to_add_output(&self, actor_id: ActorId) -> bool {
        matches!(
            self.mutation.as_deref(),
            Some(Mutation::AddOutput(map)) if map
                .values()
                .flatten()
                .any(|info| info.actor_id == actor_id)
        )
    }
}

impl PartialEq for Barrier {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.mutation == other.mutation
    }
}

impl Mutation {
    /// Return true if the mutation is stop.
    ///
    /// Note that this does not mean we will stop the current actor.
    #[cfg(test)]
    pub fn is_stop(&self) -> bool {
        matches!(self, Mutation::Stop(_))
    }
}

impl Barrier {
    pub fn to_protobuf(&self) -> ProstBarrier {
        let Barrier {
            epoch, mutation, ..
        }: Barrier = self.clone();
        ProstBarrier {
            epoch: Some(ProstEpoch {
                curr: epoch.curr,
                prev: epoch.prev,
            }),
            mutation: match mutation.as_deref() {
                None => Some(ProstMutation::Nothing(NothingMutation {})),
                Some(Mutation::Stop(actors)) => Some(ProstMutation::Stop(StopMutation {
                    actors: actors.iter().cloned().collect::<Vec<_>>(),
                })),
                Some(Mutation::UpdateOutputs(updates)) => {
                    Some(ProstMutation::Update(UpdateMutation {
                        mutations: updates
                            .iter()
                            .map(|(&(actor_id, dispatcher_id), actors)| DispatcherMutation {
                                actor_id,
                                dispatcher_id,
                                info: actors.clone(),
                            })
                            .collect(),
                    }))
                }
                Some(Mutation::AddOutput(adds)) => Some(ProstMutation::Add(AddMutation {
                    mutations: adds
                        .iter()
                        .map(|(&(actor_id, dispatcher_id), actors)| DispatcherMutation {
                            actor_id,
                            dispatcher_id,
                            info: actors.clone(),
                        })
                        .collect(),
                })),
            },
            span: vec![],
        }
    }

    pub fn from_protobuf(prost: &ProstBarrier) -> Result<Self> {
        let mutation = match prost.get_mutation()? {
            ProstMutation::Nothing(_) => (None),
            ProstMutation::Stop(stop) => {
                Some(Mutation::Stop(HashSet::from_iter(stop.get_actors().clone())).into())
            }
            ProstMutation::Update(update) => Some(
                Mutation::UpdateOutputs(
                    update
                        .mutations
                        .iter()
                        .map(|mutation| {
                            (
                                (mutation.actor_id, mutation.dispatcher_id),
                                mutation.get_info().clone(),
                            )
                        })
                        .collect::<HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>>(),
                )
                .into(),
            ),
            ProstMutation::Add(adds) => Some(
                Mutation::AddOutput(
                    adds.mutations
                        .iter()
                        .map(|mutation| {
                            (
                                (mutation.actor_id, mutation.dispatcher_id),
                                mutation.get_info().clone(),
                            )
                        })
                        .collect::<HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>>(),
                )
                .into(),
            ),
        };
        let epoch = prost.get_epoch().unwrap();
        Ok(Barrier {
            span: if ENABLE_BARRIER_AGGREGATION {
                trace_span!("barrier", epoch = ?epoch, mutation = ?mutation)
            } else {
                tracing::Span::none()
            },
            epoch: Epoch::new(epoch.curr, epoch.prev),
            mutation,
        })
    }
}

#[derive(Debug, EnumAsInner, PartialEq)]
pub enum Message {
    Chunk(StreamChunk),
    Barrier(Barrier),
}

impl<'a> TryFrom<&'a Message> for &'a Barrier {
    type Error = ();

    fn try_from(m: &'a Message) -> std::result::Result<Self, Self::Error> {
        match m {
            Message::Chunk(_) => Err(()),
            Message::Barrier(b) => Ok(b),
        }
    }
}

impl Message {
    /// Return true if the message is a stop barrier, meaning the stream
    /// will not continue, false otherwise.
    ///
    /// Note that this does not mean we will stop the current actor.
    #[cfg(test)]
    pub fn is_stop(&self) -> bool {
        matches!(
            self,
            Message::Barrier(Barrier {
                mutation,
                ..
            }) if mutation.as_ref().unwrap().is_stop()
        )
    }

    pub fn to_protobuf(&self) -> Result<ProstStreamMessage> {
        let prost = match self {
            Self::Chunk(stream_chunk) => {
                let prost_stream_chunk = stream_chunk.to_protobuf();
                StreamMessage::StreamChunk(prost_stream_chunk)
            }
            Self::Barrier(barrier) => StreamMessage::Barrier(barrier.clone().to_protobuf()),
        };
        let prost_stream_msg = ProstStreamMessage {
            stream_message: Some(prost),
        };
        Ok(prost_stream_msg)
    }

    pub fn from_protobuf(prost: &ProstStreamMessage) -> Result<Self> {
        let res = match prost.get_stream_message()? {
            StreamMessage::StreamChunk(ref stream_chunk) => {
                Message::Chunk(StreamChunk::from_protobuf(stream_chunk)?)
            }
            StreamMessage::Barrier(ref barrier) => {
                Message::Barrier(Barrier::from_protobuf(barrier)?)
            }
        };
        Ok(res)
    }
}

pub type PkIndices = Vec<usize>;
pub type PkIndicesRef<'a> = &'a [usize];
pub type PkDataTypes = SmallVec<[DataType; 1]>;

/// Get clones of inputs by given `pk_indices` from `columns`.
pub fn pk_input_arrays(pk_indices: PkIndicesRef, columns: &[Column]) -> Vec<ArrayRef> {
    pk_indices
        .iter()
        .map(|pk_idx| columns[*pk_idx].array())
        .collect()
}

/// Get references to inputs by given `pk_indices` from `columns`.
pub fn pk_input_array_refs<'a>(
    pk_indices: PkIndicesRef,
    columns: &'a [Column],
) -> Vec<&'a ArrayImpl> {
    pk_indices
        .iter()
        .map(|pk_idx| columns[*pk_idx].array_ref())
        .collect()
}

pub trait ExecutorBuilder {
    /// Create an executor.
    fn new_boxed_executor(
        executor_params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor>;
}

macro_rules! build_executor {
    ($source: expr,$node: expr,$store: expr,$stream: expr, $($proto_type_name:path => $data_type:ty),* $(,)?) => {
        match $node.get_node().unwrap() {
            $(
                $proto_type_name(..) => {
                    <$data_type>::new_boxed_executor($source,$node,$store,$stream)
                },
            )*
            _ => Err(RwError::from(
                ErrorCode::InternalError(format!(
                    "unsupported node:{:?}",
                    $node.get_node().unwrap()
                )),
            )),
        }
    }
}

pub fn create_executor(
    executor_params: ExecutorParams,
    stream: &mut LocalStreamManagerCore,
    node: &stream_plan::StreamNode,
    store: impl StateStore,
) -> Result<BoxedExecutor> {
    build_executor! {
        executor_params,
        node,
        store,
        stream,
        Node::SourceNode => SourceExecutorBuilder,
        Node::ProjectNode => ProjectExecutorBuilder,
        Node::TopNNode => TopNExecutorBuilder,
        Node::AppendOnlyTopNNode => AppendOnlyTopNExecutorBuilder,
        Node::LocalSimpleAggNode => LocalSimpleAggExecutorBuilder,
        Node::GlobalSimpleAggNode => SimpleAggExecutorBuilder,
        Node::HashAggNode => HashAggExecutorBuilder,
        Node::HashJoinNode => HashJoinExecutorBuilder,
        Node::HopWindowNode => HopWindowExecutorBuilder,
        Node::ChainNode => ChainExecutorBuilder,
        Node::BatchPlanNode => BatchQueryExecutorBuilder,
        Node::MergeNode => MergeExecutorBuilder,
        Node::MaterializeNode => MaterializeExecutorBuilder,
        Node::FilterNode => FilterExecutorBuilder,
        Node::ArrangeNode => ArrangeExecutorBuilder,
        Node::LookupNode => LookupExecutorBuilder,
        Node::UnionNode => UnionExecutorBuilder,
        Node::LookupUnionNode => LookupUnionExecutorBuilder,
    }
}

/// `StreamConsumer` is the last step in an actor.
pub trait StreamConsumer: Send + 'static {
    type BarrierStream: Stream<Item = Result<Barrier>> + Send;

    fn execute(self: Box<Self>) -> Self::BarrierStream;
}
