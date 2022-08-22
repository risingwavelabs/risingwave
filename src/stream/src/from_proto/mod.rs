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

//! Build executor from protobuf.

mod agg_call;
mod batch_query;
mod chain;
mod dynamic_filter;
mod expand;
mod filter;
mod global_simple_agg;
mod group_top_n;
mod hash_agg;
mod hash_join;
mod hop_window;
mod local_simple_agg;
mod lookup;
mod lookup_union;
mod merge;
mod mview;
mod project;
mod project_set;
mod sink;
mod source;
mod top_n;
mod top_n_appendonly;
mod union;

// import for submodules
use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_storage::{Keyspace, StateStore};

use self::batch_query::*;
use self::chain::*;
use self::dynamic_filter::*;
use self::expand::*;
use self::filter::*;
use self::global_simple_agg::*;
use self::group_top_n::GroupTopNExecutorBuilder;
use self::hash_agg::*;
use self::hash_join::*;
use self::hop_window::*;
use self::local_simple_agg::*;
use self::lookup::*;
use self::lookup_union::*;
use self::merge::*;
use self::mview::*;
use self::project::*;
use self::project_set::*;
use self::sink::*;
use self::source::*;
use self::top_n::*;
use self::top_n_appendonly::*;
use self::union::*;
use crate::executor::{BoxedExecutor, Executor, ExecutorInfo};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

trait ExecutorBuilder {
    /// Create a [`BoxedExecutor`] from [`StreamNode`].
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor>;
}

macro_rules! build_executor {
    ($source:expr, $node:expr, $store:expr, $stream:expr, $($proto_type_name:path => $data_type:ty),* $(,)?) => {
        match $node.get_node_body().unwrap() {
            $(
                $proto_type_name(..) => {
                    <$data_type>::new_boxed_executor($source, $node, $store, $stream)
                },
            )*
            NodeBody::Exchange(_) | NodeBody::DeltaIndexJoin(_) => unreachable!()
        }
    }
}

/// Create an executor from protobuf [`StreamNode`].
pub fn create_executor(
    params: ExecutorParams,
    stream: &mut LocalStreamManagerCore,
    node: &StreamNode,
    store: impl StateStore,
) -> Result<BoxedExecutor> {
    build_executor! {
        params,
        node,
        store,
        stream,
        NodeBody::Source => SourceExecutorBuilder,
        NodeBody::Sink => SinkExecutorBuilder,
        NodeBody::Project => ProjectExecutorBuilder,
        NodeBody::TopN => TopNExecutorNewBuilder,
        NodeBody::AppendOnlyTopN => AppendOnlyTopNExecutorBuilder,
        NodeBody::LocalSimpleAgg => LocalSimpleAggExecutorBuilder,
        NodeBody::GlobalSimpleAgg => GlobalSimpleAggExecutorBuilder,
        NodeBody::HashAgg => HashAggExecutorBuilder,
        NodeBody::HashJoin => HashJoinExecutorBuilder,
        NodeBody::HopWindow => HopWindowExecutorBuilder,
        NodeBody::Chain => ChainExecutorBuilder,
        NodeBody::BatchPlan => BatchQueryExecutorBuilder,
        NodeBody::Merge => MergeExecutorBuilder,
        NodeBody::Materialize => MaterializeExecutorBuilder,
        NodeBody::Filter => FilterExecutorBuilder,
        NodeBody::Arrange => ArrangeExecutorBuilder,
        NodeBody::Lookup => LookupExecutorBuilder,
        NodeBody::Union => UnionExecutorBuilder,
        NodeBody::LookupUnion => LookupUnionExecutorBuilder,
        NodeBody::Expand => ExpandExecutorBuilder,
        NodeBody::DynamicFilter => DynamicFilterExecutorBuilder,
        NodeBody::ProjectSet => ProjectSetExecutorBuilder,
        NodeBody::GroupTopN => GroupTopNExecutorBuilder,
    }
}
