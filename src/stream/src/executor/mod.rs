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

use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use self::batch_query::*;
use self::chain::*;
use self::filter::*;
use self::global_simple_agg::*;
use self::hash_agg::*;
use self::local_simple_agg::*;
use self::merge::*;
use self::mview::*;
use self::project::*;
use self::top_n::*;
use self::top_n_appendonly::*;
use crate::executor_v2::{
    BoxedExecutor, HashJoinExecutorBuilder, HopWindowExecutorBuilder, LookupExecutorBuilder,
    LookupUnionExecutorBuilder, SourceExecutorBuilder, UnionExecutorBuilder,
};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

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
