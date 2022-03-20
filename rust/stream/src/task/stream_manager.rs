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
//
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures::channel::mpsc::{channel, Receiver};
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::expr::{build_from_prost, AggKind, RowExpression};
use risingwave_common::try_match_expand;
use risingwave_common::types::DataType;
use risingwave_common::util::addr::is_local_address;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::plan::JoinType as JoinTypeProto;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::{expr, stream_plan, stream_service};
use risingwave_storage::{dispatch_state_store, Keyspace, StateStore, StateStoreImpl};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::ComputeClientPool;
use crate::executor::*;
use crate::task::{
    ActorId, ConsumableChannelPair, SharedContext, StreamEnvironment, UpDownActorIds,
    LOCAL_OUTPUT_CHANNEL_SIZE,
};

#[cfg(test)]
lazy_static::lazy_static! {
    pub static ref LOCAL_TEST_ADDR: SocketAddr = "127.0.0.1:2333".parse().unwrap();
}

pub type ActorHandle = JoinHandle<()>;

pub struct StreamManagerCore {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: HashMap<ActorId, ActorHandle>,

    pub(crate) context: Arc<SharedContext>,

    /// Stores all actor information.
    actor_infos: HashMap<ActorId, ActorInfo>,

    /// Stores all actor information, taken after actor built.
    actors: HashMap<ActorId, stream_plan::StreamActor>,

    /// Mock source, `actor_id = 0`.
    /// TODO: remove this
    mock_source: ConsumableChannelPair,

    /// The state store implement
    state_store: StateStoreImpl,

    /// Metrics of the stream manager
    streaming_metrics: Arc<StreamingMetrics>,

    /// The pool of compute clients.
    ///
    /// TODO: currently the client pool won't be cleared. Should remove compute clients when
    /// disconnected.
    compute_client_pool: ComputeClientPool,
}

/// `StreamManager` manages all stream executors in this project.
pub struct StreamManager {
    core: Mutex<StreamManagerCore>,
}

pub struct ExecutorParams {
    pub env: StreamEnvironment,

    /// Indices of primary keys
    pub pk_indices: PkIndices,

    /// Executor id, unique across all actors.
    pub executor_id: u64,

    /// Operator id, unique for each operator in fragment.
    pub operator_id: u64,

    /// Information of the operator from plan node.
    pub op_info: String,

    /// The input executor.
    pub input: Vec<Box<dyn Executor>>,

    /// Id of the actor.
    pub actor_id: ActorId,
    pub executor_stats: Arc<StreamingMetrics>,
}

impl Debug for ExecutorParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorParams")
            .field("env", &"...")
            .field("pk_indices", &self.pk_indices)
            .field("executor_id", &self.executor_id)
            .field("operator_id", &self.operator_id)
            .field("op_info", &self.op_info)
            .field("input", &self.input)
            .field("actor_id", &self.actor_id)
            .finish()
    }
}

impl StreamManager {
    fn with_core(core: StreamManagerCore) -> Self {
        Self {
            core: Mutex::new(core),
        }
    }

    pub fn new(
        addr: SocketAddr,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self::with_core(StreamManagerCore::new(addr, state_store, streaming_metrics))
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::with_core(StreamManagerCore::for_test())
    }

    /// Broadcast a barrier to all senders. Returns a receiver which will get notified when this
    /// barrier is finished.
    fn send_barrier(
        &self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> Result<oneshot::Receiver<()>> {
        let core = self.core.lock().unwrap();
        let mut barrier_manager = core.context.lock_barrier_manager();
        let rx = barrier_manager
            .send_barrier(barrier, actor_ids_to_send, actor_ids_to_collect)?
            .expect("no rx for local mode");
        Ok(rx)
    }

    /// Broadcast a barrier to all senders. Returns when the barrier is fully collected.
    pub async fn send_and_collect_barrier(
        &self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> Result<()> {
        let rx = self.send_barrier(barrier, actor_ids_to_send, actor_ids_to_collect)?;

        // Wait for all actors finishing this barrier.
        rx.await.unwrap();

        // Sync states from shared buffer to S3 before telling meta service we've done.
        dispatch_state_store!(self.state_store(), store, {
            match store.sync(Some(barrier.epoch.prev)).await {
                Ok(_) => {}
                // TODO: Handle sync failure by propagating it
                // back to global barrier manager
                Err(e) => panic!(
                    "Failed to sync state store after receving barrier {:?} due to {}",
                    barrier, e
                ),
            }
        });

        Ok(())
    }

    /// Broadcast a barrier to all senders. Returns immediately, and caller won't be notified when
    /// this barrier is finished.
    #[cfg(test)]
    pub fn send_barrier_for_test(&self, barrier: &Barrier) -> Result<()> {
        use std::iter::empty;

        let core = self.core.lock().unwrap();
        let mut barrier_manager = core.context.lock_barrier_manager();
        assert!(barrier_manager.is_local_mode());
        barrier_manager.send_barrier(barrier, empty(), empty())?;
        Ok(())
    }

    pub fn drop_actor(&self, actors: &[ActorId]) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        for id in actors {
            core.drop_actor(*id);
        }
        debug!("drop actors: {:?}", actors);
        Ok(())
    }

    pub async fn drop_materialized_view(
        &self,
        _table_id: &TableId,
        _env: StreamEnvironment,
    ) -> Result<()> {
        // TODO(august): the data in StateStore should also be dropped directly/through unpin or
        // some other way.
        Ok(())
    }

    pub fn take_receiver(&self, ids: UpDownActorIds) -> Result<Receiver<Message>> {
        let core = self.core.lock().unwrap();
        core.context.take_receiver(&ids)
    }

    pub fn update_actors(
        &self,
        actors: &[stream_plan::StreamActor],
        hanging_channels: &[stream_service::HangingChannel],
    ) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.update_actors(actors, hanging_channels)
    }

    /// This function was called while [`StreamManager`] exited.
    pub async fn wait_all(self) -> Result<()> {
        let handles = self.core.lock().unwrap().take_all_handles()?;
        for (_id, handle) in handles {
            handle.await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn wait_actors(&self, actor_ids: &[ActorId]) -> Result<()> {
        let handles = self.core.lock().unwrap().remove_actor_handles(actor_ids)?;
        for handle in handles {
            handle.await.unwrap();
        }
        Ok(())
    }

    /// This function could only be called once during the lifecycle of `StreamManager` for now.
    pub fn update_actor_info(
        &self,
        req: stream_service::BroadcastActorInfoTableRequest,
    ) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.update_actor_info(req)
    }

    /// This function could only be called once during the lifecycle of `StreamManager` for now.
    pub fn build_actors(&self, actors: &[ActorId], env: StreamEnvironment) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.build_actors(actors, env)
    }

    #[cfg(test)]
    pub fn take_source(&self) -> futures::channel::mpsc::Sender<Message> {
        let mut core = self.core.lock().unwrap();
        core.mock_source.0.take().unwrap()
    }

    #[cfg(test)]
    pub fn take_sink(&self, ids: UpDownActorIds) -> Receiver<Message> {
        let core = self.core.lock().unwrap();
        core.context.take_receiver(&ids).unwrap()
    }

    pub fn state_store(&self) -> StateStoreImpl {
        self.core.lock().unwrap().state_store.clone()
    }
}

pub fn build_agg_call_from_prost(agg_call_proto: &expr::AggCall) -> Result<AggCall> {
    let args = {
        let args = &agg_call_proto.get_args()[..];
        match args {
            [] => AggArgs::None,
            [arg] => AggArgs::Unary(
                DataType::from(arg.get_type()?),
                arg.get_input()?.column_idx as usize,
            ),
            _ => {
                return Err(RwError::from(ErrorCode::NotImplementedError(
                    "multiple aggregation args".to_string(),
                )))
            }
        }
    };
    Ok(AggCall {
        kind: AggKind::try_from(agg_call_proto.get_type()?)?,
        args,
        return_type: DataType::from(agg_call_proto.get_return_type()?),
    })
}

fn update_upstreams(context: &SharedContext, ids: &[UpDownActorIds]) {
    ids.iter()
        .map(|id| {
            let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
            context.add_channel_pairs(*id, (Some(tx), Some(rx)));
        })
        .count();
}

impl StreamManagerCore {
    fn new(
        addr: SocketAddr,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let context = SharedContext::new(addr, state_store.clone());
        Self::with_store_and_context(state_store, context, streaming_metrics)
    }

    fn with_store_and_context(
        state_store: StateStoreImpl,
        context: SharedContext,
        streaming_metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);

        Self {
            handles: HashMap::new(),
            context: Arc::new(context),
            actor_infos: HashMap::new(),
            actors: HashMap::new(),
            mock_source: (Some(tx), Some(rx)),
            state_store,
            streaming_metrics,
            compute_client_pool: ComputeClientPool::new(1024),
        }
    }

    #[cfg(test)]
    fn for_test() -> Self {
        use risingwave_storage::monitor::StateStoreMetrics;

        let register = prometheus::Registry::new();
        let streaming_metrics = Arc::new(StreamingMetrics::new(register));
        Self::with_store_and_context(
            StateStoreImpl::shared_in_memory_store(Arc::new(StateStoreMetrics::unused())),
            SharedContext::for_test(),
            streaming_metrics,
        )
    }

    fn get_actor_info(&self, actor_id: &ActorId) -> Result<&ActorInfo> {
        self.actor_infos.get(actor_id).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(
                "actor not found in info table".into(),
            ))
        })
    }

    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &mut self,
        input: Box<dyn Executor>,
        dispatcher: &stream_plan::Dispatcher,
        actor_id: ActorId,
    ) -> Result<Box<dyn StreamConsumer>> {
        // create downstream receivers
        let outputs = dispatcher
            .downstream_actor_id
            .iter()
            .map(|down_id| {
                let host_addr = self.get_actor_info(down_id)?.get_host()?;
                let downstream_addr = host_addr.to_socket_addr()?;
                new_output(&self.context, downstream_addr, actor_id, down_id)
            })
            .collect::<Result<Vec<_>>>()?;

        use stream_plan::DispatcherType::*;
        let dispatcher: Box<dyn StreamConsumer> = match dispatcher.get_type()? {
            Hash => {
                assert!(!outputs.is_empty());
                let column_indices = dispatcher
                    .column_indices
                    .iter()
                    .map(|i| *i as usize)
                    .collect();
                let hash_mapping = dispatcher
                    .hash_mapping
                    .clone()
                    .ok_or_else(|| {
                        RwError::from(ErrorCode::InternalError(
                            "hash dispatcher doesn't have consistent hash mapping".to_string(),
                        ))
                    })?
                    .hash_mapping;
                Box::new(DispatchExecutor::new(
                    input,
                    HashDataDispatcher::new(
                        dispatcher.downstream_actor_id.to_vec(),
                        outputs,
                        column_indices,
                        hash_mapping,
                    ),
                    actor_id,
                    self.context.clone(),
                ))
            }
            Broadcast => Box::new(DispatchExecutor::new(
                input,
                BroadcastDispatcher::new(outputs),
                actor_id,
                self.context.clone(),
            )),
            Simple => {
                assert_eq!(outputs.len(), 1);
                let output = outputs.into_iter().next().unwrap();
                Box::new(DispatchExecutor::new(
                    input,
                    SimpleDispatcher::new(output),
                    actor_id,
                    self.context.clone(),
                ))
            }
            Invalid => unreachable!(),
        };
        Ok(dispatcher)
    }

    /// Create a chain(tree) of nodes, with given `store`.
    fn create_nodes_inner(
        &mut self,
        fragment_id: u32,
        actor_id: ActorId,
        node: &stream_plan::StreamNode,
        input_pos: usize,
        env: StreamEnvironment,
        store: impl StateStore,
    ) -> Result<Box<dyn Executor>> {
        let op_info = node.get_identity().clone();
        // Create the input executor before creating itself
        // The node with no input must be a `MergeNode`
        let input: Vec<Box<dyn Executor>> = node
            .input
            .iter()
            .enumerate()
            .map(|(input_pos, input)| {
                self.create_nodes_inner(
                    fragment_id,
                    actor_id,
                    input,
                    input_pos,
                    env.clone(),
                    store.clone(),
                )
            })
            .try_collect()?;

        let pk_indices = node
            .get_pk_indices()
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();

        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        let executor_id = ((actor_id as u64) << 32) + node.get_operator_id();
        let operator_id = ((fragment_id as u64) << 32) + node.get_operator_id();

        let executor_params = ExecutorParams {
            env: env.clone(),
            pk_indices,
            executor_id,
            operator_id,
            op_info,
            input,
            actor_id,
            executor_stats: self.streaming_metrics.clone(),
        };
        let executor = create_executor(executor_params, self, node, store);
        let executor = Self::wrap_executor_for_debug(
            executor?,
            actor_id,
            input_pos,
            self.streaming_metrics.clone(),
        )?;
        Ok(executor)
    }

    /// Create a chain(tree) of nodes and return the head executor.
    fn create_nodes(
        &mut self,
        fragment_id: u32,
        actor_id: ActorId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
    ) -> Result<Box<dyn Executor>> {
        dispatch_state_store!(self.state_store.clone(), store, {
            self.create_nodes_inner(fragment_id, actor_id, node, 0, env, store)
        })
    }

    fn wrap_executor_for_debug(
        mut executor: Box<dyn Executor>,
        actor_id: ActorId,
        input_pos: usize,
        streaming_metrics: Arc<StreamingMetrics>,
    ) -> Result<Box<dyn Executor>> {
        if !cfg!(debug_assertions) {
            return Ok(executor);
        }
        let identity = executor.identity().to_string();

        // Trace
        executor = Box::new(TraceExecutor::new(
            executor,
            identity,
            input_pos,
            actor_id,
            streaming_metrics,
        ));
        // Schema check
        executor = Box::new(SchemaCheckExecutor::new(executor));
        // Epoch check
        executor = Box::new(EpochCheckExecutor::new(executor));
        // Cache clear
        if std::env::var(CACHE_CLEAR_ENABLED_ENV_VAR_KEY).is_ok() {
            executor = Box::new(CacheClearExecutor::new(executor));
        }

        Ok(executor)
    }

    pub(crate) fn create_hash_join_node(
        &mut self,
        mut params: ExecutorParams,
        node: &stream_plan::HashJoinNode,
        store: impl StateStore,
    ) -> Result<Box<dyn Executor>> {
        let source_r = params.input.remove(1);
        let source_l = params.input.remove(0);
        let params_l = JoinParams::new(
            node.get_left_key()
                .iter()
                .map(|key| *key as usize)
                .collect::<Vec<_>>(),
        );
        let params_r = JoinParams::new(
            node.get_right_key()
                .iter()
                .map(|key| *key as usize)
                .collect::<Vec<_>>(),
        );

        let condition = match node.get_condition() {
            Ok(cond_prost) => Some(RowExpression::new(build_from_prost(cond_prost)?)),
            Err(_) => None,
        };
        trace!("Join non-equi condition: {:?}", condition);

        macro_rules! impl_create_hash_join_executor {
            ($( { $join_type_proto:ident, $join_type:ident } ),*) => {
                |typ| match typ {
                    $( JoinTypeProto::$join_type_proto => Box::new(HashJoinExecutor::<_, { JoinType::$join_type }>::new(
                        source_l,
                        source_r,
                        params_l,
                        params_r,
                        params.pk_indices,
                        Keyspace::shared_executor_root(store.clone(), params.operator_id),
                        params.executor_id,
                        condition,
                        params.op_info,
                    )) as Box<dyn Executor>, )*
                    _ => todo!("Join type {:?} not implemented", typ),
                }
            }
        }

        macro_rules! for_all_join_types {
            ($macro:tt) => {
                $macro! {
                    { Inner, Inner },
                    { LeftOuter, LeftOuter },
                    { RightOuter, RightOuter },
                    { FullOuter, FullOuter }
                }
            };
        }
        let create_hash_join_executor = for_all_join_types! { impl_create_hash_join_executor };
        let join_type_proto = node.get_join_type()?;
        let executor = create_hash_join_executor(join_type_proto);
        Ok(executor)
    }

    pub fn create_merge_node(
        &mut self,
        params: ExecutorParams,
        node: &stream_plan::MergeNode,
    ) -> Result<Box<dyn Executor>> {
        let upstreams = node.get_upstream_actor_id();
        let fields = node.fields.iter().map(Field::from).collect();
        let schema = Schema::new(fields);
        let mut rxs = self.get_receive_message(params.actor_id, upstreams)?;

        if upstreams.len() == 1 {
            Ok(Box::new(ReceiverExecutor::new(
                schema,
                params.pk_indices,
                rxs.remove(0),
                params.op_info,
            )))
        } else {
            Ok(Box::new(MergeExecutor::new(
                schema,
                params.pk_indices,
                params.actor_id,
                rxs,
                params.op_info,
            )))
        }
    }

    pub(crate) fn get_receive_message(
        &mut self,
        actor_id: ActorId,
        upstreams: &[ActorId],
    ) -> Result<Vec<Receiver<Message>>> {
        assert!(!upstreams.is_empty());

        let rxs = upstreams
            .iter()
            .map(|up_id| {
                if *up_id == 0 {
                    Ok(self.mock_source.1.take().unwrap())
                } else {
                    let upstream_addr = self.get_actor_info(up_id)?.get_host()?;
                    let upstream_socket_addr = upstream_addr.to_socket_addr()?;
                    if !is_local_address(&upstream_socket_addr, &self.context.addr) {
                        // Get the sender for `RemoteInput` to forward received messages to
                        // receivers in `ReceiverExecutor` or
                        // `MergerExecutor`.
                        let sender = self.context.take_sender(&(*up_id, actor_id))?;
                        // spawn the `RemoteInput`
                        let up_id = *up_id;

                        let pool = self.compute_client_pool.clone();

                        tokio::spawn(async move {
                            let init_client = async move {
                                let remote_input = RemoteInput::create(
                                    pool.get_client_for_addr(&upstream_socket_addr).await?,
                                    (up_id, actor_id),
                                    sender,
                                )
                                .await?;
                                Ok::<_, RwError>(remote_input)
                            };
                            match init_client.await {
                                Ok(mut remote_input) => remote_input.run().await,
                                Err(e) => {
                                    info!("Spawn remote input fails:{}", e);
                                }
                            }
                        });
                    }
                    Ok::<_, RwError>(self.context.take_receiver(&(*up_id, actor_id))?)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            rxs.len(),
            upstreams.len(),
            "upstreams are not fully available: {} registered while {} required, actor_id={}",
            rxs.len(),
            upstreams.len(),
            actor_id
        );

        Ok(rxs)
    }

    fn build_actors(&mut self, actors: &[ActorId], env: StreamEnvironment) -> Result<()> {
        for actor_id in actors {
            let actor_id = *actor_id;
            let actor = self.actors.remove(&actor_id).unwrap();
            let executor =
                self.create_nodes(actor.fragment_id, actor_id, actor.get_nodes()?, env.clone())?;

            let dispatchers = actor.get_dispatcher();
            assert_eq!(
                dispatchers.len(),
                1,
                "compute node currently only supports single dispatcher"
            );
            let dispatcher = self.create_dispatcher(executor, &dispatchers[0], actor_id)?;

            trace!("build actor: {:#?}", &dispatcher);

            let actor = Actor::new(dispatcher, actor_id, self.context.clone());
            self.handles.insert(
                actor_id,
                tokio::spawn(async move {
                    actor.run().await.unwrap(); // unwrap the actor result to panic on error
                }),
            );
        }

        Ok(())
    }

    pub fn take_all_handles(&mut self) -> Result<HashMap<ActorId, ActorHandle>> {
        Ok(std::mem::take(&mut self.handles))
    }

    pub fn remove_actor_handles(&mut self, actor_ids: &[ActorId]) -> Result<Vec<ActorHandle>> {
        actor_ids
            .iter()
            .map(|actor_id| {
                self.handles.remove(actor_id).ok_or_else(|| {
                    RwError::from(ErrorCode::InternalError(format!(
                        "No such actor with actor id:{}",
                        actor_id
                    )))
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn update_actor_info(
        &mut self,
        req: stream_service::BroadcastActorInfoTableRequest,
    ) -> Result<()> {
        for actor in req.get_info() {
            let ret = self.actor_infos.insert(actor.get_actor_id(), actor.clone());
            if ret.is_some() {
                return Err(ErrorCode::InternalError(format!(
                    "duplicated actor {}",
                    actor.get_actor_id()
                ))
                .into());
            }
        }
        Ok(())
    }

    /// `drop_actor` is invoked by the leader node via RPC once the stop barrier arrives at the
    /// sink. All the actors in the actors should stop themselves before this method is invoked.
    fn drop_actor(&mut self, actor_id: ActorId) {
        let handle = self.handles.remove(&actor_id).unwrap();
        self.context.retain(|&(up_id, _)| up_id != actor_id);

        self.actor_infos.remove(&actor_id);
        self.actors.remove(&actor_id);
        // Task should have already stopped when this method is invoked.
        handle.abort();
    }

    fn build_channel_for_chain_node(
        &self,
        actor_id: ActorId,
        stream_node: &stream_plan::StreamNode,
    ) -> Result<()> {
        if let Node::ChainNode(_) = stream_node.node.as_ref().unwrap() {
            // Create channel based on upstream actor id for [`ChainNode`], check if upstream
            // exists.
            let merge = try_match_expand!(
                stream_node.input.get(0).unwrap().node.as_ref().unwrap(),
                Node::MergeNode,
                "first input of chain node should be merge node"
            )?;
            for upstream_actor_id in &merge.upstream_actor_id {
                if !self.actor_infos.contains_key(upstream_actor_id) {
                    return Err(ErrorCode::InternalError(format!(
                        "chain upstream actor {} not exists",
                        upstream_actor_id
                    ))
                    .into());
                }
                let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                let up_down_ids = (*upstream_actor_id, actor_id);
                self.context
                    .add_channel_pairs(up_down_ids, (Some(tx), Some(rx)));
            }
        }
        for child in &stream_node.input {
            self.build_channel_for_chain_node(actor_id, child)?;
        }
        Ok(())
    }

    fn update_actors(
        &mut self,
        actors: &[stream_plan::StreamActor],
        hanging_channels: &[stream_service::HangingChannel],
    ) -> Result<()> {
        let local_actor_ids: HashSet<ActorId> = HashSet::from_iter(
            actors
                .iter()
                .map(|actor| actor.get_actor_id())
                .collect::<Vec<_>>()
                .into_iter(),
        );

        for actor in actors {
            let ret = self.actors.insert(actor.get_actor_id(), actor.clone());
            if ret.is_some() {
                return Err(ErrorCode::InternalError(format!(
                    "duplicated actor {}",
                    actor.get_actor_id()
                ))
                .into());
            }
        }

        for (current_id, actor) in &self.actors {
            self.build_channel_for_chain_node(*current_id, actor.nodes.as_ref().unwrap())?;

            // At this time, the graph might not be complete, so we do not check if downstream
            // has `current_id` as upstream.
            let down_id = actor
                .dispatcher
                .iter()
                .flat_map(|x| x.downstream_actor_id.iter())
                .map(|id| (*current_id, *id))
                .collect_vec();
            update_upstreams(&self.context, &down_id);

            // Add remote input channels.
            let mut up_id = vec![];
            for upstream_id in actor.get_upstream_actor_id() {
                if !local_actor_ids.contains(upstream_id) {
                    up_id.push((*upstream_id, *current_id));
                }
            }
            update_upstreams(&self.context, &up_id);
        }

        for hanging_channel in hanging_channels {
            match (&hanging_channel.upstream, &hanging_channel.downstream) {
                (
                    Some(up),
                    Some(ActorInfo {
                        actor_id: down_id,
                        host: None,
                    }),
                ) => {
                    let up_down_ids = (up.actor_id, *down_id);
                    let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                    self.context
                        .add_channel_pairs(up_down_ids, (Some(tx), Some(rx)));
                }
                (
                    Some(ActorInfo {
                        actor_id: up_id,
                        host: None,
                    }),
                    Some(down),
                ) => {
                    let up_down_ids = (*up_id, down.actor_id);
                    let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                    self.context
                        .add_channel_pairs(up_down_ids, (Some(tx), Some(rx)));
                }
                _ => {
                    return Err(ErrorCode::InternalError(format!(
                        "hanging channel should has exactly one remote side: {:?}",
                        hanging_channel,
                    ))
                    .into())
                }
            }
        }
        Ok(())
    }
}
