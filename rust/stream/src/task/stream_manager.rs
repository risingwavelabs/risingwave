use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use futures::channel::mpsc::{channel, Receiver, Sender};
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::expr::{build_from_prost as build_expr_from_prost, AggKind, RowExpression};
use risingwave_common::types::DataTypeKind;
use risingwave_common::util::addr::is_local_address;
use risingwave_common::util::sort_util::{
    build_from_prost as build_order_type_from_prost, fetch_orders,
};
use risingwave_pb::common::ActorInfo;
use risingwave_pb::plan::JoinType as JoinTypeProto;
use risingwave_pb::{expr, stream_plan, stream_service};
use risingwave_storage::{dispatch_state_store, Keyspace, StateStore, StateStoreImpl};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::executor::snapshot::BatchQueryExecutor;
use crate::executor::*;
use crate::task::{LocalBarrierManager, StreamTaskEnv};

/// Default capacity of channel if two actors are on the same node
pub const LOCAL_OUTPUT_CHANNEL_SIZE: usize = 16;

pub type ConsumableChannelPair = (Option<Sender<Message>>, Option<Receiver<Message>>);
pub type ConsumableChannelVecPair = (Vec<Sender<Message>>, Vec<Receiver<Message>>);
pub type UpDownActorIds = (u32, u32);

#[cfg(test)]
lazy_static::lazy_static! {
    pub static ref LOCAL_TEST_ADDR: SocketAddr = "127.0.0.1:2333".parse().unwrap();
}

/// Stores the data which may be modified from the data plane.
pub struct SharedContext {
    /// Stores the senders and receivers for later `Processor`'s use.
    ///
    /// Each actor has several senders and several receivers. Senders and receivers are created
    /// during `update_actors`. Upon `build_actorss`, all these channels will be taken out and
    /// built into the executors and outputs.
    /// One sender or one receiver can be uniquely determined by the upstream and downstream actor
    /// id.
    ///
    /// There are three cases that we need local channels to pass around messages:
    /// 1. pass `Message` between two local actors
    /// 2. The RPC client at the downstream actor forwards received `Message` to one channel in
    /// `ReceiverExecutor` or `MergerExecutor`.
    /// 3. The RPC `Output` at the upstream actor forwards received `Message` to
    /// `ExchangeServiceImpl`.        The channel servers as a buffer because `ExchangeServiceImpl`
    /// is on the server-side and we will        also introduce backpressure.
    pub(crate) channel_pool: Mutex<HashMap<UpDownActorIds, ConsumableChannelPair>>,

    /// The receiver is on the other side of rpc `Output`. The `ExchangeServiceImpl` take it
    /// when it receives request for streaming data from downstream clients.
    pub(crate) receivers_for_exchange_service: Mutex<HashMap<UpDownActorIds, Receiver<Message>>>,

    /// Stores the local address
    ///
    /// It is used to test whether an actor is local or not,
    /// thus determining whether we should setup channel or rpc connection between
    /// two actors/actors.
    pub(crate) addr: SocketAddr,

    pub(crate) barrier_manager: Mutex<LocalBarrierManager>,
}

impl SharedContext {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            channel_pool: Mutex::new(HashMap::new()),
            receivers_for_exchange_service: Mutex::new(HashMap::new()),
            addr,
            barrier_manager: Mutex::new(LocalBarrierManager::new()),
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            channel_pool: Mutex::new(HashMap::new()),
            receivers_for_exchange_service: Mutex::new(HashMap::new()),
            addr: *LOCAL_TEST_ADDR,
            barrier_manager: Mutex::new(LocalBarrierManager::for_test()),
        }
    }

    #[inline]
    pub fn lock_channel_pool(&self) -> MutexGuard<HashMap<UpDownActorIds, ConsumableChannelPair>> {
        self.channel_pool.lock().unwrap()
    }

    pub fn lock_barrier_manager(&self) -> MutexGuard<LocalBarrierManager> {
        self.barrier_manager.lock().unwrap()
    }

    #[inline]
    pub fn lock_receivers_for_exchange_service(
        &self,
    ) -> MutexGuard<HashMap<UpDownActorIds, Receiver<Message>>> {
        self.receivers_for_exchange_service.lock().unwrap()
    }
}

pub struct StreamManagerCore {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: HashMap<u32, JoinHandle<Result<()>>>,

    context: Arc<SharedContext>,

    /// Stores all actor information.
    actor_infos: HashMap<u32, ActorInfo>,

    /// Stores all actor information.
    actors: HashMap<u32, stream_plan::StreamActor>,

    /// Mock source, `actor_id = 0`.
    /// TODO: remove this
    mock_source: ConsumableChannelPair,

    /// The state store of Hummuck
    state_store: StateStoreImpl,
}

/// `StreamManager` manages all stream executors in this project.
pub struct StreamManager {
    core: Mutex<StreamManagerCore>,
}

impl StreamManager {
    fn with_core(core: StreamManagerCore) -> Self {
        Self {
            core: Mutex::new(core),
        }
    }

    pub fn new(addr: SocketAddr, state_store: StateStoreImpl) -> Self {
        Self::with_core(StreamManagerCore::new(addr, state_store))
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::with_core(StreamManagerCore::for_test())
    }

    /// Broadcast a barrier to all senders. Returns a receiver which will get notified when this
    /// barrier is finished.
    pub fn send_barrier(
        &self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = u32>,
        actor_ids_to_collect: impl IntoIterator<Item = u32>,
    ) -> Result<oneshot::Receiver<()>> {
        let core = self.core.lock().unwrap();
        let mut barrier_manager = core.context.lock_barrier_manager();
        let rx = barrier_manager
            .send_barrier(barrier, actor_ids_to_send, actor_ids_to_collect)?
            .expect("no rx for local mode");
        Ok(rx)
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

    pub fn drop_actor(&self, actors: &[u32]) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        for id in actors {
            core.drop_actor(*id);
        }
        Ok(())
    }

    pub async fn drop_materialized_view(
        &self,
        table_id: &TableId,
        env: StreamTaskEnv,
    ) -> Result<()> {
        let table_manager = env.table_manager();
        table_manager.drop_materialized_view(table_id).await
    }

    pub fn take_receiver(&self, ids: UpDownActorIds) -> Result<Receiver<Message>> {
        let core = self.core.lock().unwrap();
        let mut guard = core.context.lock_receivers_for_exchange_service();
        guard.remove(&ids).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(format!(
                "No receivers for rpc output from {} to {}",
                ids.0, ids.1
            )))
        })
    }

    pub fn update_actors(&self, actors: &[stream_plan::StreamActor]) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.update_actors(actors)
    }

    /// This function was called while [`StreamManager`] exited.
    ///
    /// Suspend was allowed here.
    pub async fn wait_all(self) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.wait_all().await
    }

    #[cfg(test)]
    pub async fn wait_actors(&self, actor_ids: &[u32]) -> Result<()> {
        let handles = self
            .core
            .lock()
            .unwrap()
            .remove_actor_handles(actor_ids)
            .await?;
        for handle in handles {
            handle.await.unwrap()?
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
    pub fn build_actors(&self, actors: &[u32], env: StreamTaskEnv) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.build_actors(actors, env)
    }

    #[cfg(test)]
    pub fn take_source(&self) -> Sender<Message> {
        let mut core = self.core.lock().unwrap();
        core.mock_source.0.take().unwrap()
    }

    #[cfg(test)]
    pub fn take_sink(&self, ids: UpDownActorIds) -> Receiver<Message> {
        let core = self.core.lock().unwrap();
        let mut guard = core.context.lock_channel_pool();
        guard.get_mut(&ids).unwrap().1.take().unwrap()
    }
}

fn build_agg_call_from_prost(agg_call_proto: &expr::AggCall) -> Result<AggCall> {
    let args = {
        let args = &agg_call_proto.get_args()[..];
        match args {
            [] => AggArgs::None,
            [arg] => AggArgs::Unary(
                DataTypeKind::from(arg.get_type()?),
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
        return_type: DataTypeKind::from(agg_call_proto.get_return_type()?),
    })
}

impl StreamManagerCore {
    fn new(addr: SocketAddr, state_store: StateStoreImpl) -> Self {
        Self::with_store_and_context(state_store, SharedContext::new(addr))
    }

    fn with_store_and_context(state_store: StateStoreImpl, context: SharedContext) -> Self {
        let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);

        Self {
            handles: HashMap::new(),
            context: Arc::new(context),
            actor_infos: HashMap::new(),
            actors: HashMap::new(),
            mock_source: (Some(tx), Some(rx)),
            state_store,
        }
    }

    #[cfg(test)]
    fn for_test() -> Self {
        Self::with_store_and_context(
            StateStoreImpl::shared_in_memory_store(),
            SharedContext::for_test(),
        )
    }

    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &mut self,
        input: Box<dyn Executor>,
        dispatcher: &stream_plan::Dispatcher,
        actor_id: u32,
        downstreams: &[u32],
    ) -> Result<Box<dyn StreamConsumer>> {
        // create downstream receivers
        let outputs = downstreams
            .iter()
            .map(|down_id| {
                let up_down_ids = (actor_id, *down_id);
                let host_addr = self
                    .actor_infos
                    .get(down_id)
                    .ok_or_else(|| {
                        RwError::from(ErrorCode::InternalError(format!(
                            "channel between {} and {} does not exist",
                            actor_id, down_id
                        )))
                    })?
                    .get_host()?;
                let downstream_addr = host_addr.to_socket_addr()?;
                if is_local_address(&downstream_addr, &self.context.addr) {
                    // if this is a local downstream actor
                    let mut guard = self.context.lock_channel_pool();
                    let tx = guard
                        .get_mut(&(actor_id, *down_id))
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "channel between {} and {} does not exist",
                                actor_id, down_id
                            )))
                        })?
                        .0
                        .take()
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "sender from {} to {} does no exist",
                                actor_id, down_id
                            )))
                        })?;
                    Ok(Box::new(ChannelOutput::new(tx)) as Box<dyn Output>)
                } else {
                    // This channel is used for `RpcOutput` and `ExchangeServiceImpl`.
                    let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                    // later, `ExchangeServiceImpl` comes to get it
                    let mut guard = self.context.lock_receivers_for_exchange_service();

                    guard.insert(up_down_ids, rx);
                    Ok(Box::new(RemoteOutput::new(tx)) as Box<dyn Output>)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(downstreams.len(), outputs.len());

        use stream_plan::dispatcher::DispatcherType::*;
        let dispatcher: Box<dyn StreamConsumer> = match dispatcher.get_type()? {
            Hash => {
                assert!(!outputs.is_empty());
                Box::new(DispatchExecutor::new(
                    input,
                    HashDataDispatcher::new(
                        downstreams.to_vec(),
                        outputs,
                        vec![dispatcher.get_column_idx() as usize],
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
        };
        Ok(dispatcher)
    }

    // TODO: all barriers should be triggered by meta service
    fn send_conf_change_barrier(&mut self, _mutation: Mutation) -> Result<()> {
        todo!("conf change barrier should be sent from meta, mv on mv is temporially not supported")
        // self
        //     .context
        //     .lock_barrier_manager()
        //     .send_barrier(&Barrier::new(0).with_mutation(mutation), vec![])?;
        // Ok(())
    }

    /// Create a chain(tree) of nodes, with given `store`.
    fn create_nodes_inner(
        &mut self,
        actor_id: u32,
        node: &stream_plan::StreamNode,
        input_pos: usize,
        env: StreamTaskEnv,
        store: impl StateStore,
    ) -> Result<Box<dyn Executor>> {
        use stream_plan::stream_node::Node::*;

        // Create the input executor before creating itself
        // The node with no input must be a `MergeNode`
        let mut input: Vec<Box<dyn Executor>> = node
            .input
            .iter()
            .enumerate()
            .map(|(input_pos, input)| {
                self.create_nodes_inner(actor_id, input, input_pos, env.clone(), store.clone())
            })
            .try_collect()?;

        let table_manager = env.table_manager();
        let source_manager = env.source_manager();

        let pk_indices = node
            .get_pk_indices()
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();

        // We assume that the node_id of different instances from the same RelNode will be the same.
        let executor_id = ((actor_id as u64) << 32) + node.get_node_id();
        let node_id = node.get_node_id().try_into().unwrap();

        let executor: Result<Box<dyn Executor>> = match node.get_node()? {
            SourceNode(node) => {
                let source_id = TableId::from(&node.table_ref_id);
                let source_desc = source_manager.get_source(&source_id)?;

                let (sender, barrier_receiver) = unbounded_channel();
                self.context
                    .lock_barrier_manager()
                    .register_sender(actor_id, sender);

                let column_ids = node.get_column_ids().to_vec();
                let mut fields = Vec::with_capacity(column_ids.len());
                for &column_id in &column_ids {
                    let column_desc = source_desc
                        .columns
                        .iter()
                        .find(|c| c.column_id == column_id)
                        .unwrap();
                    fields.push(Field::with_name(
                        column_desc.data_type,
                        column_desc.name.clone(),
                    ));
                }
                let schema = Schema::new(fields);

                Ok(Box::new(StreamSourceExecutor::new(
                    source_id,
                    source_desc,
                    column_ids,
                    schema,
                    pk_indices,
                    barrier_receiver,
                    executor_id,
                )?))
            }
            ProjectNode(project_node) => {
                let project_exprs = project_node
                    .get_select_list()
                    .iter()
                    .map(build_expr_from_prost)
                    .collect::<Result<Vec<_>>>()?;
                Ok(Box::new(ProjectExecutor::new(
                    input.remove(0),
                    pk_indices,
                    project_exprs,
                    executor_id,
                )))
            }
            FilterNode(filter_node) => {
                let search_condition = build_expr_from_prost(filter_node.get_search_condition()?)?;
                Ok(Box::new(FilterExecutor::new(
                    input.remove(0),
                    search_condition,
                    executor_id,
                )))
            }
            SimpleAggNode(aggr_node) => {
                let agg_calls: Vec<AggCall> = aggr_node
                    .get_agg_calls()
                    .iter()
                    .map(build_agg_call_from_prost)
                    .try_collect()?;

                Ok(Box::new(SimpleAggExecutor::new(
                    input.remove(0),
                    agg_calls,
                    Keyspace::executor_root(store.clone(), executor_id),
                    pk_indices,
                    executor_id,
                )))
            }
            HashAggNode(aggr_node) => {
                let keys = aggr_node
                    .get_group_keys()
                    .iter()
                    .map(|key| key.column_idx as usize)
                    .collect::<Vec<_>>();

                let agg_calls: Vec<AggCall> = aggr_node
                    .get_agg_calls()
                    .iter()
                    .map(build_agg_call_from_prost)
                    .try_collect()?;

                Ok(Box::new(HashAggExecutor::new(
                    input.remove(0),
                    agg_calls,
                    keys,
                    Keyspace::shared_executor_root(store.clone(), node_id),
                    pk_indices,
                    executor_id,
                )))
            }
            AppendOnlyTopNNode(top_n_node) => {
                let order_types = top_n_node
                    .get_order_types()
                    .iter()
                    .map(build_order_type_from_prost)
                    .collect::<Result<Vec<_>>>()?;
                assert_eq!(order_types.len(), pk_indices.len());
                let limit = if top_n_node.limit == 0 {
                    None
                } else {
                    Some(top_n_node.limit as usize)
                };
                let cache_size = Some(1024);
                let total_count = (0, 0);
                Ok(Box::new(AppendOnlyTopNExecutor::new(
                    input.remove(0),
                    order_types,
                    (top_n_node.offset as usize, limit),
                    pk_indices,
                    Keyspace::executor_root(store.clone(), executor_id),
                    cache_size,
                    total_count,
                    executor_id,
                )))
            }
            TopNNode(top_n_node) => {
                let order_types = top_n_node
                    .get_order_types()
                    .iter()
                    .map(build_order_type_from_prost)
                    .collect::<Result<Vec<_>>>()?;
                assert_eq!(order_types.len(), pk_indices.len());
                let limit = if top_n_node.limit == 0 {
                    None
                } else {
                    Some(top_n_node.limit as usize)
                };
                let cache_size = Some(1024);
                let total_count = (0, 0, 0);
                Ok(Box::new(TopNExecutor::new(
                    input.remove(0),
                    order_types,
                    (top_n_node.offset as usize, limit),
                    pk_indices,
                    Keyspace::executor_root(store.clone(), executor_id),
                    cache_size,
                    total_count,
                    executor_id,
                )))
            }
            HashJoinNode(hash_join_node) => {
                let source_r = input.remove(1);
                let source_l = input.remove(0);
                let params_l = JoinParams::new(
                    hash_join_node
                        .get_left_key()
                        .iter()
                        .map(|key| *key as usize)
                        .collect::<Vec<_>>(),
                );
                let params_r = JoinParams::new(
                    hash_join_node
                        .get_right_key()
                        .iter()
                        .map(|key| *key as usize)
                        .collect::<Vec<_>>(),
                );

                let condition = match hash_join_node.get_condition() {
                    Ok(cond_prost) => Some(RowExpression::new(build_expr_from_prost(cond_prost)?)),
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
                                pk_indices,
                                Keyspace::shared_executor_root(store.clone(), node_id),
                                executor_id,
                                condition,
                            )) as Box<dyn Executor>, )*
                            _ => todo!("Join type {:?} not inplemented", typ),
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
                let create_hash_join_executor =
                    for_all_join_types! { impl_create_hash_join_executor };
                let join_type_proto = hash_join_node.get_join_type()?;
                let executor = create_hash_join_executor(join_type_proto);
                Ok(executor)
            }
            MviewNode(materialized_view_node) => {
                let table_id = TableId::from(&materialized_view_node.table_ref_id);
                let columns = materialized_view_node.get_column_descs();
                let pks = materialized_view_node
                    .pk_indices
                    .iter()
                    .map(|key| *key as usize)
                    .collect::<Vec<_>>();

                let column_orders = materialized_view_node.get_column_orders();
                let order_pairs = fetch_orders(column_orders).unwrap();
                let orderings = order_pairs
                    .iter()
                    .map(|order| order.order_type)
                    .collect::<Vec<_>>();

                let keyspace = if materialized_view_node.associated_table_ref_id.is_some() {
                    // share the keyspace between mview and table v2
                    let associated_table_id =
                        TableId::from(&materialized_view_node.associated_table_ref_id);
                    Keyspace::table_root(store, &associated_table_id)
                } else {
                    Keyspace::table_root(store, &table_id)
                };

                let executor = Box::new(MaterializeExecutor::new(
                    input.remove(0),
                    keyspace,
                    Schema::try_from(columns)?,
                    pks,
                    orderings,
                    executor_id,
                ));
                Ok(executor)
            }
            MergeNode(merge_node) => {
                let schema = Schema::try_from(merge_node.get_input_column_descs())?;
                let upstreams = merge_node.get_upstream_actor_id();
                self.create_merge_node(actor_id, schema, upstreams, pk_indices)
            }
            ChainNode(chain_node) => {
                let table_id = TableId::from(&chain_node.table_ref_id);
                let table = table_manager.get_table(&table_id)?;
                let snapshot = Box::new(BatchQueryExecutor::new(table.clone(), pk_indices));
                let up_id = chain_node.upstream_actor_id;
                let rx = {
                    let mut guard = self.context.lock_channel_pool();
                    guard
                        .get_mut(&(up_id, actor_id))
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "chain node: channel between {} and {} does not exist",
                                up_id, actor_id
                            )))
                        })?
                        .1
                        .take()
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "chain node: receiver from {} to {} does no exist",
                                up_id, actor_id
                            )))
                        })?
                };
                let pk_indices = chain_node
                    .pk_indices
                    .iter()
                    .map(|x| *x as usize)
                    .collect_vec();
                let upstream_schema = table.schema().into_owned();
                let mview = Box::new(ReceiverExecutor::new(
                    upstream_schema.clone(),
                    pk_indices,
                    rx,
                ));

                // TODO(MrCroxx): ConfChange should be triggered by meta when it's done.
                self.send_conf_change_barrier(Mutation::AddOutput(
                    chain_node.upstream_actor_id,
                    vec![self.actor_infos.get(&actor_id).unwrap().to_owned()],
                ))?;
                // TODO(MrCroxx): Use column_descs to get idx after mv planner can generate stable
                // column_ids. Now simply treat column_id as column_idx.
                let column_idxs: Vec<usize> = chain_node
                    .column_ids
                    .iter()
                    .map(|id| *id as usize)
                    .collect();
                let schema = Schema::new(
                    column_idxs
                        .iter()
                        .map(|i| upstream_schema.fields()[*i].clone())
                        .collect_vec(),
                );
                Ok(Box::new(ChainExecutor::new(
                    snapshot,
                    mview,
                    schema,
                    column_idxs,
                )))
            }
            _ => Err(RwError::from(ErrorCode::InternalError(format!(
                "unsupported node:{:?}",
                node
            )))),
        };

        let executor = Self::wrap_executor_for_debug(executor?, actor_id, input_pos)?;
        Ok(executor)
    }

    /// Create a chain(tree) of nodes and return the head executor.
    fn create_nodes(
        &mut self,
        actor_id: u32,
        node: &stream_plan::StreamNode,
        env: StreamTaskEnv,
    ) -> Result<Box<dyn Executor>> {
        dispatch_state_store!(self.state_store.clone(), store, {
            self.create_nodes_inner(actor_id, node, 0, env, store)
        })
    }

    fn wrap_executor_for_debug(
        mut executor: Box<dyn Executor>,
        actor_id: u32,
        input_pos: usize,
    ) -> Result<Box<dyn Executor>> {
        if !cfg!(debug_assertions) {
            return Ok(executor);
        }
        let identity = executor.identity().to_string();

        // Trace
        executor = Box::new(TraceExecutor::new(executor, identity, input_pos, actor_id));
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

    fn create_merge_node(
        &mut self,
        actor_id: u32,
        schema: Schema,
        upstreams: &[u32],
        pk_indices: PkIndices,
    ) -> Result<Box<dyn Executor>> {
        assert!(!upstreams.is_empty());

        let mut rxs = upstreams
            .iter()
            .map(|up_id| {
                if *up_id == 0 {
                    Ok(self.mock_source.1.take().unwrap())
                } else {
                    let upstream_addr = self
                        .actor_infos
                        .get(up_id)
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(
                                "upstream actor not found in info table".into(),
                            ))
                        })?
                        .get_host()?;
                    let upstream_socket_addr = upstream_addr.to_socket_addr()?;
                    if !is_local_address(&upstream_socket_addr, &self.context.addr) {
                        // Get the sender for `RemoteInput` to forward received messages to
                        // receivers in `ReceiverExecutor` or
                        // `MergerExecutor`.
                        let mut guard = self.context.lock_channel_pool();
                        let sender = guard
                            .get_mut(&(*up_id, actor_id))
                            .ok_or_else(|| {
                                RwError::from(ErrorCode::InternalError(format!(
                                    "channel between {} and {} does not exist",
                                    up_id, actor_id
                                )))
                            })?
                            .0
                            .take()
                            .ok_or_else(|| {
                                RwError::from(ErrorCode::InternalError(format!(
                                    "sender from {} to {} does no exist",
                                    up_id, actor_id
                                )))
                            })?;
                        // spawn the `RemoteInput`
                        let up_id = *up_id;
                        tokio::spawn(async move {
                            let remote_input_res = RemoteInput::create(
                                upstream_socket_addr,
                                (up_id, actor_id),
                                sender,
                            )
                            .await;
                            match remote_input_res {
                                Ok(mut remote_input) => remote_input.run().await,
                                Err(e) => {
                                    info!("Spawn remote input fails:{}", e);
                                }
                            }
                        });
                    }
                    let mut guard = self.context.lock_channel_pool();
                    Ok(guard
                        .get_mut(&(*up_id, actor_id))
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "channel between {} and {} does not exist",
                                up_id, actor_id
                            )))
                        })?
                        .1
                        .take()
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "receiver from {} to {} does no exist",
                                up_id, actor_id
                            )))
                        })?)
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

        if upstreams.len() == 1 {
            // Only one upstream, use `ReceiverExecutor`.
            // FIXME: after merger is refactored in proto, put pk_indices into it.
            Ok(Box::new(ReceiverExecutor::new(
                schema,
                pk_indices,
                rxs.remove(0),
            )))
        } else {
            Ok(Box::new(MergeExecutor::new(
                schema, pk_indices, actor_id, rxs,
            )))
        }
    }

    fn build_actors(&mut self, actors: &[u32], env: StreamTaskEnv) -> Result<()> {
        for actor_id in actors {
            let actor_id = *actor_id;
            let actor = self.actors.remove(&actor_id).unwrap();
            let executor = self.create_nodes(actor_id, actor.get_nodes()?, env.clone())?;
            let dispatcher = self.create_dispatcher(
                executor,
                actor.get_dispatcher()?,
                actor_id,
                actor.get_downstream_actor_id(),
            )?;

            trace!("build actor: {:#?}", &dispatcher);

            let actor = Actor::new(dispatcher, actor_id, self.context.clone());
            self.handles.insert(actor_id, tokio::spawn(actor.run()));
        }

        Ok(())
    }

    pub async fn wait_all(&mut self) -> Result<()> {
        for (_sid, handle) in std::mem::take(&mut self.handles) {
            handle.await.unwrap()?;
        }
        Ok(())
    }

    pub async fn remove_actor_handles(
        &mut self,
        actor_ids: &[u32],
    ) -> Result<Vec<JoinHandle<Result<()>>>> {
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
    fn drop_actor(&mut self, actor_id: u32) {
        let handle = self.handles.remove(&actor_id).unwrap();
        let mut channel_pool_guard = self.context.lock_channel_pool();
        let mut exhange_guard = self.context.lock_receivers_for_exchange_service();
        channel_pool_guard.retain(|(x, _), _| *x != actor_id);
        exhange_guard.retain(|(x, _), _| *x != actor_id);

        self.actor_infos.remove(&actor_id);
        self.actors.remove(&actor_id);
        // Task should have already stopped when this method is invoked.
        handle.abort();
    }

    fn build_channel_for_chain_node(
        &self,
        actor_id: u32,
        stream_node: &stream_plan::StreamNode,
    ) -> Result<()> {
        if let stream_plan::stream_node::Node::ChainNode(chain) = stream_node.node.as_ref().unwrap()
        {
            // Create channel based on upstream actor id for [`ChainNode`], check if upstream
            // exists.
            if !self.actor_infos.contains_key(&chain.upstream_actor_id) {
                return Err(ErrorCode::InternalError(format!(
                    "chain upstream actor {} not exists",
                    chain.upstream_actor_id
                ))
                .into());
            }
            let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
            let up_down_ids = (chain.upstream_actor_id, actor_id);
            let mut guard = self.context.lock_channel_pool();
            guard.insert(up_down_ids, (Some(tx), Some(rx)));
        }
        for child in &stream_node.input {
            self.build_channel_for_chain_node(actor_id, child)?;
        }
        Ok(())
    }

    fn update_actors(&mut self, actors: &[stream_plan::StreamActor]) -> Result<()> {
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

            for downstream_id in actor.get_downstream_actor_id() {
                // At this time, the graph might not be complete, so we do not check if downstream
                // has `current_id` as upstream.
                let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                let up_down_ids = (*current_id, *downstream_id);
                let mut guard = self.context.lock_channel_pool();
                guard.insert(up_down_ids, (Some(tx), Some(rx)));
            }
        }
        Ok(())
    }
}
