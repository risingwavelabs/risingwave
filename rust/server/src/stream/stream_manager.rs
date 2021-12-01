use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

use async_std::net::SocketAddr;
use futures::channel::mpsc::{channel, unbounded, Receiver, Sender, UnboundedSender};
use itertools::Itertools;
use tokio::task::JoinHandle;

use pb_convert::FromProtobuf;
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::expr::{build_from_prost as build_expr_from_prost, AggKind};
use risingwave_common::types::build_from_prost as build_type_from_prost;
use risingwave_common::util::addr::{get_host_port, is_local_address};
use risingwave_common::util::sort_util::fetch_orders;
use risingwave_pb::expr;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::table_source_node::SourceType;
use risingwave_pb::stream_service;
use risingwave_pb::ToProto;

use crate::source::Source;
use crate::source::*;
use crate::storage::hummock::{HummockOptions, HummockStorage};
use crate::storage::*;
use crate::stream_op::*;
use crate::task::GlobalTaskEnv;

/// Default capacity of channel if two fragments are on the same node
pub const LOCAL_OUTPUT_CHANNEL_SIZE: usize = 16;

type ConsumableChannelPair = (Option<Sender<Message>>, Option<Receiver<Message>>);
type ConsumableChannelVecPair = (Vec<Sender<Message>>, Vec<Receiver<Message>>);
pub type UpDownFragmentIds = (u32, u32);

pub struct StreamManagerCore {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: HashMap<u32, JoinHandle<Result<()>>>,

    /// Stores the senders and receivers for later `Processor`'s use.
    ///
    /// Each actor has several senders and several receivers. Senders and receivers are created
    /// during `update_fragment`. Upon `build_fragment`, all these channels will be taken out and
    /// built into the executors and outputs.
    /// One sender or one receiver can be uniquely determined by the upstream and downstream
    /// fragment id.
    ///
    /// There are three cases that we need local channels to pass around messages:
    /// 1. pass `Message` between two local fragments
    /// 2. The RPC client at the downstream fragment forwards received `Message` to one channel in
    /// `ReceiverExecutor` or `MergerExecutor`.
    /// 3. The RPC `Output` at the upstream fragment forwards received `Message` to
    /// `ExchangeServiceImpl`.    The channel servers as a buffer because `ExchangeServiceImpl` is
    /// on the server-side and we will    also introduce backpressure.
    channel_pool: HashMap<UpDownFragmentIds, ConsumableChannelPair>,

    /// The receiver is on the other side of rpc `Output`. The `ExchangeServiceImpl` take it
    /// when it receives request for streaming data from downstream clients.
    receivers_for_exchange_service: HashMap<UpDownFragmentIds, Receiver<Message>>,

    /// Stores all actor information.
    actors: HashMap<u32, stream_service::ActorInfo>,

    /// Stores all fragment information.
    fragments: HashMap<u32, stream_plan::StreamFragment>,

    sender_placeholder: Vec<UnboundedSender<Message>>,

    /// Mock source, `fragment_id = 0`.
    /// TODO: remove this
    mock_source: ConsumableChannelPair,

    /// Stores the local address
    ///
    /// It is used to test whether an actor is local or not,
    /// thus determining whether we should setup channel or rpc connection between
    /// two actors/fragments.
    addr: SocketAddr,

    /// The state store of Hummuck
    state_store: HummockStorage,
}

/// `StreamManager` manages all stream executors in this project.
pub struct StreamManager {
    core: Mutex<StreamManagerCore>,
}

impl StreamManager {
    pub fn new(addr: SocketAddr) -> Self {
        StreamManager {
            core: Mutex::new(StreamManagerCore::new(addr)),
        }
    }

    // TODO: We will refine this method when the meta service is ready.
    pub fn send_barrier(&self, epoch: u64) {
        let core = self.core.lock().unwrap();
        core.send_barrier(epoch);
    }

    // TODO: We will refine this method when the meta service is ready.
    pub fn send_stop_barrier(&self) {
        let core = self.core.lock().unwrap();
        core.send_stop_barrier()
    }

    pub fn drop_fragment(&self, fragments: &[u32]) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        for id in fragments {
            core.drop_fragment(*id);
        }
        Ok(())
    }

    pub fn take_receiver(&self, ids: UpDownFragmentIds) -> Result<Receiver<Message>> {
        let mut core = self.core.lock().unwrap();
        core.receivers_for_exchange_service
            .remove(&ids)
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "No receivers for rpc output from {} to {}",
                    ids.0, ids.1
                )))
            })
    }

    pub fn update_fragment(&self, fragments: &[stream_plan::StreamFragment]) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.update_fragment(fragments)
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
    pub fn build_fragment(&self, fragments: &[u32], env: GlobalTaskEnv) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.build_fragment(fragments, env)
    }

    #[cfg(test)]
    pub fn take_source(&self) -> Sender<Message> {
        let mut core = self.core.lock().unwrap();
        core.mock_source.0.take().unwrap()
    }

    #[cfg(test)]
    pub fn take_sink(&self, ids: UpDownFragmentIds) -> Receiver<Message> {
        let mut core = self.core.lock().unwrap();
        core.channel_pool.get_mut(&ids).unwrap().1.take().unwrap()
    }
}

fn build_agg_call_from_prost(agg_call_proto: &expr::AggCall) -> Result<AggCall> {
    let args = {
        let args = &agg_call_proto.get_args()[..];
        match args {
            [] => AggArgs::None,
            [arg] => AggArgs::Unary(
                build_type_from_prost(arg.get_type())?,
                arg.get_input().column_idx as usize,
            ),
            _ => {
                return Err(RwError::from(ErrorCode::NotImplementedError(
                    "multiple aggregation args".to_string(),
                )))
            }
        }
    };
    Ok(AggCall {
        kind: AggKind::try_from(agg_call_proto.get_type())?,
        args,
        return_type: build_type_from_prost(agg_call_proto.get_return_type())?,
    })
}

impl StreamManagerCore {
    fn new(addr: SocketAddr) -> Self {
        let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
        Self {
            handles: HashMap::new(),
            channel_pool: HashMap::new(),
            receivers_for_exchange_service: HashMap::new(),
            actors: HashMap::new(),
            fragments: HashMap::new(),
            sender_placeholder: vec![],
            mock_source: (Some(tx), Some(rx)),
            addr,
            state_store: HummockStorage::new(
                Arc::new(InMemObjectStore::new()),
                HummockOptions::default(),
            ),
        }
    }

    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &mut self,
        input: Box<dyn Executor>,
        dispatcher: &stream_plan::Dispatcher,
        fragment_id: u32,
        downstreams: &[u32],
    ) -> Result<Box<dyn StreamConsumer>> {
        // create downstream receivers
        let outputs = downstreams
            .iter()
            .map(|down_id| {
                let up_down_ids = (fragment_id, *down_id);
                let host_addr = self
                    .actors
                    .get(down_id)
                    .ok_or_else(|| {
                        RwError::from(ErrorCode::InternalError(format!(
                            "channel between {} and {} does not exist",
                            fragment_id, down_id
                        )))
                    })?
                    .get_host();
                let downstream_addr = format!("{}:{}", host_addr.get_host(), host_addr.get_port());
                if is_local_address(&get_host_port(&downstream_addr)?, &self.addr) {
                    // if this is a local downstream fragment
                    let tx = self
                        .channel_pool
                        .get_mut(&(fragment_id, *down_id))
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "channel between {} and {} does not exist",
                                fragment_id, down_id
                            )))
                        })?
                        .0
                        .take()
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "sender from {} to {} does no exist",
                                fragment_id, down_id
                            )))
                        })?;
                    Ok(Box::new(ChannelOutput::new(tx)) as Box<dyn Output>)
                } else {
                    // This channel is used for `RpcOutput` and `ExchangeServiceImpl`.
                    let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                    // later, `ExchangeServiceImpl` comes to get it
                    self.receivers_for_exchange_service.insert(up_down_ids, rx);
                    Ok(Box::new(RemoteOutput::new(tx)) as Box<dyn Output>)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(downstreams.len(), outputs.len());

        use stream_plan::dispatcher::DispatcherType::*;
        let dispatcher: Box<dyn StreamConsumer> = match dispatcher.get_type() {
            RoundRobin => {
                assert!(!outputs.is_empty());
                Box::new(DispatchExecutor::new(
                    input,
                    RoundRobinDataDispatcher::new(outputs),
                ))
            }
            Hash => {
                assert!(!outputs.is_empty());
                Box::new(DispatchExecutor::new(
                    input,
                    HashDataDispatcher::new(outputs, vec![dispatcher.get_column_idx() as usize]),
                ))
            }
            Broadcast => {
                assert!(!outputs.is_empty());
                Box::new(DispatchExecutor::new(
                    input,
                    BroadcastDispatcher::new(outputs),
                ))
            }
            Simple => {
                assert_eq!(outputs.len(), 1);
                let output = outputs.into_iter().next().unwrap();
                Box::new(DispatchExecutor::new(input, SimpleDispatcher::new(output)))
            }
            Blackhole => Box::new(DispatchExecutor::new(input, BlackHoleDispatcher::new())),
        };
        Ok(dispatcher)
    }

    /// broadcast a barrier to all senders with specific epoch.
    // TODO: We will refine this method when the meta service is ready.
    fn send_barrier(&self, epoch: u64) {
        for sender in &self.sender_placeholder {
            sender
                .unbounded_send(Message::Barrier(Barrier { epoch, stop: false }))
                .unwrap();
        }
    }

    // TODO: We will refine this method when the meta service is ready.
    fn send_stop_barrier(&self) {
        for sender in &self.sender_placeholder {
            sender
                .unbounded_send(Message::Barrier(Barrier {
                    epoch: 0,
                    stop: true,
                }))
                .unwrap();
        }
    }

    /// Create a chain(tree) of nodes and return the head executor.
    fn create_nodes(
        &mut self,
        node: &stream_plan::StreamNode,
        mergers: Vec<Box<dyn Executor>>,
        env: GlobalTaskEnv,
    ) -> Result<Box<dyn Executor>> {
        use stream_plan::stream_node::Node::*;

        // Create the input executor before creating itself
        // Note(eric): Maybe we should put a `Merger` executor in proto
        let mut input: Vec<Box<dyn Executor>> = {
            if !node.input.is_empty() {
                assert_eq!(node.input.len(), mergers.len());
                node.input
                    .iter()
                    .zip(mergers.into_iter())
                    .map(|(input, merger)| self.create_nodes(input, vec![merger], env.clone()))
                    .collect::<Result<Vec<_>>>()?
            } else {
                mergers
            }
        };

        let source_manager = env.source_manager();

        let executor: Result<Box<dyn Executor>> = match node.get_node() {
            TableSourceNode(table_source_node) => {
                let table_id = TableId::from_protobuf(
                    &table_source_node
                        .get_table_ref_id()
                        .to_proto::<risingwave_proto::plan::TableRefId>(),
                )
                .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

                let source_desc = source_manager.get_source(&table_id)?;

                // TODO: The channel pair should be created by the Checkpoint manger. So this line
                // may be removed later.
                let (sender, barrier_receiver) = unbounded();
                self.sender_placeholder.push(sender);

                match table_source_node.get_source_type() {
                    SourceType::Table => {
                        let column_ids = table_source_node.get_column_ids().to_vec();
                        let source = source_desc.source;

                        let mut fields = Vec::with_capacity(column_ids.len());
                        for &column_id in column_ids.iter() {
                            let column_desc = source_desc
                                .columns
                                .iter()
                                .find(|c| c.column_id == column_id)
                                .unwrap();
                            fields.push(Field::new(column_desc.data_type.clone()));
                        }
                        let schema = Schema::new(fields);

                        if let SourceImpl::Table(ref table) = *source {
                            let stream_reader =
                                table.stream_reader(TableReaderContext {}, column_ids)?;

                            Ok(Box::new(TableSourceExecutor::new(
                                table_id,
                                schema,
                                stream_reader,
                                barrier_receiver,
                            )))
                        } else {
                            Err(RwError::from(InternalError(
                                "Streaming source only supports table source".to_string(),
                            )))
                        }
                    }
                    SourceType::Stream => Ok(Box::new(StreamSourceExecutor::new(
                        source_desc,
                        barrier_receiver,
                    )?)),
                }
            }
            ProjectNode(project_node) => {
                let project_exprs = project_node
                    .get_select_list()
                    .iter()
                    .map(build_expr_from_prost)
                    .collect::<Result<Vec<_>>>()?;
                Ok(Box::new(ProjectExecutor::new(
                    input.remove(0),
                    project_exprs,
                )))
            }
            FilterNode(filter_node) => {
                let search_condition = build_expr_from_prost(filter_node.get_search_condition())?;
                Ok(Box::new(FilterExecutor::new(
                    input.remove(0),
                    search_condition,
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
                )?))
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

                let schema = generate_hash_agg_schema(&*input[0], &agg_calls, &keys);

                Ok(Box::new(HashAggExecutor::new(
                    input.remove(0),
                    agg_calls.clone(),
                    keys,
                    InMemoryKeyedState::new(
                        RowSerializer::new(schema.clone()),
                        AggStateSerializer::new(agg_calls),
                    ),
                    schema,
                )))
            }
            AppendOnlyTopNNode(top_n_node) => {
                let column_orders = &top_n_node.column_orders;
                let order_paris = fetch_orders(column_orders)?;
                let limit = if top_n_node.limit == 0 {
                    None
                } else {
                    Some(top_n_node.limit as usize)
                };
                Ok(Box::new(AppendOnlyTopNExecutor::new(
                    input.remove(0),
                    Arc::new(order_paris),
                    limit,
                    top_n_node.offset as usize,
                )))
            }
            TopNNode(top_n_node) => {
                let column_orders = &top_n_node.column_orders;
                let order_paris = fetch_orders(column_orders)?;
                let limit = if top_n_node.limit == 0 {
                    None
                } else {
                    Some(top_n_node.limit as usize)
                };
                Ok(Box::new(TopNExecutor::new(
                    input.remove(0),
                    Arc::new(order_paris),
                    limit,
                    top_n_node.offset as usize,
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

                Ok(Box::new(HashJoinExecutor::<{ JoinType::Inner }>::new(
                    source_l, source_r, params_l, params_r,
                )))
            }
            MviewNode(materialized_view_node) => {
                let columns = materialized_view_node.get_column_descs();
                let pks = materialized_view_node
                    .pk_indices
                    .iter()
                    .map(|key| *key as usize)
                    .collect::<Vec<_>>();

                let column_orders = materialized_view_node.get_column_orders();
                let order_pairs = fetch_orders(column_orders).unwrap();
                let pk_schema = Schema::try_from(
                    &pks.iter()
                        .map(|col_idx| columns[*col_idx].clone())
                        .collect::<Vec<_>>(),
                )?;
                let schema = Schema::try_from(columns)?;

                let executor = Box::new(MViewSinkExecutor::new(
                    input.remove(0),
                    schema.clone(),
                    InMemoryKeyedState::new(
                        RowSerializer::new(pk_schema),
                        RowSerializer::new(schema),
                    ),
                    pks,
                    Arc::new(order_pairs),
                ));
                Ok(executor)
            }
        };

        executor
    }

    fn create_merger(
        &mut self,
        fragment_id: u32,
        schema: Schema,
        upstreams: &[u32],
    ) -> Result<Box<dyn Executor>> {
        assert!(!upstreams.is_empty());

        let mut rxs = upstreams
            .iter()
            .map(|up_id| {
                if *up_id == 0 {
                    Ok(self.mock_source.1.take().unwrap())
                } else {
                    let host_addr = self
                        .actors
                        .get(up_id)
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(
                                "upstream actor not found in info table".into(),
                            ))
                        })?
                        .get_host();
                    let upstream_addr =
                        format!("{}:{}", host_addr.get_host(), host_addr.get_port());
                    let upstream_socket_addr = get_host_port(&upstream_addr)?;
                    if !is_local_address(&upstream_socket_addr, &self.addr) {
                        // Get the sender for `RemoteInput` to forward received messages to
                        // receivers in `ReceiverExecutor` or
                        // `MergerExecutor`.
                        let sender = self
                            .channel_pool
                            .get_mut(&(*up_id, fragment_id))
                            .ok_or_else(|| {
                                RwError::from(ErrorCode::InternalError(format!(
                                    "channel between {} and {} does not exist",
                                    up_id, fragment_id
                                )))
                            })?
                            .0
                            .take()
                            .ok_or_else(|| {
                                RwError::from(ErrorCode::InternalError(format!(
                                    "sender from {} to {} does no exist",
                                    up_id, fragment_id
                                )))
                            })?;
                        // spawn the `RemoteInput`
                        let up_id = *up_id;
                        tokio::spawn(async move {
                            let remote_input_res = RemoteInput::create(
                                upstream_socket_addr,
                                (up_id, fragment_id),
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
                    Ok(self
                        .channel_pool
                        .get_mut(&(*up_id, fragment_id))
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "channel between {} and {} does not exist",
                                up_id, fragment_id
                            )))
                        })?
                        .1
                        .take()
                        .ok_or_else(|| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "receiver from {} to {} does no exist",
                                up_id, fragment_id
                            )))
                        })?)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            rxs.len(),
            upstreams.len(),
            "upstreams are not fully available: {} registered while {} required, fragment_id={}",
            rxs.len(),
            upstreams.len(),
            fragment_id
        );

        if upstreams.len() == 1 {
            // Only one upstream, use `ReceiverExecutor`.
            Ok(Box::new(ReceiverExecutor::new(schema, rxs.remove(0))))
        } else {
            Ok(Box::new(MergeExecutor::new(schema, rxs)))
        }
    }

    fn build_fragment(&mut self, fragments: &[u32], env: GlobalTaskEnv) -> Result<()> {
        for fragment_id in fragments {
            let fragment = self.fragments.remove(fragment_id).unwrap();

            let schema = Schema::try_from(fragment.get_input_column_descs())?;

            let mergers: Vec<Box<dyn Executor>> = fragment
                .mergers
                .iter()
                .filter_map(|merger| {
                    let upstream_ids = merger.get_upstream_fragment_id();
                    if upstream_ids.is_empty() {
                        None
                    } else {
                        Some(self.create_merger(*fragment_id, schema.clone(), upstream_ids))
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            let executor = self.create_nodes(fragment.get_nodes(), mergers, env.clone())?;
            let dispatcher = self.create_dispatcher(
                executor,
                fragment.get_dispatcher(),
                *fragment_id,
                fragment.get_downstream_fragment_id(),
            )?;

            let actor = Actor::new(dispatcher);
            self.handles.insert(*fragment_id, tokio::spawn(actor.run()));
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
            let ret = self.actors.insert(actor.get_fragment_id(), actor.clone());
            if ret.is_some() {
                return Err(ErrorCode::InternalError(format!(
                    "duplicated actor {}",
                    actor.get_fragment_id()
                ))
                .into());
            }
        }
        Ok(())
    }

    /// `drop_fragment` is invoked by the leader node via RPC once the stop barrier arrives at the
    ///  sink. All the actors in the fragments should stop themselves before this method is invoked.
    fn drop_fragment(&mut self, fragment_id: u32) {
        let handle = self.handles.remove(&fragment_id).unwrap();
        self.channel_pool.retain(|(x, _), _| *x != fragment_id);
        self.receivers_for_exchange_service
            .retain(|(x, _), _| *x != fragment_id);

        self.actors.remove(&fragment_id);
        self.fragments.remove(&fragment_id);
        // Task should have already stopped when this method is invoked.
        handle.abort();
    }

    fn update_fragment(&mut self, fragments: &[stream_plan::StreamFragment]) -> Result<()> {
        for fragment in fragments {
            let ret = self
                .fragments
                .insert(fragment.get_fragment_id(), fragment.clone());
            if ret.is_some() {
                return Err(ErrorCode::InternalError(format!(
                    "duplicated fragment {}",
                    fragment.get_fragment_id()
                ))
                .into());
            }
        }

        for (current_id, fragment) in &self.fragments {
            for downstream_id in fragment.get_downstream_fragment_id() {
                // At this time, the graph might not be complete, so we do not check if downstream
                // has `current_id` as upstream.
                let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                let up_down_ids = (*current_id, *downstream_id);
                self.channel_pool.insert(up_down_ids, (Some(tx), Some(rx)));
            }
        }
        Ok(())
    }
}
