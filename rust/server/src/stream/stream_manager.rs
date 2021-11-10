use std::collections::HashMap;
use std::sync::Mutex;

use crate::storage::*;
use crate::stream_op::*;
use async_std::net::SocketAddr;
use futures::channel::mpsc::{channel, unbounded, Receiver, Sender, UnboundedSender};
use itertools::Itertools;
use pb_convert::FromProtobuf;
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::expr::{build_from_prost as build_expr_from_prost, AggKind};
use risingwave_common::types::build_from_prost as build_type_from_prost;
use risingwave_common::util::addr::{get_host_port, is_local_address};
use risingwave_pb::expr;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_service;
use risingwave_pb::ToProst;
use risingwave_pb::ToProto;
use std::convert::TryFrom;
use tokio::task::JoinHandle;

/// Default capacity of channel if two fragments are on the same node
pub const LOCAL_OUTPUT_CHANNEL_SIZE: usize = 16;

type ConsumableChannelPair = (Option<Sender<Message>>, Option<Receiver<Message>>);
type ConsumableChannelVecPair = (Vec<Sender<Message>>, Vec<Receiver<Message>>);
pub type UpDownFragmentIds = (u32, u32);

pub struct StreamManagerCore {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: Vec<JoinHandle<Result<()>>>,

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
    #[allow(must_not_suspend)]
    pub async fn wait_all(&self) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.wait_all().await
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
    pub fn build_fragment(&self, fragments: &[u32], table_manager: TableManagerRef) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.build_fragment(fragments, table_manager)
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
            handles: vec![],
            channel_pool: HashMap::new(),
            receivers_for_exchange_service: HashMap::new(),
            actors: HashMap::new(),
            fragments: HashMap::new(),
            sender_placeholder: vec![],
            mock_source: (Some(tx), Some(rx)),
            addr,
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

    /// Create a chain of nodes and return the head executor.
    fn create_nodes(
        &mut self,
        node: &stream_plan::StreamNode,
        merger: Box<dyn Executor>,
        table_manager: TableManagerRef,
    ) -> Result<Box<dyn Executor>> {
        use stream_plan::stream_node::Node::*;

        // Create the input executor before creating itself
        // Note(eric): Maybe we should put a `Merger` executor in proto
        let input = {
            if node.input.is_some() {
                self.create_nodes(node.get_input(), merger, table_manager.clone())?
            } else {
                merger
            }
        };

        let executor: Result<Box<dyn Executor>> = match node.get_node() {
            TableSourceNode(table_source_node) => {
                let table_id = TableId::from_protobuf(
                    &table_source_node
                        .get_table_ref_id()
                        .to_proto::<risingwave_proto::plan::TableRefId>(),
                )
                .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

                let table_ref = table_manager.clone().get_table(&table_id).unwrap();
                if let SimpleTableRef::Columnar(table) = table_ref {
                    let schema = Schema::try_from(
                        &table.columns().iter().map(ToProst::to_prost).collect_vec(),
                    )?;
                    let stream_receiver = table.create_stream()?;
                    // TODO: The channel pair should be created by the Checkpoint manger. So this
                    // line may be removed later.
                    let (_sender, barrier_receiver) = unbounded();
                    // TODO: Take the ownership to avoid drop of this channel. This should be
                    // removed too.
                    self.sender_placeholder.push(_sender);
                    Ok(Box::new(TableSourceExecutor::new(
                        table_id,
                        schema,
                        stream_receiver,
                        barrier_receiver,
                    )))
                } else {
                    Err(RwError::from(InternalError(
                        "Streaming source only supports columnar table".to_string(),
                    )))
                }
            }
            ProjectNode(project_node) => {
                let project_exprs = project_node
                    .get_select_list()
                    .iter()
                    .map(build_expr_from_prost)
                    .collect::<Result<Vec<_>>>()?;
                Ok(Box::new(ProjectExecutor::new(input, project_exprs)))
            }
            FilterNode(filter_node) => {
                let search_condition = build_expr_from_prost(filter_node.get_search_condition())?;
                Ok(Box::new(FilterExecutor::new(input, search_condition)))
            }
            SimpleAggNode(aggr_node) => {
                let agg_calls: Vec<AggCall> = aggr_node
                    .get_agg_calls()
                    .iter()
                    .map(build_agg_call_from_prost)
                    .try_collect()?;
                Ok(Box::new(SimpleAggExecutor::new(input, agg_calls)?))
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

                Ok(Box::new(HashAggExecutor::new(input, agg_calls, keys)))
            }
            MviewNode(materialized_view_node) => {
                let table_id = TableId::from_protobuf(
                    &materialized_view_node
                        .get_table_ref_id()
                        .to_proto::<risingwave_proto::plan::TableRefId>(),
                )
                .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

                let columns = materialized_view_node
                    .get_column_descs()
                    .iter()
                    .map(|column_desc| column_desc.to_proto::<risingwave_proto::plan::ColumnDesc>())
                    .collect();
                let pks = materialized_view_node
                    .pk_indices
                    .iter()
                    .map(|key| *key as usize)
                    .collect::<Vec<_>>();
                table_manager.create_materialized_view(&table_id, columns, pks.clone())?;
                let table_ref = table_manager.get_table(&table_id).unwrap();
                if let SimpleTableRef::Row(table) = table_ref {
                    let executor = Box::new(MViewSinkExecutor::new(input, table, pks));
                    Ok(executor)
                } else {
                    Err(RwError::from(InternalError(
                        "Materialized view creation internal error".to_string(),
                    )))
                }
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

    fn build_fragment(&mut self, fragments: &[u32], table_manager: TableManagerRef) -> Result<()> {
        for fragment_id in fragments {
            let fragment = self.fragments.remove(fragment_id).unwrap();

            let schema = Schema::try_from(fragment.get_input_column_descs())?;

            let merger =
                self.create_merger(*fragment_id, schema, fragment.get_upstream_fragment_id())?;

            let executor =
                self.create_nodes(fragment.get_nodes(), merger, table_manager.clone())?;

            let dispatcher = self.create_dispatcher(
                executor,
                fragment.get_dispatcher(),
                *fragment_id,
                fragment.get_downstream_fragment_id(),
            )?;

            let actor = Actor::new(dispatcher);
            tokio::spawn(actor.run());
        }

        Ok(())
    }

    pub async fn wait_all(&mut self) -> Result<()> {
        for handle in std::mem::take(&mut self.handles) {
            handle.await.unwrap()?;
        }
        Ok(())
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
