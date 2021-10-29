use std::collections::HashMap;
use std::sync::Mutex;

use crate::catalog::TableId;
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{build_from_proto as build_expr_from_proto, AggKind};
use crate::storage::*;
use crate::stream_op::*;
use crate::types::build_from_prost as build_type_from_prost;
use futures::channel::mpsc::{channel, unbounded, Receiver, Sender, UnboundedSender};
use itertools::Itertools;
use pb_convert::FromProtobuf;
use risingwave_pb::expr;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_service;
use risingwave_pb::ToProto;
use std::convert::TryFrom;
use tokio::task::JoinHandle;

/// Default capacity of channel if two fragments are on the same node
pub const LOCAL_OUTPUT_CHANNEL_SIZE: usize = 16;

type ConsumableChannelPair = (Option<Sender<Message>>, Option<Receiver<Message>>);
type ConsumableChannelVecPair = (Vec<Sender<Message>>, Vec<Receiver<Message>>);

pub struct StreamManagerCore {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: Vec<JoinHandle<Result<()>>>,

    /// `channel_pool` store the senders and receivers for later `Processor`'s use. When `StreamManager`
    ///
    /// Each actor has several senders and several receivers. Senders and receivers are created during
    /// `update_fragment`. Upon `build_fragment`, all these channels will be taken out and built into
    /// the executors and outputs.
    channel_pool: HashMap<u32, ConsumableChannelVecPair>,

    /// Stores all actor information.
    actors: HashMap<u32, stream_service::ActorInfo>,

    /// Stores all fragment information.
    fragments: HashMap<u32, stream_plan::StreamFragment>,

    sender_placeholder: Vec<UnboundedSender<Message>>,
    /// Mock source, `fragment_id = 0`.
    /// TODO: remove this
    mock_source: ConsumableChannelPair,
}

/// `StreamManager` manages all stream executors in this project.
pub struct StreamManager {
    core: Mutex<StreamManagerCore>,
}

impl StreamManager {
    pub fn new() -> Self {
        StreamManager {
            core: Mutex::new(StreamManagerCore::new()),
        }
    }

    pub fn update_fragment(&self, fragments: &[stream_plan::StreamFragment]) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.update_fragment(fragments)
    }

    pub async fn wait_all(&self) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.wait_all().await
    }

    /// This function could only be called once during the lifecycle of `StreamManager` for now.
    pub fn update_actor_info(&self, table: stream_service::ActorInfoTable) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.update_actor_info(table)
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
    pub fn take_sink(&self, id: u32) -> Receiver<Message> {
        let mut core = self.core.lock().unwrap();
        core.channel_pool.get_mut(&id).unwrap().1.remove(0)
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
    fn new() -> Self {
        let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
        Self {
            handles: vec![],
            channel_pool: HashMap::new(),
            actors: HashMap::new(),
            fragments: HashMap::new(),
            sender_placeholder: vec![],
            mock_source: (Some(tx), Some(rx)),
        }
    }

    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &mut self,
        input: Box<dyn Executor>,
        dispatcher: &stream_plan::Dispatcher,
        fragment_id: u32,
        downstreams: &[u32],
    ) -> Box<dyn StreamConsumer> {
        // create downstream receivers
        let outputs = self
            .channel_pool
            .get_mut(&fragment_id)
            .map(|x| std::mem::take(&mut x.0))
            .unwrap_or_default()
            .into_iter()
            .map(|tx| Box::new(ChannelOutput::new(tx)) as Box<dyn Output>)
            .collect::<Vec<_>>();

        assert_eq!(downstreams.len(), outputs.len());

        use stream_plan::dispatcher::DispatcherType::*;
        match dispatcher.get_type() {
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
        }
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
                    let stream_receiver = table.create_stream()?;
                    // TODO: The channel pair should be created by the Checkpoint manger. So this line may be removed later.
                    let (_sender, barrier_receiver) = unbounded();
                    // TODO: Take the ownership to avoid drop of this channel. This should be removed too.
                    self.sender_placeholder.push(_sender);

                    Ok(Box::new(TableSourceExecutor::new(
                        table_id,
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
                    .map(|select| {
                        build_expr_from_proto(
                            &select.to_proto::<risingwave_proto::expr::ExprNode>(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Box::new(ProjectExecutor::new(input, project_exprs)))
            }
            FilterNode(filter_node) => {
                let search_condition = build_expr_from_proto(
                    &filter_node
                        .get_search_condition()
                        .to_proto::<risingwave_proto::expr::ExprNode>(),
                )?;
                Ok(Box::new(FilterExecutor::new(input, search_condition)))
            }
            SimpleAggNode(aggr_node) => {
                let agg_calls: Vec<AggCall> = aggr_node
                    .get_agg_calls()
                    .iter()
                    .map(|agg_call| build_agg_call_from_prost(agg_call))
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
                    .map(|agg_call| build_agg_call_from_prost(agg_call))
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

    fn create_merger(&mut self, fragment_id: u32, upstreams: &[u32]) -> Result<Box<dyn Executor>> {
        assert!(!upstreams.is_empty());

        let mut rxs = self
            .channel_pool
            .get_mut(&fragment_id)
            .map(|x| std::mem::take(&mut x.1))
            .unwrap_or_default();

        for upstream in upstreams {
            // TODO: remove this
            if *upstream == 0 {
                // `fragment_id = 0` is used as mock input
                rxs.push(self.mock_source.1.take().unwrap());
                continue;
            }

            let actor = self
                .actors
                .get(upstream)
                .expect("upstream actor not found in info table");
            // FIXME: use `is_local_address` from `ExchangeExecutor`.
            if actor.get_host().get_host() == "127.0.0.1" {
                continue;
            } else {
                todo!("remote node is not supported in streaming engine");
                // TODO: create gRPC connection
            }
        }

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
            Ok(Box::new(ReceiverExecutor::new(rxs.remove(0))))
        } else {
            Ok(Box::new(MergeExecutor::new(rxs)))
        }
    }

    fn build_fragment(&mut self, fragments: &[u32], table_manager: TableManagerRef) -> Result<()> {
        for fragment_id in fragments {
            let fragment = self.fragments.remove(fragment_id).unwrap();

            let merger = self.create_merger(*fragment_id, fragment.get_upstream_fragment_id())?;

            let executor =
                self.create_nodes(fragment.get_nodes(), merger, table_manager.clone())?;

            let dispatcher = self.create_dispatcher(
                executor,
                fragment.get_dispatcher(),
                *fragment_id,
                fragment.get_downstream_fragment_id(),
            );

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

    fn update_actor_info(&mut self, table: stream_service::ActorInfoTable) -> Result<()> {
        for actor in table.get_info() {
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
            for downstream in fragment.get_downstream_fragment_id() {
                // At this time, the graph might not be complete, so we do not check if downstream has `current_id`
                // as upstream.
                let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                let current_channels = self.channel_pool.entry(*current_id).or_default();
                current_channels.0.push(tx);
                let downstream_channels = self.channel_pool.entry(*downstream).or_default();
                downstream_channels.1.push(rx);
            }
        }
        Ok(())
    }
}

impl Default for StreamManager {
    fn default() -> Self {
        Self::new()
    }
}
