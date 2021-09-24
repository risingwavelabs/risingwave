use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::{ErrorCode, Result};
use crate::expr::build_from_proto;
use crate::protobuf::Message as _;
use crate::stream_op::*;
use futures::channel::mpsc::{channel, Receiver, Sender};
use risingwave_proto::stream_plan;
use risingwave_proto::stream_service;
use tokio::task::JoinHandle;

/// Default capacity of channel if two fragments are on the same node
pub const LOCAL_OUTPUT_CHANNEL_SIZE: usize = 16;

type ConsumableChannelPair = (Option<Sender<Message>>, Option<Receiver<Message>>);

pub struct StreamManagerCore {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: Vec<JoinHandle<Result<()>>>,

    /// `channels` store the senders and receivers for later `Processor`'s use. When `StreamManager`
    ///
    /// When `create_nodes` is called,
    /// 1. The manager will take receivers out and create `Processor`.
    /// 2. The manager will add downstream receivers.
    ///
    /// For example, when we create `1 -> 2, 3 -> 4`,
    /// * Client should first send creating `1` request. The manager will add `2 => rx` and `3 => rx` to `receivers`.
    /// * Then the client send creating `2`, and the manager will take `2 => rx` to create processor, and add `4 => rx`.
    /// * Then the client send creating `3`, and the manager will take `3 => rx` to create processor, and add `4 => rx`.
    /// * Finally the client send creating `4`, and the manager will take `4 => [rx, rx]` to create new processor.
    receivers: HashMap<u32, Vec<Receiver<Message>>>,

    /// Stores all actor information
    info: HashMap<u32, stream_service::ActorInfo>,

    /// Mock source, `fragment_id = 0`
    /// TODO: remove this
    mock_source: ConsumableChannelPair,
}

/// `StreamManager` manages all stream operators in this project.
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

    #[cfg(test)]
    pub fn take_source(&self) -> Sender<Message> {
        let mut core = self.core.lock().unwrap();
        core.mock_source.0.take().unwrap()
    }

    #[cfg(test)]
    pub fn take_sink(&self) -> Receiver<Message> {
        let mut core = self.core.lock().unwrap();
        core.receivers.remove(&233).unwrap().remove(0)
    }
}

impl StreamManagerCore {
    fn new() -> Self {
        let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
        Self {
            handles: vec![],
            receivers: HashMap::new(),
            info: HashMap::new(),
            mock_source: (Some(tx), Some(rx)),
        }
    }

    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &mut self,
        dispatcher: &stream_plan::Dispatcher,
        downstreams: &[u32],
    ) -> Box<dyn Output> {
        // create downstream receivers
        let outputs = downstreams
            .iter()
            .map(|downstream| {
                let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                self.receivers
                    .entry(*downstream)
                    .or_insert_with(Vec::new)
                    .push(rx);
                Box::new(ChannelOutput::new(tx)) as Box<dyn Output>
            })
            .collect::<Vec<_>>();

        use stream_plan::Dispatcher_DispatcherType::*;
        match dispatcher.field_type {
            SIMPLE => {
                let mut outputs = outputs;
                assert_eq!(outputs.len(), 1);
                outputs.pop().unwrap()
            }
            ROUND_ROBIN => {
                Box::new(Dispatcher::new(RoundRobinDataDispatcher::new(outputs))) as Box<dyn Output>
            }
            HASH => Box::new(Dispatcher::new(HashDataDispatcher::new(
                outputs,
                vec![dispatcher.get_column_idx() as usize],
            ))),
            BROADCAST => Box::new(Dispatcher::new(BroadcastDispatcher::new(outputs))),
        }
    }

    /// Create a chain of nodes and return the head operator
    fn create_nodes(
        &mut self,
        node: &stream_plan::StreamNode,
        dispatcher: &stream_plan::Dispatcher,
        downstreams: &[u32],
    ) -> Result<Box<dyn UnaryStreamOperator>> {
        let downstream_node: Box<dyn Output> = if node.has_downstream_node() {
            Box::new(OperatorOutput::new(self.create_nodes(
                node.get_downstream_node(),
                dispatcher,
                downstreams,
            )?))
        } else {
            self.create_dispatcher(dispatcher, downstreams)
        };

        use stream_plan::StreamNode_StreamNodeType::*;

        let operator: Box<dyn UnaryStreamOperator> = match node.get_node_type() {
            PROJECTION => {
                let project_node =
                    stream_plan::ProjectNode::parse_from_bytes(node.get_body().get_value())
                        .map_err(ErrorCode::ProtobufError)?;
                let project_exprs = project_node
                    .get_select_list()
                    .iter()
                    .map(build_from_proto)
                    .collect::<Result<Vec<_>>>()?;
                Box::new(ProjectionOperator::new(downstream_node, project_exprs))
            }
            FILTER => {
                let filter_node =
                    stream_plan::FilterNode::parse_from_bytes(node.get_body().get_value())
                        .map_err(ErrorCode::ProtobufError)?;
                let search_condition = build_from_proto(filter_node.get_search_condition())?;
                Box::new(FilterOperator::new(downstream_node, search_condition))
            }
            // TODO: get configuration body of each operator
            SIMPLE_AGG => todo!(),
            GLOBAL_SIMPLE_AGG => todo!(),
            HASH_AGG => todo!(),
            GLOBAL_HASH_AGG => todo!(),
            others => todo!("unsupported StreamNodeType: {:?}", others),
        };
        Ok(operator)
    }

    pub fn update_fragment(&mut self, fragments: &[stream_plan::StreamFragment]) -> Result<()> {
        // TODO: implement this part
        let fragment = &fragments[0];
        let operator_head = self.create_nodes(
            fragment.get_nodes(),
            fragment.get_dispatcher(),
            fragment.get_downstream_fragment_id(),
        )?;
        let fragment_id = fragment.get_fragment_id();

        let upstreams = fragment.get_upstream_fragment_id();
        assert!(!upstreams.is_empty());

        let mut rxs = self.receivers.remove(&fragment_id).unwrap_or_else(Vec::new);

        for upstream in upstreams {
            // TODO: remove this
            if *upstream == 0 {
                // `fragment_id = 0` is used as mock input
                rxs.push(self.mock_source.1.take().unwrap());
                continue;
            }

            let actor = self
                .info
                .get(upstream)
                .expect("upstream actor not found in info table");
            // FIXME: use `is_local_address` from `ExchangeExecutor`.
            if actor.get_host().get_host() == "127.0.0.1" {
                continue;
            } else {
                todo!("remote node is not supported in streaming engine");
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

        let join_handle = if upstreams.len() == 1 {
            // Only one upstream, use `UnarySimpleProcessor`.
            let processor = UnarySimpleProcessor::new(rxs.remove(0), operator_head);
            // Create processor
            tokio::spawn(processor.run())
        } else {
            // Create processor
            let processor = UnaryMergeProcessor::new(rxs, operator_head);
            // Store join handle
            tokio::spawn(processor.run())
        };
        // Store handle for later use
        self.handles.push(join_handle);

        Ok(())
    }

    pub async fn wait_all(&mut self) -> Result<()> {
        for handle in std::mem::take(&mut self.handles) {
            handle.await.unwrap()?;
        }
        Ok(())
    }

    fn update_actor_info(&mut self, table: stream_service::ActorInfoTable) -> Result<()> {
        self.info.clear();
        for actor in table.get_info() {
            assert!(
                self.info
                    .insert(actor.get_fragment_id(), actor.clone())
                    .is_none(),
                "error when creating actor: duplicated actor {}",
                actor.get_fragment_id()
            );
        }
        Ok(())
    }
}

impl Default for StreamManager {
    fn default() -> Self {
        Self::new()
    }
}
