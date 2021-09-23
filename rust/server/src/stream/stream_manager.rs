use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::{ErrorCode, Result};
use crate::expr::build_from_proto;
use crate::protobuf::Message as _;
use crate::stream_op::*;
use futures::channel::mpsc::{channel, Receiver};
use risingwave_proto::stream_plan;
use tokio::task::JoinHandle;

pub struct StreamManagerCore {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: Vec<JoinHandle<Result<()>>>,
    /// `receivers` store the channel receivers for later `Processor`'s use. When `StreamManager`
    /// receives a fragment registeration request, the manager will take receivers of upstreams
    /// out of the `receivers` and create a `Processor`. Meanwhile, it will create a pair of
    /// senders and receivers, where senders are used in `Dispatchers`, and receivers are
    /// stored here for later use.
    receivers: HashMap<u32, Receiver<Message>>,
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

    pub fn create_fragment(&self, fragment: stream_plan::StreamFragment) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.create_fragment(fragment)
    }

    pub async fn wait_all(&mut self) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        core.wait_all().await
    }
}

impl StreamManagerCore {
    fn new() -> Self {
        Self {
            handles: vec![],
            receivers: HashMap::new(),
        }
    }

    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &mut self,
        dispatcher: &stream_plan::Dispatcher,
        downstreams: &[u32],
    ) -> Box<dyn Output> {
        let outputs = downstreams
            .iter()
            .map(|downstream| {
                let (tx, rx) = channel(16);
                assert!(self.receivers.insert(*downstream, rx).is_none());
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
            Box::new(LocalOutput::new(self.create_nodes(
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

    pub fn create_fragment(&mut self, fragment: stream_plan::StreamFragment) -> Result<()> {
        let operator_head = self.create_nodes(
            fragment.get_nodes(),
            fragment.get_dispatcher(),
            fragment.get_downstream_fragment_id(),
        )?;

        let upstreams = fragment.get_upstream_fragment_id();
        assert!(!upstreams.is_empty());

        let join_handle = if upstreams.len() == 1 {
            // Only one upstream, use `UnarySimpleProcessor`.
            let rx = self
                .receivers
                .remove(&upstreams[0])
                .expect("upstream not found");
            let processor = UnarySimpleProcessor::new(rx, operator_head);
            // Create processor
            tokio::spawn(processor.run())
        } else {
            // There are multiple upstreams, so use `UnaryMergeProcessor`.
            // Get all upstream channels from `receivers`.
            let rxs = upstreams
                .iter()
                .map(|upstream| self.receivers.remove(upstream).expect("upstream not found"))
                .collect::<Vec<_>>();
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
}

impl Default for StreamManager {
    fn default() -> Self {
        Self::new()
    }
}
