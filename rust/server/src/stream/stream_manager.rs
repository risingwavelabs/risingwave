use std::sync::Mutex;

use crate::error::Result;
use crate::stream_op::*;
use futures::channel::mpsc::channel;
use risingwave_proto::stream_plan;
use tokio::task::JoinHandle;

/// `StreamManager` manages all stream operators in this project.
pub struct StreamManager {
    handles: Mutex<Vec<JoinHandle<Result<()>>>>,
}

impl StreamManager {
    pub fn new() -> Self {
        StreamManager {
            handles: Mutex::new(vec![]),
        }
    }

    fn create_nodes(
        node: &stream_plan::StreamNode,
        dispatcher: &stream_plan::Dispatcher,
    ) -> Box<dyn UnaryStreamOperator> {
        let downstream_node: Box<dyn Output> = if node.has_downstream_node() {
            Box::new(LocalOutput::new(Self::create_nodes(
                node.get_downstream_node(),
                dispatcher,
            )))
        } else {
            use stream_plan::Dispatcher_DispatcherType::*;
            match dispatcher.field_type {
                SIMPLE => {
                    // TODO: dispatch real message
                    let (tx, _) = channel(16);
                    Box::new(ChannelOutput::new(tx)) as Box<dyn Output>
                }
                ROUND_ROBIN => Box::new(Dispatcher::new(RoundRobinDataDispatcher::new(vec![])))
                    as Box<dyn Output>,
                HASH => Box::new(Dispatcher::new(HashDataDispatcher::new(
                    vec![],
                    vec![dispatcher.get_column_idx() as usize],
                ))),
            }
        };

        use stream_plan::StreamNode_StreamNodeType::*;

        let operator: Box<dyn UnaryStreamOperator> = match node.get_node_type() {
            // TODO: get configuration body of each operator
            PROJECTION => Box::new(ProjectionOperator::new(downstream_node, vec![])),
            FILTER => todo!(),
            SIMPLE_AGG => todo!(),
            GLOBAL_SIMPLE_AGG => todo!(),
            HASH_AGG => todo!(),
            GLOBAL_HASH_AGG => todo!(),
            others => todo!("unsupported StreamNodeType: {:?}", others),
        };
        operator
    }

    pub fn create_fragment(&self, fragment: stream_plan::StreamFragment) -> Result<()> {
        // TODO: receive real message
        let (_, rx) = channel(16);
        let operator_head = Self::create_nodes(fragment.get_nodes(), fragment.get_dispatcher());
        let processor = UnarySimpleProcessor::new(rx, operator_head);
        let join_handle = tokio::spawn(processor.run());
        let mut handles = self.handles.lock().unwrap();
        handles.push(join_handle);
        Ok(())
    }
}

impl Default for StreamManager {
    fn default() -> Self {
        Self::new()
    }
}
