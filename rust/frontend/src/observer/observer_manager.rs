use std::net::SocketAddr;

use log::{debug, error};
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use risingwave_rpc_client::MetaClient;
use tonic::Streaming;

use crate::scheduler::schedule::WorkerNodeManagerRef;

/// `ObserverManager` is used to update data based on notification from meta.
/// Use `set_***` method to init ObserverManager and then
/// call `start` to spawn a new asynchronous task
/// which receives meta's notification and update frontend's data.
pub(crate) struct ObserverManager {
    rx: Streaming<SubscribeResponse>,
    worker_node_manager: Option<WorkerNodeManagerRef>,
}

impl ObserverManager {
    pub async fn new(client: MetaClient, addr: SocketAddr) -> Self {
        let rx = client.subscribe(addr, WorkerType::Frontend).await.unwrap();
        Self {
            rx,
            worker_node_manager: None,
        }
    }

    /// `start` is used to spawn a new asynchronous task which receives meta's notification and
    /// update frontend's data. `start` use `mut self` as parameter, it supposes that the code
    /// for initialization is sequential and `start` should be called after
    /// `set_worker_node_manager`.
    pub fn start(mut self) {
        tokio::spawn(async move {
            loop {
                if let Ok(resp) = self.rx.message().await {
                    if resp.is_none() {
                        error!("Stream of notification terminated.");
                        break;
                    }
                    let resp = resp.unwrap();
                    let operation = resp.operation();

                    match resp.info {
                        Some(Info::Database(_database)) => {}
                        Some(Info::Node(node)) => {
                            self.update_worker_node_manager(operation, node);
                        }
                        Some(Info::Schema(_schema)) => {}
                        Some(Info::Table(_table)) => {}
                        None => (),
                    }
                }
            }
        });
    }

    /// `set_worker_node_manager` is used to get a quote of `WorkerNodeManager`.
    pub fn set_worker_node_manager(&mut self, worker_node_manager: WorkerNodeManagerRef) {
        self.worker_node_manager = Some(worker_node_manager);
    }

    /// `update_worker_node_manager` is called in `start` method.
    /// It calls `add_worker_node` and `remove_worker_node` of `WorkerNodeManager`.
    fn update_worker_node_manager(&self, operation: Operation, node: WorkerNode) {
        debug!(
            "Update worker nodes, operation: {:?}, node: {:?}",
            operation, node
        );
        let manager = self
            .worker_node_manager
            .as_ref()
            .expect("forget to call set_worker_node_manager before call start");

        match operation {
            Operation::Add => manager.add_worker_node(node),
            Operation::Delete => manager.remove_worker_node(node),
            _ => (),
        }
    }
}
