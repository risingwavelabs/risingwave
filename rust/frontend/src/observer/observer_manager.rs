use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use risingwave_rpc_client::MetaClient;
use tonic::Streaming;

/// Used to update based on notification from meta.
pub(crate) struct ObserverManager {
    rx: Streaming<SubscribeResponse>,
    worker_nodes: Option<Arc<RwLock<Vec<WorkerNode>>>>,
}

impl ObserverManager {
    pub async fn new(client: MetaClient, addr: SocketAddr) -> Self {
        let rx = client.subscribe(addr, WorkerType::Frontend).await.unwrap();
        Self {
            rx,
            worker_nodes: None,
        }
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            loop {
                if let Ok(resp) = self.rx.message().await {
                    if resp.is_none() {
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

    pub fn set_worker_node_manager(&mut self, worker_nodes: Arc<RwLock<Vec<WorkerNode>>>) {
        self.worker_nodes = Some(worker_nodes);
    }

    pub fn update_worker_node_manager(&self, operation: Operation, node: WorkerNode) {
        let worker_nodes = self
            .worker_nodes
            .as_ref()
            .expect("forget to call set_worker_node_manager before call start");

        match operation {
            Operation::Add => {
                worker_nodes.write().unwrap().push(node);
            }
            Operation::Delete => {
                worker_nodes.write().unwrap().retain(|x| *x == node);
            }
            _ => (),
        }
    }
}
