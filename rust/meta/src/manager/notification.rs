use std::sync::Arc;

use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;
use futures::future;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result as RwResult, ToRwResult};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::cluster::WorkerKey;

pub type Notification = Result<SubscribeResponse, Status>;

const BUFFER_SIZE: usize = 4;

pub struct NotificationManager {
    fe_observers: DashMap<WorkerKey, Sender<Notification>>,
    be_observers: DashMap<WorkerKey, Sender<Notification>>,
}

#[allow(dead_code)]
pub enum NotificationTarget {
    ComputeNode,
    Frontend,
    All,
}

pub type NotificationManagerRef = Arc<NotificationManager>;

/// [`NotificationManager`] manager notification meta data.
impl NotificationManager {
    pub fn new() -> Self {
        Self {
            fe_observers: DashMap::new(),
            be_observers: DashMap::new(),
        }
    }

    pub fn subscribe(
        &self,
        host_address: HostAddress,
        worker_type: WorkerType,
    ) -> RwResult<ReceiverStream<Notification>> {
        let (tx, rx) = mpsc::channel(BUFFER_SIZE);
        match worker_type {
            WorkerType::ComputeNode => self.be_observers.insert(WorkerKey(host_address), tx),
            WorkerType::Frontend => self.fe_observers.insert(WorkerKey(host_address), tx),
            _ => {
                return Err(RwError::from(InternalError(format!(
                    "fail to subscribe with unknown type: {:?}",
                    worker_type
                ))));
            }
        };

        Ok(ReceiverStream::new(rx))
    }

    pub async fn notify(
        &self,
        operation: Operation,
        info: &Info,
        target_type: NotificationTarget,
    ) -> RwResult<()> {
        future::join_all(self.get_iter(target_type).map(|e| async move {
            e.value()
                .send(Ok(SubscribeResponse {
                    status: None,
                    operation: operation as i32,
                    info: Some(info.clone()),
                }))
                .await
                .to_rw_result_with(format!(
                    "fail to notify with operation: {:?} and info: {:?}",
                    operation, info
                ))?;
            Ok(())
        }))
        .await
        .into_iter()
        .collect::<RwResult<()>>()
    }

    /// Return iter of frontend observers or compute node observers or their combination.
    pub fn get_iter(
        &self,
        target_type: NotificationTarget,
    ) -> Box<dyn '_ + Iterator<Item = RefMulti<WorkerKey, Sender<Notification>>> + Send> {
        match target_type {
            NotificationTarget::All => {
                Box::new(self.fe_observers.iter().chain(self.be_observers.iter()))
            }
            NotificationTarget::ComputeNode => Box::new(self.be_observers.iter()),
            NotificationTarget::Frontend => Box::new(self.fe_observers.iter()),
        }
    }
}
