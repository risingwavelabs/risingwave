// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::subscribe_response::Info;
use risingwave_pb::meta::SubscribeResponse;
use risingwave_rpc_client::{MetaClient, NotificationStream};
use risingwave_source::SourceManagerRef;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

pub struct ObserverManager {
    rx: Box<dyn NotificationStream>,
    source_manager: SourceManagerRef,
}

impl ObserverManager {
    pub async fn new(client: MetaClient, addr: HostAddr, source_manager: SourceManagerRef) -> Self {
        let rx = client
            .subscribe(addr, WorkerType::ComputeNode)
            .await
            .unwrap();
        Self { rx, source_manager }
    }

    pub fn handle_first_notification(&mut self, resp: SubscribeResponse) -> Result<()> {
        match resp.info {
            Some(Info::BeSnapshot(sources)) => {
                // Create sources here
                self.source_manager.apply_snapshot(sources)?;
                Ok(())
            }
            _ => {
                return Err(ErrorCode::InternalError(format!(
                    "the first notify should be snapshot, but get {:?}",
                    resp
                ))
                .into())
            }
        }
    }

    pub fn handle_notification(&mut self, _resp: SubscribeResponse) {
        // TODO: handle more notifications
    }

    pub async fn start(mut self) -> Result<(JoinHandle<()>, UnboundedSender<()>)> {
        let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
        let first_resp = self.rx.next().await?.ok_or_else(|| {
            ErrorCode::InternalError(
                "ObserverManager start failed, Stream of notification terminated at the start."
                    .to_string(),
            )
        })?;
        self.handle_first_notification(first_resp)?;
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_recv.recv() => {
                        tracing::info!("ObserverManager shutdown.");
                        break;
                    }
                    resp = self.rx.next() => {
                        match resp {
                            Ok(resp) => {
                                if resp.is_none() {
                                    tracing::error!("Stream of notification terminated.");
                                    break;
                                }
                                self.handle_notification(resp.unwrap());
                            }
                            Err(e) => {
                                tracing::error!("Stream of notification terminated with error: {:?}", e);
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok((handle, shutdown_send))
    }
}
