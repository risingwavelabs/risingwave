// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::BorrowMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use etcd_client::{Client, ConnectOptions, Error, GetOptions};
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::{oneshot, watch};
use tokio::time;
use tokio_stream::StreamExt;

use crate::MetaResult;

const META_ELECTION_KEY: &str = "__meta_election";

#[async_trait::async_trait]
pub trait ElectionClient: Send + Sync + 'static {
    async fn run_once(&self, ttl: i64, stop: watch::Receiver<()>) -> MetaResult<()>;
    async fn leader(&self) -> MetaResult<Option<MetaLeaderInfo>>;
    async fn get_members(&self) -> MetaResult<Vec<(String, i64, bool)>>;
    async fn is_leader(&self) -> bool;
}

pub struct EtcdElectionClient {
    pub client: Client,
    is_leader: AtomicBool,
    pub id: String,
}

#[async_trait::async_trait]
impl ElectionClient for EtcdElectionClient {
    async fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    async fn leader(&self) -> MetaResult<Option<MetaLeaderInfo>> {
        let mut election_client = self.client.election_client();
        let leader = election_client.leader(META_ELECTION_KEY).await;

        let leader = match leader {
            Ok(leader) => Ok(Some(leader)),
            Err(Error::GRpcStatus(e)) if e.message() == "election: no leader" => Ok(None),
            Err(e) => Err(e),
        }?;

        Ok(leader.and_then(|leader| {
            leader.kv().map(|leader_kv| MetaLeaderInfo {
                node_address: String::from_utf8_lossy(leader_kv.value()).to_string(),
                lease_id: leader_kv.lease() as u64,
            })
        }))
    }

    async fn run_once(&self, ttl: i64, stop: watch::Receiver<()>) -> MetaResult<()> {
        let mut lease_client = self.client.lease_client();
        let mut election_client = self.client.election_client();
        let mut stop = stop;

        self.is_leader.store(false, Ordering::Relaxed);

        let leader_resp = election_client.leader(META_ELECTION_KEY).await;

        let mut lease_id = match leader_resp.map(|mut resp| resp.take_kv()) {
            // leader exists
            Ok(Some(leader_kv)) if leader_kv.value() == self.id.as_bytes() => Ok(leader_kv.lease()),

            // leader kv not exists (may not happen)
            Ok(_) => lease_client.grant(ttl, None).await.map(|resp| resp.id()),

            // no leader
            Err(Error::GRpcStatus(e)) if e.message() == "election: no leader" => {
                lease_client.grant(ttl, None).await.map(|resp| resp.id())
            }

            // connection error
            Err(e) => Err(e),
        }?;

        // try keep alive
        let (mut keeper, mut resp_stream) = lease_client.keep_alive(lease_id).await?;
        let _resp = keeper.keep_alive().await?;
        let resp = resp_stream.message().await?;
        if let Some(resp) = resp && resp.ttl() <= 0 {
            tracing::info!("lease {} expired or revoked, re-granting", lease_id);
            // renew lease_id
            lease_id = lease_client.grant(ttl, None).await.map(|resp| resp.id())?;
            tracing::info!("lease {} re-granted", lease_id);
        }

        let (keep_alive_fail_tx, mut keep_alive_fail_rx) = oneshot::channel();

        let mut lease_client = self.client.lease_client();

        let mut stop_ = stop.clone();

        let handle = tokio::spawn(async move {
            let (mut keeper, mut resp_stream) = match lease_client.keep_alive(lease_id).await {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(
                        "create lease keeper for {} failed {}",
                        lease_id,
                        e.to_string()
                    );
                    keep_alive_fail_tx.send(()).unwrap();
                    return;
                }
            };

            let mut ticker = time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    biased;

                    _ = ticker.tick() => {
                        if let Err(err) = keeper.keep_alive().await {
                            tracing::error!("keep alive for lease {} failed {}", lease_id, err);
                            keep_alive_fail_tx.send(()).unwrap();
                            break;
                        }

                        match resp_stream.message().await {
                            Ok(Some(resp)) => {
                                if resp.ttl() <= 0 {
                                    tracing::warn!("lease expired or revoked {}", lease_id);
                                    keep_alive_fail_tx.send(()).unwrap();
                                    break;
                                }
                            },
                            Ok(None) => {
                                tracing::warn!("lease keeper for lease {} response stream closed unexpected", lease_id);
                                keep_alive_fail_tx.send(()).unwrap();
                                break;
                            }
                            Err(e) => {
                                tracing::error!("lease keeper failed {}", e.to_string());
                                keep_alive_fail_tx.send(()).unwrap();
                                break;
                            }
                        };
                    }

                    _ = stop_.changed() => {
                        tracing::info!("stop signal received");
                        break;
                    }
                }
            }
            tracing::info!("keep alive loop for lease {} stopped", lease_id);
        });

        let _guard = scopeguard::guard(handle, |handle| handle.abort());

        tokio::select! {
            biased;

            _ = stop.changed() => {
                tracing::info!("stop signal received");
                return Ok(());
            }

            _ = election_client.campaign(META_ELECTION_KEY, self.id.as_bytes().to_vec(), lease_id) => {
                tracing::info!("client {} wins election {}", self.id, META_ELECTION_KEY);
            }
        };

        let mut observe_stream = election_client.observe(META_ELECTION_KEY).await?;

        self.is_leader.store(true, Ordering::Relaxed);

        loop {
            tokio::select! {
                biased;
                _ = stop.changed() => {
                    tracing::info!("stop signal received");
                    break;
                },
                _ = keep_alive_fail_rx.borrow_mut() => {
                    tracing::error!("keep alive failed, stopping main loop");
                    break;
                },
                resp = observe_stream.next() => {
                    match resp {
                        None => unreachable!(),
                        Some(Ok(leader)) => {
                            if let Some(kv) = leader.kv() && kv.value() != self.id.as_bytes() {
                                tracing::warn!("leader has been changed to {}", String::from_utf8_lossy(kv.value()).to_string());
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            tracing::error!("error {} received from leader observe stream", e.to_string());
                            break;
                        }
                    }
                }
            }
        }

        tracing::warn!("client {} lost leadership", self.id);

        self.is_leader.store(false, Ordering::Relaxed);

        Ok(())
    }

    async fn get_members(&self) -> MetaResult<Vec<(String, i64, bool)>> {
        let mut client = self.client.kv_client();
        let keys = client
            .get(META_ELECTION_KEY, Some(GetOptions::new().with_prefix()))
            .await?;

        // todo, sort by revision
        Ok(keys
            .kvs()
            .iter()
            .enumerate()
            .map(|(i, kv)| {
                (
                    String::from_utf8_lossy(kv.value()).to_string(),
                    kv.lease(),
                    i == 0,
                )
            })
            .collect())
    }
}

impl EtcdElectionClient {
    pub(crate) async fn new(
        endpoints: Vec<String>,
        options: Option<ConnectOptions>,
        id: String,
    ) -> MetaResult<Self> {
        let client = Client::connect(&endpoints, options.clone()).await?;

        Ok(Self {
            client,
            is_leader: AtomicBool::from(false),
            id,
        })
    }
}
