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
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use etcd_client::{Client, ConnectOptions, Error, LeaseClient};
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_stream::StreamExt;

use crate::rpc::elections::run_elections;
use crate::storage::MetaStore;
use crate::MetaResult;
const META_ELECTION_KEY: &str = "ELECTION";

pub struct ElectionContext {
    pub leader: MetaLeaderInfo,
    pub events: watch::Receiver<Option<MetaLeaderInfo>>,
    pub handle: JoinHandle<()>,
    pub stop_sender: oneshot::Sender<()>,
}

#[async_trait::async_trait]
pub trait ElectionClient {
    async fn start(&self, id: String, lease_ttl: i64) -> MetaResult<ElectionContext>;
}

pub struct EtcdElectionClient {
    pub client: Client,
}

struct EtcdLeaseSession {
    lease_id: i64,
    ttl: i64,
    handle: JoinHandle<()>,
    client: LeaseClient,
}

impl EtcdLeaseSession {
    async fn drop_session(mut self) {
        let _ = self.client.revoke(self.lease_id).await;
        self.handle.abort()
    }

    async fn replace_lease(&mut self, lease_id: i64) -> MetaResult<()> {
        self.handle.abort();

        let mut lease_client = self.client.clone();

        let ttl = self.ttl;
        let handle = tokio::spawn(async move {
            Self::lease_keep_loop(
                &mut lease_client,
                lease_id,
                Duration::from_secs((ttl / 2) as u64),
            )
            .await
        });

        self.lease_id = lease_id;
        self.handle = handle;

        Ok(())
    }

    pub async fn lease_keep_loop(
        lease_client: &mut LeaseClient,
        lease_id: i64,
        period: Duration,
    ) -> ! {
        let mut ticker = time::interval(period);

        let (mut lease_keeper, _lease_keep_alive_stream) =
            lease_client.keep_alive(lease_id).await.unwrap();

        loop {
            ticker.tick().await;
            if let Err(e) = lease_keeper.keep_alive().await {
                tracing::error!("lease keeper failed {}, recreating", e);

                let (new_keeper, _stream) = match lease_client.keep_alive(lease_id).await {
                    Ok((a, b)) => (a, b),
                    Err(e) => {
                        tracing::error!("rebuild lease keeper failed {}", e);
                        continue;
                    }
                };

                lease_keeper = new_keeper;
            }
        }
    }
}

impl EtcdElectionClient {
    async fn lease_session(&self, ttl: i64) -> MetaResult<EtcdLeaseSession> {
        let mut lease_client = self.client.lease_client();
        let lease_id = lease_client.grant(ttl, None).await.unwrap();

        let lease_id = lease_id.id();

        let handle = tokio::spawn(async move {
            EtcdLeaseSession::lease_keep_loop(
                &mut lease_client,
                lease_id,
                Duration::from_secs((ttl / 2) as u64),
            )
            .await
        });

        Ok(EtcdLeaseSession {
            lease_id,
            ttl,
            handle,
            client: self.client.lease_client(),
        })
    }

    fn value_to_address(value: &[u8]) -> String {
        String::from_utf8_lossy(value).to_string()
    }

    async fn run(
        &self,
        stop: oneshot::Receiver<()>,
        value: &[u8],
        lease_ttl: i64,
    ) -> MetaResult<(
        JoinHandle<()>,
        watch::Receiver<Option<MetaLeaderInfo>>,
        MetaLeaderInfo,
    )> {
        #[derive(Debug, Clone, Copy)]
        enum State {
            Init,
            Reconnect,
            Campaign,
            Observe,
        }

        let client = self.client.clone();
        let mut stop = stop;

        let mut lease_session = self
            .lease_session(lease_ttl)
            .await
            .expect("lease session must available");

        let (sender, receiver) = watch::channel::<Option<MetaLeaderInfo>>(None);

        let id = value.to_vec();

        let event_sender = sender;

        let ticker_period = Duration::from_secs(1);
        let join_handle = tokio::spawn(async move {
            let mut state = State::Init;

            let mut current_leader: Option<Vec<u8>> = None;

            tracing::info!("init election with lease id {}", lease_session.lease_id);

            let mut ticker = time::interval(ticker_period);
            loop {
                ticker.tick().await;

                tracing::info!("current etcd election state {:?}", state);

                match state {
                    State::Init => {
                        let leader_kv =
                            match client.election_client().leader(META_ELECTION_KEY).await {
                                Ok(mut leader) => match leader.take_kv() {
                                    None => continue,
                                    Some(kv) => kv,
                                },
                                Err(Error::GRpcStatus(e))
                                    if e.message() == "election: no leader" =>
                                {
                                    tracing::info!("no leader now, run election");

                                    // no leader now
                                    state = State::Campaign;
                                    continue;
                                }
                                Err(e) => {
                                    tracing::error!("error happened when calling leader {}", e);

                                    // todo, continue is ok?
                                    state = State::Reconnect;
                                    continue;
                                }
                            };

                        let discovered_leader_id = leader_kv.value().to_vec();

                        if match &mut current_leader {
                            None => true,
                            Some(leader_id) => *leader_id != discovered_leader_id,
                        } {
                            event_sender
                                .send(Some(MetaLeaderInfo {
                                    node_address: Self::value_to_address(leader_kv.value()),
                                    lease_id: leader_kv.lease() as u64,
                                }))
                                .unwrap();

                            current_leader = Some(discovered_leader_id.clone());
                        }

                        if leader_kv.value() == id.as_slice() {
                            lease_session
                                .replace_lease(leader_kv.lease())
                                .await
                                .unwrap();

                            state = State::Observe
                        } else {
                            state = State::Campaign
                        }
                    }
                    State::Campaign => {
                        let mut election_client = client.election_client();
                        tokio::select! {
                            resp = election_client.campaign(META_ELECTION_KEY, id.clone(), lease_session.lease_id) => {
                                match resp {
                                    Ok(_) => {
                                        tracing::info!("election campaign done, changing state to observe mode");
                                        state = State::Observe;
                                    }
                                    Err(e) => {
                                        tracing::error!("election campaign failed due to {}, changing state to observe mode", e.to_string());
                                        state = State::Reconnect;
                                    }
                                }
                                continue
                            }
                            _ = stop.borrow_mut() => {
                                tracing::info!("stopping election");
                                lease_session.drop_session().await;
                                return
                            }
                        }
                    }
                    State::Observe => {
                        let mut observe_stream = client
                            .election_client()
                            .observe(META_ELECTION_KEY)
                            .await
                            .expect("creating observe stream failed, fail asap");

                        loop {
                            tokio::select! {
                                _ = stop.borrow_mut() => {
                                    lease_session.drop_session().await;
                                    return
                                }

                                leader_resp = observe_stream.next() => {
                                    let leader_resp =
                                        leader_resp.expect("observe stream should never stop");

                                    let leader_kv = match leader_resp {
                                        Ok(mut leader) => match leader.take_kv() {
                                            Some(kv) => kv,
                                            None => continue,
                                        },
                                        Err(e) => {
                                            tracing::error!("observing stream failed {}", e.to_string());
                                            state = State::Reconnect;

                                            break;
                                        }
                                    };

                                    if leader_kv.value() != id.as_slice() {
                                        panic!("leader changed!");
                                    }
                                }
                            }
                        }
                    }

                    State::Reconnect => match client.maintenance_client().status().await {
                        Ok(s) => {
                            tracing::info!("etcd election server status is {:?}", s);
                            state = State::Init;
                            continue;
                        }
                        Err(e) => {
                            tracing::error!(
                                "etcd election server status is failed {}",
                                e.to_string()
                            );
                        }
                    },
                }
            }
        });

        let mut ticker = time::interval(ticker_period);

        let leader_info = loop {
            ticker.tick().await;
            let leader_resp = self
                .client
                .election_client()
                .leader(META_ELECTION_KEY)
                .await;
            match leader_resp {
                Ok(mut leader) => {
                    let kv = match leader.take_kv() {
                        None => continue,
                        Some(kv) => kv,
                    };

                    let meta_leader_info = MetaLeaderInfo {
                        node_address: Self::value_to_address(kv.value()),
                        lease_id: kv.lease() as u64,
                    };
                    break meta_leader_info;
                }
                Err(Error::GRpcStatus(e)) if e.message() == "election: no leader" => {
                    tracing::info!("waiting for first leader");
                    continue;
                }
                Err(e) => {
                    tracing::error!("waiting for first leader failed {}", e.to_string());
                    continue;
                }
            };
        };

        Ok((join_handle, receiver, leader_info))
    }
}

#[async_trait::async_trait]
impl ElectionClient for EtcdElectionClient {
    async fn start(&self, id: String, lease_ttl: i64) -> MetaResult<ElectionContext> {
        let (stop_sender, stop_recv) = oneshot::channel::<()>();

        let value = id.as_bytes().to_vec();

        let (handle, receiver, leader) = self
            .run(stop_recv, &value, lease_ttl)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(ElectionContext {
            leader,
            events: receiver,
            handle,
            stop_sender,
        })
    }
}

impl EtcdElectionClient {
    #[allow(dead_code)]
    pub(crate) async fn new(
        endpoints: Vec<String>,
        options: Option<ConnectOptions>,
        auth_enabled: bool,
    ) -> MetaResult<Self> {
        assert!(!auth_enabled, "auth not supported");

        let client = Client::connect(&endpoints, options.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(Self { client })
    }
}

pub struct KvBasedElectionClient<S: MetaStore> {
    meta_store: Arc<S>,
}

impl<S: MetaStore> KvBasedElectionClient<S> {
    pub fn new(meta_store: Arc<S>) -> KvBasedElectionClient<S> {
        Self { meta_store }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> ElectionClient for KvBasedElectionClient<S> {
    async fn start(&self, id: String, lease_ttl: i64) -> MetaResult<ElectionContext> {
        let (leader, handle, stop_sender, events) =
            run_elections(id, self.meta_store.clone(), lease_ttl as u64).await?;

        let ctx = ElectionContext {
            leader,
            events,
            handle,
            stop_sender,
        };

        Ok(ctx)
    }
}
