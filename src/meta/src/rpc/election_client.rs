// Copyright 2023 RisingWave Labs
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
use std::collections::HashSet;
use std::time::Duration;

use etcd_client::{ConnectOptions, Error, GetOptions, LeaderKey, ResignOptions};
use risingwave_common::bail;
use tokio::sync::watch::Receiver;
use tokio::sync::{oneshot, watch};
use tokio::time;
use tokio_stream::StreamExt;

use crate::storage::WrappedEtcdClient;
use crate::MetaResult;

const META_ELECTION_KEY: &str = "__meta_election_";

pub struct ElectionMember {
    pub id: String,
    pub is_leader: bool,
}

#[async_trait::async_trait]
pub trait ElectionClient: Send + Sync + 'static {
    fn id(&self) -> MetaResult<String>;
    async fn run_once(&self, ttl: i64, stop: Receiver<()>) -> MetaResult<()>;
    fn subscribe(&self) -> Receiver<bool>;
    async fn leader(&self) -> MetaResult<Option<ElectionMember>>;
    async fn get_members(&self) -> MetaResult<Vec<ElectionMember>>;
    async fn is_leader(&self) -> bool;
}

pub struct EtcdElectionClient {
    id: String,
    is_leader_sender: watch::Sender<bool>,
    client: WrappedEtcdClient,
}

#[async_trait::async_trait]
impl ElectionClient for EtcdElectionClient {
    async fn is_leader(&self) -> bool {
        *self.is_leader_sender.borrow()
    }

    async fn leader(&self) -> MetaResult<Option<ElectionMember>> {
        let leader = self.client.leader(META_ELECTION_KEY).await;
        let leader = match leader {
            Ok(leader) => Ok(Some(leader)),
            Err(Error::GRpcStatus(e)) if e.message() == "election: no leader" => Ok(None),
            Err(e) => Err(e),
        }?;

        Ok(leader.and_then(|leader| {
            leader.kv().map(|leader_kv| ElectionMember {
                id: String::from_utf8_lossy(leader_kv.value()).to_string(),
                is_leader: true,
            })
        }))
    }

    async fn run_once(&self, ttl: i64, stop: watch::Receiver<()>) -> MetaResult<()> {
        let mut stop = stop;

        tracing::info!("client {} start election", self.id);

        // is restored leader from previous session?
        let mut restored_leader = false;

        let mut lease_id = match self
            .client
            .leader(META_ELECTION_KEY)
            .await
            .map(|mut resp| resp.take_kv())
        {
            // leader exists, restore leader
            Ok(Some(leader_kv)) if leader_kv.value() == self.id.as_bytes() => {
                tracing::info!("restoring leader from lease {}", leader_kv.lease());
                restored_leader = true;
                Ok(leader_kv.lease())
            }

            // leader kv not exists (may not happen)
            Ok(_) => self.client.grant(ttl, None).await.map(|resp| resp.id()),

            // no leader
            Err(Error::GRpcStatus(e)) if e.message() == "election: no leader" => {
                self.client.grant(ttl, None).await.map(|resp| resp.id())
            }

            // connection error
            Err(e) => Err(e),
        }?;

        tracing::info!("use lease id {}", lease_id);

        // try keep alive
        let (mut keeper, mut resp_stream) = self.client.keep_alive(lease_id).await?;
        let _resp = keeper.keep_alive().await?;
        let resp = resp_stream.message().await?;
        if let Some(resp) = resp && resp.ttl() <= 0 {
            tracing::info!("lease {} expired or revoked, re-granting", lease_id);
            if restored_leader {
                tracing::info!("restored leader lease {} lost", lease_id);
                restored_leader = false;
            }
            // renew lease_id
            lease_id = self.client.grant(ttl, None).await.map(|resp| resp.id())?;
            tracing::info!("lease {} re-granted", lease_id);
        }

        let (keep_alive_fail_tx, mut keep_alive_fail_rx) = oneshot::channel();

        let mut stop_ = stop.clone();

        let lease_client = self.client.clone();

        let handle = tokio::task::spawn(async move {
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

            // timeout controller, when keep alive fails for more than a certain period of time
            // before it is considered a complete failure
            let mut timeout = time::interval(Duration::from_secs_f64(ttl as f64 / 2.0));
            timeout.reset();

            let mut keep_alive_sending = false;

            loop {
                tokio::select! {
                    biased;

                    _ = timeout.tick() => {
                        tracing::error!("lease {} keep alive timeout", lease_id);
                        keep_alive_fail_tx.send(()).unwrap();
                        break;
                    }

                    _ = ticker.tick(), if !keep_alive_sending => {
                        if let Err(err) = keeper.keep_alive().await {
                            tracing::debug!("keep alive for lease {} failed {}", lease_id, err);
                            continue
                        }

                        keep_alive_sending = true;
                    }

                    resp = resp_stream.message() => {
                        keep_alive_sending = false;
                        match resp {
                            Ok(Some(resp)) => {
                                if resp.ttl() <= 0 {
                                    tracing::error!("lease expired or revoked {}", lease_id);
                                    keep_alive_fail_tx.send(()).unwrap();
                                    break;
                                }

                                timeout.reset();
                            },
                            Ok(None) => {
                                tracing::debug!("lease keeper for lease {} response stream closed unexpected", lease_id);

                                // try to re-create lease keeper, with timeout as ttl / 2
                                if let Ok(Ok((keeper_, resp_stream_))) = time::timeout(Duration::from_secs_f64(ttl as f64 / 2.0), lease_client.keep_alive(lease_id)).await {
                                    keeper = keeper_;
                                    resp_stream = resp_stream_;
                                };

                                continue;
                            }
                            Err(e) => {
                                tracing::error!("lease keeper failed {}", e.to_string());
                                continue;
                            }
                        };
                    }

                    _ = stop_.changed() => {
                        tracing::info!("stop signal received when keeping alive");
                        break;
                    }
                }
            }
            tracing::info!("keep alive loop for lease {} stopped", lease_id);
        });

        let _guard = scopeguard::guard(handle, |handle| handle.abort());

        if !restored_leader {
            self.is_leader_sender.send_replace(false);
            tracing::info!("no restored leader, campaigning");
        }

        // Even if we are already a restored leader, we still need to campaign to obtain the correct
        // `LeaderKey` from campaign response, which is used to provide the parameter for the
        // resign call.
        let leader_key: Option<LeaderKey> = tokio::select! {
            biased;

            _ = stop.changed() => {
                tracing::info!("stop signal received when campaigning");
                return Ok(());
            }

            _ = keep_alive_fail_rx.borrow_mut() => {
                tracing::error!("keep alive failed, stopping main loop");
                bail!("keep alive failed, stopping main loop");
            },


            campaign_resp = self.client.campaign(META_ELECTION_KEY, self.id.as_bytes().to_vec(), lease_id) => {
                let campaign_resp = campaign_resp?;
                tracing::info!("client {} wins election {}", self.id, META_ELECTION_KEY);
                campaign_resp.leader().cloned()
            }
        };

        self.is_leader_sender.send_replace(true);

        let mut observe_stream = self.client.observe(META_ELECTION_KEY).await?;

        loop {
            tokio::select! {
                biased;
                _ = stop.changed() => {
                    tracing::info!("stop signal received when observing");

                    if let Some(leader_key) = leader_key {
                        tracing::info!("leader key found with lease {}, resigning", leader_key.lease());
                        self.client.resign(Some(ResignOptions::new().with_leader(leader_key))).await?;
                    }

                    break;
                },
                _ = keep_alive_fail_rx.borrow_mut() => {
                    tracing::error!("keep alive failed, stopping main loop");
                    break;
                },
                resp = observe_stream.next() => {
                    match resp {
                        None => {
                            tracing::debug!("observe stream closed unexpected, recreating");

                            // try to re-create observe stream, with timeout as ttl / 2
                            if let Ok(Ok(stream)) = time::timeout(Duration::from_secs_f64(ttl as f64 / 2.0), self.client.observe(META_ELECTION_KEY)).await {
                                observe_stream = stream;
                                tracing::debug!("recreating observe stream");
                            }
                        }
                        Some(Ok(leader)) => {
                            if let Some(kv) = leader.kv() && kv.value() != self.id.as_bytes() {
                                tracing::warn!("leader has been changed to {}", String::from_utf8_lossy(kv.value()).to_string());
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!("error {} received from leader observe stream", e.to_string());
                            continue
                        }
                    }
                }
            }
        }

        tracing::warn!("client {} lost leadership", self.id);

        self.is_leader_sender.send_replace(false);

        Ok(())
    }

    async fn get_members(&self) -> MetaResult<Vec<ElectionMember>> {
        let keys = self
            .client
            .get(META_ELECTION_KEY, Some(GetOptions::new().with_prefix()))
            .await?;

        let member_ids: HashSet<_> = keys
            .kvs()
            .iter()
            .map(|kv| String::from_utf8_lossy(kv.value()).to_string())
            .collect();

        let members = match self.leader().await? {
            Some(leader) => member_ids
                .into_iter()
                .map(|id| {
                    let is_leader = id == leader.id;
                    ElectionMember { id, is_leader }
                })
                .collect(),
            None => member_ids
                .into_iter()
                .map(|id| ElectionMember {
                    id,
                    is_leader: false,
                })
                .collect(),
        };

        Ok(members)
    }

    fn id(&self) -> MetaResult<String> {
        Ok(self.id.clone())
    }

    fn subscribe(&self) -> Receiver<bool> {
        self.is_leader_sender.subscribe()
    }
}

impl EtcdElectionClient {
    pub(crate) async fn new(
        endpoints: Vec<String>,
        options: Option<ConnectOptions>,
        auth_enabled: bool,
        id: String,
    ) -> MetaResult<Self> {
        let (sender, _) = watch::channel(false);

        let client = WrappedEtcdClient::connect(endpoints, options, auth_enabled).await?;

        Ok(Self {
            id,
            is_leader_sender: sender,
            client,
        })
    }
}

#[cfg(madsim)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use etcd_client::GetOptions;
    use itertools::Itertools;
    use tokio::sync::watch;
    use tokio::sync::watch::Sender;
    use tokio::time;

    use crate::rpc::election_client::{ElectionClient, EtcdElectionClient, META_ELECTION_KEY};

    type ElectionHandle = (Sender<()>, Arc<dyn ElectionClient>);

    #[tokio::test]
    async fn test_election() {
        let clients = prepare_election_client(3, 5).await;

        time::sleep(Duration::from_secs(10)).await;

        let (leaders, followers) = check_role(clients, 1, 2).await;

        let leader = leaders.into_iter().next().unwrap();

        // stop leader
        leader.0.send(()).unwrap();

        time::sleep(Duration::from_secs(10)).await;

        let (new_leaders, new_followers) = check_role(followers, 1, 1).await;

        let leader = new_leaders.into_iter().next().unwrap();
        let election_leader = leader.1.leader().await.unwrap().unwrap();

        assert_eq!(election_leader.id, leader.1.id().unwrap());

        let client = etcd_client::Client::connect(&vec!["localhost:2388"], None)
            .await
            .unwrap();

        let kvs = client
            .kv_client()
            .get(META_ELECTION_KEY, Some(GetOptions::new().with_prefix()))
            .await
            .unwrap();

        let leader_kv = kvs
            .kvs()
            .iter()
            .find(|kv| kv.value() == election_leader.id.as_bytes())
            .cloned()
            .unwrap();

        let lease_id = leader_kv.lease();

        client.lease_client().revoke(lease_id).await.unwrap();

        time::sleep(Duration::from_secs(10)).await;

        let leader = new_followers.into_iter().next().unwrap();

        assert!(leader.1.is_leader().await);
    }

    #[tokio::test]
    async fn test_resign() {
        // with a long ttl
        let clients = prepare_election_client(3, 1000).await;

        time::sleep(Duration::from_secs(10)).await;

        let (leaders, followers) = check_role(clients, 1, 2).await;

        let leader = leaders.into_iter().next().unwrap();

        // stop leader
        leader.0.send(()).unwrap();

        time::sleep(Duration::from_secs(10)).await;

        check_role(followers, 1, 1).await;
    }

    async fn check_role(
        clients: Vec<ElectionHandle>,
        expected_leader_count: usize,
        expected_follower_count: usize,
    ) -> (Vec<ElectionHandle>, Vec<ElectionHandle>) {
        let mut leaders = vec![];
        let mut followers = vec![];
        for (sender, client) in clients {
            if client.is_leader().await {
                leaders.push((sender, client));
            } else {
                followers.push((sender, client));
            }
        }

        assert_eq!(leaders.len(), expected_leader_count);
        assert_eq!(followers.len(), expected_follower_count);
        (leaders, followers)
    }

    async fn prepare_election_client(
        count: i32,
        ttl: i64,
    ) -> Vec<(Sender<()>, Arc<dyn ElectionClient>)> {
        let handle = tokio::spawn(async move {
            let addr = "0.0.0.0:2388".parse().unwrap();
            let mut builder = etcd_client::SimServer::builder();
            builder.serve(addr).await.unwrap();
        });

        let mut clients: Vec<(watch::Sender<()>, Arc<dyn ElectionClient>)> = vec![];

        for i in 0..count {
            let (stop_sender, stop_receiver) = watch::channel(());
            clients.push((
                stop_sender,
                Arc::new(
                    EtcdElectionClient::new(
                        vec!["localhost:2388".to_string()],
                        None,
                        false,
                        format!("client_{}", i).to_string(),
                    )
                    .await
                    .unwrap(),
                ),
            ));
        }

        for client in &clients {
            assert!(!client.1.is_leader().await);
        }

        for (stop_sender, client) in &clients {
            let client_ = client.clone();
            let stop = stop_sender.subscribe();

            tokio::spawn(async move {
                let mut ticker = time::interval(Duration::from_secs(1));
                loop {
                    ticker.tick().await;
                    if let Ok(_) = client_.run_once(ttl, stop.clone()).await {
                        break;
                    }
                }
            });
        }

        clients
    }
}
