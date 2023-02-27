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

use etcd_client::{Client, ConnectOptions, Error, GetOptions};
use tokio::sync::watch::Receiver;
use tokio::sync::{oneshot, watch};
use tokio::time;
use tokio_stream::StreamExt;

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
    endpoints: Vec<String>,
    options: Option<ConnectOptions>,
}

#[async_trait::async_trait]
impl ElectionClient for EtcdElectionClient {
    async fn is_leader(&self) -> bool {
        *self.is_leader_sender.borrow()
    }

    async fn leader(&self) -> MetaResult<Option<ElectionMember>> {
        let mut election_client = self.client().await?.election_client();
        let leader = election_client.leader(META_ELECTION_KEY).await;

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
        let client = self.client().await?;
        let mut lease_client = client.lease_client();
        let mut election_client = client.election_client();
        let mut stop = stop;

        tracing::info!("client {} start election", self.id);

        // is restored leader from previous session?
        let mut restored_leader = false;

        let mut lease_id = match election_client
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
            Ok(_) => lease_client.grant(ttl, None).await.map(|resp| resp.id()),

            // no leader
            Err(Error::GRpcStatus(e)) if e.message() == "election: no leader" => {
                lease_client.grant(ttl, None).await.map(|resp| resp.id())
            }

            // connection error
            Err(e) => Err(e),
        }?;

        tracing::info!("use lease id {}", lease_id);

        // try keep alive
        let (mut keeper, mut resp_stream) = lease_client.keep_alive(lease_id).await?;
        let _resp = keeper.keep_alive().await?;
        let resp = resp_stream.message().await?;
        if let Some(resp) = resp && resp.ttl() <= 0 {
            tracing::info!("lease {} expired or revoked, re-granting", lease_id);
            if restored_leader {
                tracing::info!("restored leader lease {} lost", lease_id);
                restored_leader = false;
            }
            // renew lease_id
            lease_id = lease_client.grant(ttl, None).await.map(|resp| resp.id())?;
            tracing::info!("lease {} re-granted", lease_id);
        }

        let (keep_alive_fail_tx, mut keep_alive_fail_rx) = oneshot::channel();

        let mut lease_client = client.lease_client();

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

            // timeout controller, when keep alive fails for more than a certain period of time
            // before it is considered a complete failure
            let mut timeout = time::interval(Duration::from_secs((ttl / 2) as u64));
            timeout.reset();

            loop {
                tokio::select! {
                    biased;

                    _ = timeout.tick() => {
                        tracing::warn!("lease {} keep alive timeout", lease_id);
                        keep_alive_fail_tx.send(()).unwrap();
                        break;
                    }

                    _ = ticker.tick() => {
                        if let Err(err) = keeper.keep_alive().await {
                            tracing::warn!("keep alive for lease {} failed {}", lease_id, err);
                            continue
                        }

                        match resp_stream.message().await {
                            Ok(Some(resp)) => {
                                if resp.ttl() <= 0 {
                                    tracing::warn!("lease expired or revoked {}", lease_id);
                                    keep_alive_fail_tx.send(()).unwrap();
                                    break;
                                }

                                timeout.reset();
                            },
                            Ok(None) => {
                                tracing::warn!("lease keeper for lease {} response stream closed unexpected", lease_id);

                                // try to re-create lease keeper, with timeout as ttl / 2
                                if let Ok(Ok((keeper_, resp_stream_))) = time::timeout(Duration::from_secs((ttl / 2) as u64), lease_client.keep_alive(lease_id)).await {
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
                        tracing::info!("stop signal received");
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
            tokio::select! {
                biased;

                _ = stop.changed() => {
                    tracing::info!("stop signal received");
                    return Ok(());
                }

                campaign_resp = election_client.campaign(META_ELECTION_KEY, self.id.as_bytes().to_vec(), lease_id) => {
                    campaign_resp?;
                    tracing::info!("client {} wins election {}", self.id, META_ELECTION_KEY);
                }
            };
        }

        self.is_leader_sender.send_replace(true);

        let mut observe_stream = election_client.observe(META_ELECTION_KEY).await?;

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
                        None => {
                            tracing::warn!("observe stream closed unexpected, recreating");

                            // try to re-create observe stream, with timeout as ttl / 2
                            if let Ok(Ok(stream)) = time::timeout(Duration::from_secs((ttl / 2) as u64), election_client.observe(META_ELECTION_KEY)).await {
                                observe_stream = stream;
                                tracing::info!("recreating observe stream");
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
        let mut client = self.client().await?.kv_client();
        let keys = client
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
    pub(crate) fn new(endpoints: Vec<String>, options: Option<ConnectOptions>, id: String) -> Self {
        let (sender, _) = watch::channel(false);
        Self {
            endpoints,
            options,
            id,
            is_leader_sender: sender,
        }
    }

    async fn client(&self) -> MetaResult<Client> {
        let client = Client::connect(self.endpoints.clone(), self.options.clone()).await?;
        Ok(client)
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
    use tokio::time;

    use crate::rpc::election_client::{ElectionClient, EtcdElectionClient, META_ELECTION_KEY};

    #[tokio::test]
    async fn test_election() {
        let handle = tokio::spawn(async move {
            let addr = "0.0.0.0:2388".parse().unwrap();
            let mut builder = etcd_client::SimServer::builder();
            builder.serve(addr).await;
        });

        let mut clients: Vec<(watch::Sender<()>, Arc<dyn ElectionClient>)> = vec![];

        for i in 0..3 {
            let (stop_sender, stop_receiver) = watch::channel(());
            clients.push((
                stop_sender,
                Arc::new(EtcdElectionClient::new(
                    vec!["localhost:2388".to_string()],
                    None,
                    format!("client_{}", i).to_string(),
                )),
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
                    if let Ok(_) = client_.run_once(3, stop.clone()).await {
                        break;
                    }
                }
            });
        }

        time::sleep(Duration::from_secs(10)).await;

        let mut leaders = vec![];
        let mut followers = vec![];
        for (sender, client) in &clients {
            if client.is_leader().await {
                leaders.push((sender, client));
            } else {
                followers.push((sender, client));
            }
        }

        assert_eq!(leaders.len(), 1);
        assert_eq!(followers.len(), 2);

        let leader = leaders.into_iter().next().unwrap();

        // stop leader
        leader.0.send(()).unwrap();

        time::sleep(Duration::from_secs(10)).await;

        let mut new_leaders = vec![];
        let mut new_followers = vec![];
        for (sender, client) in followers {
            if client.is_leader().await {
                new_leaders.push((sender, client));
            } else {
                new_followers.push((sender, client));
            }
        }

        assert_eq!(new_leaders.len(), 1);
        assert_eq!(new_followers.len(), 1);

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
}
