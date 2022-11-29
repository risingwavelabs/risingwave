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

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::meta::{MetaLeaderInfo, MetaLeaseInfo};
use tokio::sync::oneshot::Sender;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;

use crate::rpc::{META_CF_NAME, META_LEADER_KEY, META_LEASE_KEY};
use crate::storage::{MetaStore, MetaStoreError, Transaction};
use crate::MetaResult;

// get duration since epoch
fn since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

/// Contains the outcome of an election
/// Use this to get information about the current leader and yourself
struct ElectionOutcome {
    pub meta_leader_info: MetaLeaderInfo,
    pub _meta_lease_info: MetaLeaseInfo,

    // True if current node is leader, else false
    pub is_leader: bool,
    pub host_addr: HostAddr,
}

/// Runs for election in an attempt to become leader
///
/// ## Returns
/// Returns `ElectionResult`, containing infos about the leader who won or
/// None if the election needs to be repeated
///
/// ## Arguments
/// `meta_store`: The meta store which holds the lease, deciding about the election outcome
/// `addr`: Address of the node that runs for election
/// `lease_time_sec`: Amount of seconds that this lease will be valid
/// `next_lease_id`: If the node wins, the lease used until the next election will have this id
async fn campaign<S: MetaStore>(
    meta_store: &Arc<S>,
    addr: &String,
    lease_time_sec: u64,
    next_lease_id: u64,
) -> Option<ElectionOutcome> {
    tracing::info!("running for election with lease {}", next_lease_id);

    // below is old code
    // get old leader info and lease
    let current_leader_info = match get_infos(meta_store).await {
        None => return None,
        Some(infos) => {
            let (leader, _) = infos;
            leader
        }
    };

    let leader_info = MetaLeaderInfo {
        lease_id: next_lease_id,
        node_address: addr.to_string(),
    };

    let now = since_epoch();
    let lease_info = MetaLeaseInfo {
        leader: Some(leader_info.clone()),
        lease_register_time: now.as_secs(),
        lease_expire_time: now.as_secs() + lease_time_sec,
    };

    // Initial leader election
    if current_leader_info.is_empty() {
        tracing::info!("We have no leader");

        // cluster has no leader
        if let Err(e) = meta_store
            .put_cf(
                META_CF_NAME,
                META_LEADER_KEY.as_bytes().to_vec(),
                leader_info.encode_to_vec(),
            )
            .await
        {
            tracing::warn!(
                "new cluster put leader info failed, MetaStoreError: {:?}",
                e
            );
            return None;
        }

        let mut addr_port = addr.split(":");
        // Check if new leader was elected in the meantime
        return match renew_lease(&leader_info, lease_time_sec, meta_store).await {
            Some(is_leader) => {
                if !is_leader {
                    return None;
                }
                Some(ElectionOutcome {
                    meta_leader_info: leader_info,
                    _meta_lease_info: lease_info,
                    is_leader: true,
                    host_addr: HostAddr {
                        host: addr_port.next().unwrap().to_owned(),
                        port: addr_port.next().unwrap().to_owned().parse::<u16>().unwrap(),
                    },
                })
            }
            None => None,
        };
    }

    // follow-up elections: There have already been leaders before
    let is_leader = match renew_lease(&leader_info, lease_time_sec, meta_store).await {
        None => return None,
        Some(val) => val,
    };

    if is_leader {
        // if is leader, return HostAddress to this node
        let mut addr_port = addr.split(":");
        return Some(ElectionOutcome {
            meta_leader_info: leader_info,
            _meta_lease_info: lease_info,
            is_leader,
            host_addr: HostAddr {
                host: addr_port.next().unwrap().to_owned(),
                port: addr_port.next().unwrap().to_owned().parse::<u16>().unwrap(),
            },
        });
    }

    // if it is not leader, then get the current leaders HostAddress

    // TODO: Can I get the infos here or do I have to get these in
    // one transaction when I call renew_lease?
    let (leader, _) = get_infos_obj(meta_store).await?;
    let mut addr_port = leader.get_node_address().split(":");

    Some(ElectionOutcome {
        meta_leader_info: leader_info,
        _meta_lease_info: lease_info,
        is_leader,
        host_addr: HostAddr {
            // TODO: write MetaLeaderInfo to HostAddr into func
            host: addr_port.next().unwrap().to_owned(),
            port: addr_port.next().unwrap().to_owned().parse::<u16>().unwrap(),
        },
    })
}

/// Try to renew/acquire the leader lease
///
/// ## Returns
/// True, if the current node could acquire/renew the lease
/// False, if the current node could acquire/renew the lease
/// None, if the operation failed
///
/// ## Arguments
/// `leader_info`: Info of the node that tries to acquire/renew the lease
/// `lease_time_sec`: Time for which the lease should be extended
/// `meta_store`: Store which holds the lease
///
/// Returns true if node was leader and was able to renew/acquire the lease
/// Returns false if node was follower and thus could not renew/acquire lease
/// Returns None if operation has to be repeated
async fn renew_lease<S: MetaStore>(
    leader_info: &MetaLeaderInfo,
    lease_time_sec: u64,
    meta_store: &Arc<S>,
) -> Option<bool> {
    // does this function work?
    let now = since_epoch();
    let mut txn = Transaction::default();
    let lease_info = MetaLeaseInfo {
        leader: Some(leader_info.clone()),
        lease_register_time: now.as_secs(),
        lease_expire_time: now.as_secs() + lease_time_sec,
    };

    txn.check_equal(
        META_CF_NAME.to_string(),
        META_LEADER_KEY.as_bytes().to_vec(),
        leader_info.encode_to_vec(), // will not work, because leader_info differs
    );
    txn.put(
        META_CF_NAME.to_string(),
        META_LEASE_KEY.as_bytes().to_vec(),
        lease_info.encode_to_vec(),
    );

    // TODO: transaction get the current leader
    let is_leader = match meta_store.txn(txn).await {
        Err(e) => match e {
            MetaStoreError::TransactionAbort() => false,
            MetaStoreError::Internal(e) => {
                tracing::warn!(
                    "Renew/acquire lease: try again later, MetaStoreError: {:?}",
                    e
                );
                return None;
            }
            MetaStoreError::ItemNotFound(e) => {
                tracing::warn!("Renew/acquire lease: MetaStoreError: {:?}", e);
                return None;
            }
        },
        Ok(_) => true,
    };
    Some(is_leader)
}

type MetaLeaderInfoVec = Vec<u8>;
type MetaLeaseInfoVec = Vec<u8>;

/// Retrieve infos about the current leader
///
/// ## Returns
/// Returns a tuple containing information about the Leader and the Leader lease
/// If there was never a leader elected or no lease is found this will return an empty vector
/// Returns None if the operation failed
///
/// ## Attributes:
/// `meta_store`: The store holding information about the leader
async fn get_infos<S: MetaStore>(
    meta_store: &Arc<S>,
) -> Option<(MetaLeaderInfoVec, MetaLeaseInfoVec)> {
    let current_leader_info = match meta_store
        .get_cf(META_CF_NAME, META_LEADER_KEY.as_bytes())
        .await
    {
        Err(MetaStoreError::ItemNotFound(_)) => vec![],
        Ok(v) => v,
        _ => return None,
    };
    let current_leader_lease = match meta_store
        .get_cf(META_CF_NAME, META_LEASE_KEY.as_bytes())
        .await
    {
        Err(MetaStoreError::ItemNotFound(_)) => vec![],
        Ok(v) => v,
        _ => return None,
    };
    Some((current_leader_info, current_leader_lease))
}

/// Retrieve infos about the current leader
/// Wrapper for get_infos
///
/// ## Returns
/// None on error, else infos about the leader
async fn get_infos_obj<S: MetaStore>(
    meta_store: &Arc<S>,
) -> Option<(MetaLeaderInfo, MetaLeaseInfo)> {
    match get_infos(meta_store).await {
        None => return None,
        Some(infos) => {
            let (leader, lease) = infos;
            return Some((
                MetaLeaderInfo::decode(&mut leader.as_slice()).unwrap(),
                MetaLeaseInfo::decode(&mut lease.as_slice()).unwrap(),
            ));
        }
    }
}

// TODO: Implement retry logic for get_infos

fn gen_rand_lease_id() -> u64 {
    rand::thread_rng().gen_range(0..std::u64::MAX)
}

/// Used to manage single leader setup. `run_elections` will continuously run elections to determine
/// which nodes are **leaders** and which are **followers**.
///
/// To become a leader a **follower** node **campaigns**. A follower only ever campaigns if it
/// detects that the current leader is down. The follower becomes a leader by acquiring a lease
/// from the **meta store**. After getting elected the new node will start its **term** as a leader.
/// A term lasts until the current leader crashes.   
///
/// ## Arguments
/// `addr`: Address of the current leader, e.g. "127.0.0.1"
/// `meta_store`: Holds information about the leader
/// `lease_time_sec`: Time that a lease will be valid for.
/// A large value reduces the meta store traffic. A small value reduces the downtime during failover
///
/// ## Returns:
/// `MetaLeaderInfo` containing the leader who got initially elected
/// `JoinHandle` running all future elections concurrently
/// `Sender` for signaling a shutdown
/// `Receiver` receiving true if this node got elected as leader and false if it is a follower
pub async fn run_elections<S: MetaStore>(
    addr: String,
    meta_store: Arc<S>,
    lease_time_sec: u64,
) -> MetaResult<(
    MetaLeaderInfo,
    JoinHandle<()>,
    Sender<()>,
    Receiver<(HostAddr, bool)>,
)> {
    // Randomize interval to reduce mitigate likelihood of simultaneous requests
    let mut rng: StdRng = SeedableRng::from_entropy();
    let mut ticker = tokio::time::interval(
        Duration::from_secs(lease_time_sec) + Duration::from_millis(rng.gen_range(0..500)),
    );

    // runs the initial election, determining who the first leader is
    'initial_election: loop {
        ticker.tick().await;

        // every lease gets a random ID to differentiate between leases/leaders
        let mut initial_election = true;
        let init_lease_id = gen_rand_lease_id();

        // run the initial election
        let election_outcome = campaign(&meta_store, &addr, lease_time_sec, init_lease_id).await;
        let (initial_leader, is_initial_leader) = match election_outcome {
            Some(outcome) => {
                tracing::info!("initial election finished");
                (outcome.meta_leader_info, outcome.is_leader)
            }
            None => {
                tracing::info!("initial election failed. Repeating election");
                continue 'initial_election;
            }
        };
        if is_initial_leader {
            tracing::info!(
                "Initial leader with address '{}' elected. New lease id is {}",
                initial_leader.node_address,
                initial_leader.lease_id
            );
        }

        let initial_leader_clone = initial_leader.clone();

        // define all follow up elections and terms in handle
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let (leader_tx, leader_rx) = tokio::sync::watch::channel((
            HostAddr {
                host: "127.0.0.1".to_owned(), // TODO: add implementation
                port: 123,
            },
            is_initial_leader,
        ));
        let handle = tokio::spawn(async move {
            // runs all follow-up elections
            let mut wait = true;
            'election: loop {
                if wait {
                    tokio::select! {
                        _ = &mut shutdown_rx => {
                            tracing::info!("Register leader info is stopped");
                            return;
                        }
                        _ = ticker.tick() => {},
                    }
                }
                wait = true;

                // TODO: write MetaLeaderInfo to HostAddr function
                // TODO: What is the difference between HostAddr and HostAddress?  Do we need both?

                // Do not elect new leader directly after running the initial election
                let mut is_leader = is_initial_leader;
                let mut leader_info = initial_leader.clone();
                let mut addr_port = initial_leader.get_node_address().split(":");
                let mut leader_addr = HostAddr {
                    host: addr_port.next().unwrap().to_owned(),
                    port: addr_port.next().unwrap().to_owned().parse::<u16>().unwrap(),
                };
                if !initial_election {
                    let (l_addr, l_info, is_l) =
                        match campaign(&meta_store, &addr, lease_time_sec, gen_rand_lease_id())
                            .await
                        {
                            None => {
                                tracing::info!("election failed. Repeating election");
                                continue 'election;
                            }
                            Some(outcome) => {
                                tracing::info!("election finished");
                                (
                                    outcome.host_addr,
                                    outcome.meta_leader_info,
                                    outcome.is_leader,
                                )
                            }
                        };

                    if is_l {
                        tracing::info!(
                            "Leader with address '{}' elected. New lease id is {}",
                            l_info.node_address,
                            l_info.lease_id
                        );
                    }
                    leader_info = l_info;
                    is_leader = is_l;
                    leader_addr = l_addr;
                }
                initial_election = false;

                // signal to observers if there is a change in leadership
                loop {
                    if let Err(err) = leader_tx.send((leader_addr.clone(), is_leader)) {
                        tracing::info!("Error when sending leader update: {}", err);
                        ticker.tick().await;
                        continue;
                    }
                    break;
                }

                // election done. Enter the term of the current leader
                // Leader stays in power until leader crashes
                '_term: loop {
                    // sleep OR abort if shutdown
                    tokio::select! {
                        _ = &mut shutdown_rx => {
                            tracing::info!("Register leader info is stopped");
                            return;
                        }
                        _ = ticker.tick() => {},
                    }

                    if let Some(leader_alive) =
                        manage_term(&leader_info, lease_time_sec, &meta_store).await
                    {
                        if !leader_alive {
                            // leader failed. Immediately elect new leader
                            wait = false;
                            continue 'election;
                        }
                    }
                }
            }
        });
        return Ok((initial_leader_clone, handle, shutdown_tx, leader_rx));
    }
}

/// Acts on the current leaders term
/// Leaders will try to extend the term
/// Followers will check if the leader is still alive
///
/// ## Returns
/// True if the leader is still in power
/// False if the leader failed
/// None if there was an error
async fn manage_term<S: MetaStore>(
    leader_info: &MetaLeaderInfo,
    lease_time_sec: u64,
    meta_store: &Arc<S>,
) -> Option<bool> {
    // try to renew/acquire the lease
    match renew_lease(leader_info, lease_time_sec, meta_store).await {
        None => return Some(false),
        Some(val) => {
            if val {
                return None; // node is leader and lease was renewed
            }
        }
    };
    // node is follower

    // get leader info
    let (_, lease_info) = get_infos(meta_store).await.unwrap_or_default();
    if lease_info.is_empty() {
        // ETCD does not have leader lease. Elect new leader
        tracing::info!("ETCD does not have leader lease. Running new election");
        return Some(false);
    }

    // delete lease and run new election if lease is expired for some time
    let some_time = lease_time_sec / 2;
    let lease_info = MetaLeaseInfo::decode(&mut lease_info.as_slice()).unwrap();
    if lease_info.get_lease_expire_time() + some_time < since_epoch().as_secs() {
        tracing::warn!("Detected that leader is down");
        let mut txn = Transaction::default();
        txn.delete(
            META_CF_NAME.to_string(),
            META_LEADER_KEY.as_bytes().to_vec(),
        );
        txn.delete(META_CF_NAME.to_string(), META_LEASE_KEY.as_bytes().to_vec());
        match meta_store.txn(txn).await {
            Err(e) => tracing::warn!("Unable to update lease. Error {}", e),
            Ok(_) => tracing::info!("Deleted leader and lease"),
        }
        return Some(false);
    }
    // lease exists and leader continues term
    Some(true)
}
