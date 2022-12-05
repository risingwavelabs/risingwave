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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{process, thread};

use prost::Message;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use risingwave_pb::meta::{MetaLeaderInfo, MetaLeaseInfo};
use tokio::sync::oneshot::Sender;
use tokio::sync::watch::Receiver as WatchReceiver;
use tokio::task::JoinHandle;

use crate::rpc::{META_CF_NAME, META_LEADER_KEY, META_LEASE_KEY};
use crate::storage::{MetaStore, MetaStoreError, Transaction};
use crate::{MetaError, MetaResult};

// get duration since epoch
fn since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
}

/// Contains the result of an election
/// Use this to get information about the current leader and yourself
struct ElectionResult {
    pub meta_leader_info: MetaLeaderInfo,
    pub _meta_lease_info: MetaLeaseInfo,

    // True if current node is leader. False if follower
    pub is_leader: bool,
}

/// Runs for election in an attempt to become leader
///
/// ## Arguments
/// `meta_store`: The meta store which holds the lease, deciding about the election result
/// `addr`: Address of the node that runs for election
/// `lease_time_sec`: Amount of seconds that this lease will be valid
/// `next_lease_id`: If the node wins, the lease used until the next election will have this id
///
/// ## Returns
/// Returns `ElectionResult`, containing infos about the node who won the election or
/// `MetaError` if the election ran into an error
async fn campaign<S: MetaStore>(
    meta_store: &Arc<S>,
    addr: &String,
    lease_time_sec: u64,
    next_lease_id: u64,
) -> MetaResult<ElectionResult> {
    tracing::info!("running for election with lease {}", next_lease_id);

    let campaign_leader_info = MetaLeaderInfo {
        lease_id: next_lease_id,
        node_address: addr.to_string(),
    };

    let now = since_epoch();
    let campaign_lease_info = MetaLeaseInfo {
        leader: Some(campaign_leader_info.clone()),
        lease_register_time: now.as_secs(),
        lease_expire_time: now.as_secs() + lease_time_sec,
    };

    // get old leader info and lease
    let get_infos_result = get_leader_lease_obj(meta_store).await;
    let has_leader = get_infos_result.is_some();

    // Delete leader info, if leader lease timed out
    let lease_expired = if has_leader {
        let some_time = lease_time_sec / 2;
        let (_, lease_info) = get_infos_result.unwrap();
        lease_info.get_lease_expire_time() + some_time < since_epoch().as_secs()
    } else {
        false
    };

    // Leader is down
    if !has_leader || lease_expired {
        tracing::info!("We have no leader");

        // cluster has no leader
        if let Err(e) = meta_store
            .put_cf(
                META_CF_NAME,
                META_LEADER_KEY.as_bytes().to_vec(),
                campaign_leader_info.encode_to_vec(),
            )
            .await
        {
            let msg = format!(
                "new cluster put leader info failed, MetaStoreError: {:?}",
                e
            );
            tracing::warn!(msg);
            return Err(MetaError::unavailable(msg));
        }

        // Check if new leader was elected in the meantime
        return match renew_lease(&campaign_leader_info, lease_time_sec, meta_store).await {
            Ok(is_leader) => {
                if !is_leader {
                    return Err(MetaError::permission_denied(
                        "Node could not acquire/renew leader lease".into(),
                    ));
                }
                Ok(ElectionResult {
                    meta_leader_info: campaign_leader_info,
                    _meta_lease_info: campaign_lease_info,
                    is_leader: true,
                })
            }
            Err(e) => Err(e),
        };
    }

    // follow-up elections: There have already been leaders before
    let is_leader = match renew_lease(&campaign_leader_info, lease_time_sec, meta_store).await {
        Err(e) => {
            tracing::warn!("Encountered error when renewing lease {}", e);
            return Err(e);
        }
        Ok(val) => val,
    };

    if is_leader {
        // if is leader, return HostAddress to this node
        return Ok(ElectionResult {
            meta_leader_info: campaign_leader_info,
            _meta_lease_info: campaign_lease_info,
            is_leader,
        });
    }

    // FIXME: This has to be done with a single transaction, not 2
    // if it is not leader, then get the current leaders HostAddress
    // Ask Pin how to implement txn.get here
    return match get_leader_lease_obj(meta_store).await {
        None => Err(MetaError::unavailable(
            "Meta information not stored in meta store".into(),
        )),
        Some(infos) => Ok(ElectionResult {
            meta_leader_info: infos.0,
            _meta_lease_info: infos.1,
            is_leader,
        }),
    };
}

/// Try to renew/acquire the leader lease
///
///
/// ## Arguments
/// `leader_info`: Info of the node that trie
/// `lease_time_sec`: Time in seconds that the lease is valid
/// `meta_store`: Store which holds the lease#
///
/// ## Returns
/// True if node was leader and was able to renew/acquire the lease.
/// False if node was follower and thus could not renew/acquire lease.
/// `MetaError` if operation ran into an error
async fn renew_lease<S: MetaStore>(
    leader_info: &MetaLeaderInfo,
    lease_time_sec: u64,
    meta_store: &Arc<S>,
) -> MetaResult<bool> {
    let now = since_epoch();
    let lease_info = MetaLeaseInfo {
        leader: Some(leader_info.clone()),
        lease_register_time: now.as_secs(),
        lease_expire_time: now.as_secs() + lease_time_sec,
    };

    let mut txn = Transaction::default();
    txn.check_equal(
        META_CF_NAME.to_string(),
        META_LEADER_KEY.as_bytes().to_vec(),
        leader_info.encode_to_vec(),
    );
    txn.put(
        META_CF_NAME.to_string(),
        META_LEASE_KEY.as_bytes().to_vec(),
        lease_info.encode_to_vec(),
    );

    let is_leader = match meta_store.txn(txn).await {
        Err(e) => match e {
            MetaStoreError::TransactionAbort() => false,
            e => return Err(e.into()),
        },
        Ok(_) => true,
    };
    Ok(is_leader)
}

/// Retrieve infos about the current leader
///
/// ## Arguments:
/// `meta_store`: The store holding information about the leader
///
/// ## Returns
/// None if leader OR lease is not present in store
/// else infos about the leader and lease
/// Panics if request against `meta_store` failed
async fn get_leader_lease_obj<S: MetaStore>(
    meta_store: &Arc<S>,
) -> Option<(MetaLeaderInfo, MetaLeaseInfo)> {
    let current_leader_info = MetaLeaderInfo::decode(
        match meta_store
            .get_cf(META_CF_NAME, META_LEADER_KEY.as_bytes())
            .await
        {
            Err(MetaStoreError::ItemNotFound(_)) => return None,
            Err(e) => panic!("Meta Store Error when retrieving leader info {:?}", e),
            Ok(v) => v,
        }
        .as_slice(),
    )
    .unwrap();

    let current_leader_lease = MetaLeaseInfo::decode(
        match meta_store
            .get_cf(META_CF_NAME, META_LEASE_KEY.as_bytes())
            .await
        {
            Err(MetaStoreError::ItemNotFound(_)) => return None,
            Err(e) => panic!("Meta Store Error when retrieving lease info {:?}", e),
            Ok(v) => v,
        }
        .as_slice(),
    )
    .unwrap();

    Some((current_leader_info, current_leader_lease))
}

fn gen_rand_lease_id(addr: &str) -> u64 {
    let mut ds = DefaultHasher::new();
    addr.hash(&mut ds);
    ds.finish()
    // FIXME: We are unable to use a random lease at the moment
    // During testing, meta gets killed, new meta starts
    // meta detects that lease is still there, with same addr, but diff ID
    // meta believes that leader is out there and becomes follower
    // IMHO we can only use random lease id, if we have at least 2 meta nodes
    // https://github.com/risingwavelabs/risingwave/issues/6844
    // rand::thread_rng().gen_range(0..std::u64::MAX)
}

/// Used to manage single leader setup. `run_elections` will continuously run elections to determine
/// which nodes is the **leader** and which are **followers**.
///
/// To become a leader a **follower** node **campaigns**. A follower only ever campaigns if it
/// detects that the current leader is down. The follower becomes a leader by acquiring a lease
/// from the **meta store**. After getting elected the new node will start its **term** as a leader.
/// A term lasts until the current leader crashes.   
///
/// ## Arguments
/// `addr`: Address of the current leader, e.g. "127.0.0.1:5690".
/// `meta_store`: Holds information about the leader.
/// `lease_time_sec`: Time in seconds that a lease will be valid for.
/// A large value reduces the meta store traffic. A small value reduces the downtime during failover
///
/// ## Returns:
/// `MetaLeaderInfo` containing the leader who got elected initially.
/// `JoinHandle` running all future elections concurrently.
/// `Sender` for signaling a shutdown.
/// `Receiver` receiving true if this node got elected as leader and false if it is a follower.
pub async fn run_elections<S: MetaStore>(
    addr: String,
    meta_store: Arc<S>,
    lease_time_sec: u64,
) -> MetaResult<(
    MetaLeaderInfo,
    JoinHandle<()>,
    Sender<()>,
    WatchReceiver<(MetaLeaderInfo, bool)>,
)> {
    // Randomize interval to reduce mitigate likelihood of simultaneous requests
    let mut rng: StdRng = SeedableRng::from_entropy();

    // runs the initial election, determining who the first leader is
    'initial_election: loop {
        // every lease gets a random ID to differentiate between leases/leaders
        let mut initial_election = true;

        // run the initial election
        let election_result = campaign(
            &meta_store,
            &addr,
            lease_time_sec,
            gen_rand_lease_id(addr.as_str()),
        )
        .await;
        let (initial_leader, is_initial_leader) = match election_result {
            Ok(elect_result) => {
                tracing::info!("initial election finished");
                (elect_result.meta_leader_info, elect_result.is_leader)
            }
            Err(_) => {
                tracing::info!("initial election failed. Repeating election");
                thread::sleep(std::time::Duration::from_millis(500));
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
        let (leader_tx, leader_rx) = tokio::sync::watch::channel(Some(initial_leader.clone()));
        let handle = tokio::spawn(async move {
            // runs all followup elections
            let mut ticker = tokio::time::interval(Duration::from_millis(
                lease_time_sec * 500 + rng.gen_range(1..500),
            ));
            ticker.reset();

            let mut is_leader = is_initial_leader;
            let mut leader_info = initial_leader.clone();
            'election: loop {
                // Do not elect new leader directly after running the initial election
                if !initial_election {
                    let (leader_info_, is_leader_) = match campaign(
                        &meta_store,
                        &addr,
                        lease_time_sec,
                        gen_rand_lease_id(addr.as_str()),
                    )
                    .await
                    {
                        Err(_) => {
                            tracing::info!("election failed. Repeating election");
                            _ = ticker.tick().await;
                            continue 'election;
                        }
                        Ok(elect_result) => {
                            tracing::info!("election finished");
                            (elect_result.meta_leader_info, elect_result.is_leader)
                        }
                    };

                    if is_leader_ {
                        tracing::info!(
                            "Leader with address '{}' elected. New lease id is {}",
                            leader_info_.node_address,
                            leader_info_.lease_id
                        );
                    }
                    leader_info = leader_info_;
                    is_leader = is_leader_;
                }
                initial_election = false;

                // signal to observers if there is a change in leadership
                leader_tx
                    .send(Some(leader_info.clone()))
                    .expect("Leader receiver dropped");

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

                    if let Ok(leader_alive) =
                        manage_term(is_leader, &leader_info, lease_time_sec, &meta_store).await && !leader_alive {
                            // Leader lost leadership. Trigger fencing
                            if is_leader {
                                tracing::error!("This node lost its leadership. Exiting node");
                                process::exit(0);
                            }
                            // leader failed. Elect new leader
                            continue 'election;
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
/// ## Arguments:
/// `is_leader`: True if this node currently is a leader
/// `leader_info`: Info about the last observed leader
/// `lease_time_sec`: Time in seconds that a lease is valid
/// `meta_store`: Holds lease and leader data
///
/// ## Returns
/// True if the leader defined in `leader_info` is still in power.
/// False if the old leader failed, there is no leader, or there a new leader got elected
/// `MetaError` if there was an error.
async fn manage_term<S: MetaStore>(
    is_leader: bool,
    leader_info: &MetaLeaderInfo,
    lease_time_sec: u64,
    meta_store: &Arc<S>,
) -> MetaResult<bool> {
    // try to renew/acquire the lease if this node is a leader
    if is_leader {
        return Ok(renew_lease(leader_info, lease_time_sec, meta_store)
            .await
            .unwrap_or(false));
    };

    // get leader info
    let leader_lease_result = get_leader_lease_obj(meta_store).await;
    let has_leader = leader_lease_result.is_some();

    if !has_leader {
        // ETCD does not have leader lease. Elect new leader
        tracing::info!("ETCD does not have leader lease. Running new election");
        return Ok(false);
    }

    match leader_changed(leader_info, meta_store).await {
        Err(e) => {
            tracing::warn!("Error when observing leader change {}", e);
            return Err(e);
        }
        Ok(has_new_leader) => {
            if has_new_leader {
                return Ok(false);
            }
        }
    }

    // delete lease and run new election if lease is expired for some time
    let some_time = lease_time_sec / 2;
    let (_, lease_info) = leader_lease_result.unwrap();
    if lease_info.get_lease_expire_time() + some_time < since_epoch().as_secs() {
        tracing::warn!("Detected that leader is down");
        let mut txn = Transaction::default();
        // FIXME: No deletion here, directly write new key
        txn.delete(
            META_CF_NAME.to_string(),
            META_LEADER_KEY.as_bytes().to_vec(),
        );
        txn.delete(META_CF_NAME.to_string(), META_LEASE_KEY.as_bytes().to_vec());
        match meta_store.txn(txn).await {
            Err(e) => tracing::warn!("Unable to update lease. Error {:?}", e),
            Ok(_) => tracing::info!("Deleted leader and lease"),
        }
        return Ok(false);
    }

    // lease exists and the same leader continues term
    Ok(true)
}

/// True if leader changed
/// False if leader is still the leader defined in `leader_info`
/// `MetaError` on error
async fn leader_changed<S: MetaStore>(
    leader_info: &MetaLeaderInfo,
    meta_store: &Arc<S>,
) -> MetaResult<bool> {
    let mut txn = Transaction::default();
    txn.check_equal(
        META_CF_NAME.to_string(),
        META_LEADER_KEY.as_bytes().to_vec(),
        leader_info.encode_to_vec(),
    );

    return match meta_store.txn(txn).await {
        Err(e) => match e {
            MetaStoreError::TransactionAbort() => Ok(true),
            e => return Err(e.into()),
        },
        Ok(_) => Ok(false),
    };
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_get_leader_lease_obj() {
        // no impfo present should give empty results or default objects
        let mock_meta_store = Arc::new(MemStore::new());
        let res = get_leader_lease_obj(&mock_meta_store).await;
        assert!(res.is_none());

        // get_info should retrieve old leader info
        let test_leader = MetaLeaderInfo {
            node_address: "some_address".into(),
            lease_id: 123,
        };
        let res = mock_meta_store
            .put_cf(
                META_CF_NAME,
                META_LEADER_KEY.as_bytes().to_vec(),
                test_leader.encode_to_vec(),
            )
            .await;
        assert!(res.is_ok(), "unable to send leader info to mock store");
        let info_res = get_leader_lease_obj(&mock_meta_store).await;
        assert!(
            info_res.is_none(),
            "get infos about leader should be none if either leader or lease is not set"
        );
        let res = mock_meta_store
            .put_cf(
                META_CF_NAME,
                META_LEASE_KEY.as_bytes().to_vec(),
                MetaLeaseInfo {
                    leader: Some(test_leader.clone()),
                    lease_register_time: since_epoch().as_secs(),
                    lease_expire_time: since_epoch().as_secs() + 1,
                }
                .encode_to_vec(),
            )
            .await;
        assert!(res.is_ok(), "unable to send lease info to mock store");
        let (leader, _) = get_leader_lease_obj(&mock_meta_store).await.unwrap();
        assert_eq!(
            leader, test_leader,
            "leader_info retrieved != leader_info send"
        );
    }

    async fn put_lease_info<S: MetaStore>(lease: &MetaLeaseInfo, meta_store: &Arc<S>) {
        let mut txn = Transaction::default();
        txn.put(
            META_CF_NAME.to_string(),
            META_LEASE_KEY.as_bytes().to_vec(),
            lease.encode_to_vec(),
        );
        meta_store
            .txn(txn)
            .await
            .expect("Putting test lease failed");
    }

    async fn put_leader_info<S: MetaStore>(leader: &MetaLeaderInfo, meta_store: &Arc<S>) {
        let mut txn = Transaction::default();
        txn.put(
            META_CF_NAME.to_string(),
            META_LEADER_KEY.as_bytes().to_vec(),
            leader.encode_to_vec(),
        );
        meta_store
            .txn(txn)
            .await
            .expect("Putting test lease failed");
    }

    async fn put_leader_lease<S: MetaStore>(
        leader: &MetaLeaderInfo,
        lease: &MetaLeaseInfo,
        meta_store: &Arc<S>,
    ) {
        put_leader_info(leader, meta_store).await;
        put_lease_info(lease, meta_store).await;
    }

    /// Default setup
    /// ## Returns:
    /// lease timeout, meta store, leader info, lease info, lease registration time
    async fn default_setup() -> (u64, Arc<MemStore>, MetaLeaderInfo, MetaLeaseInfo, Duration) {
        let lease_timeout = 10;
        let mock_meta_store = Arc::new(MemStore::new());
        let leader_info = MetaLeaderInfo {
            node_address: "localhost:1234".into(),
            lease_id: 123,
        };
        let now = since_epoch();
        let lease_info = MetaLeaseInfo {
            leader: Some(leader_info.clone()),
            lease_register_time: now.as_secs(),
            lease_expire_time: now.as_secs() + lease_timeout,
        };
        put_leader_lease(&leader_info, &lease_info, &mock_meta_store).await;
        (lease_timeout, mock_meta_store, leader_info, lease_info, now)
    }

    #[tokio::test]
    async fn test_manage_term() {
        let mock_meta_store = Arc::new(MemStore::new());
        let lease_timeout = 10;

        // Leader: If nobody was elected leader renewing lease fails and leader is marked as failed
        let leader_info = MetaLeaderInfo {
            node_address: "localhost:1234".into(),
            lease_id: 123,
        };
        assert!(
            !manage_term(true, &leader_info, lease_timeout, &mock_meta_store)
                .await
                .unwrap()
        );

        // Follower: If nobody was elected leader renewing lease also fails
        assert!(
            !manage_term(false, &leader_info, lease_timeout, &mock_meta_store)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn leader_should_renew_lease() {
        // if node is leader lease should be renewed
        let (lease_timeout, mock_meta_store, leader_info, _, _) = default_setup().await;
        let now = since_epoch();
        let lease_info = MetaLeaseInfo {
            leader: Some(leader_info.clone()),
            lease_register_time: now.as_secs(),
            lease_expire_time: now.as_secs() + lease_timeout,
        };
        put_leader_lease(&leader_info, &lease_info, &mock_meta_store).await;
        assert!(
            manage_term(true, &leader_info, lease_timeout, &mock_meta_store)
                .await
                .unwrap(),
            "Leader should still be in power after updating lease"
        );
        let (_, new_lease_info) = get_leader_lease_obj(&mock_meta_store).await.unwrap();
        assert_eq!(
            now.as_secs() + lease_timeout,
            new_lease_info.get_lease_expire_time(),
            "Lease was not extended by {}s, but by {}s",
            lease_timeout,
            new_lease_info.get_lease_expire_time() - lease_info.get_lease_expire_time()
        );
    }

    #[tokio::test]
    async fn follower_cannot_renew_lease() {
        // If node is follower, lease should not be renewed
        let (lease_timeout, mock_meta_store, leader_info, _, _) = default_setup().await;
        let now = since_epoch();
        let lease_info = MetaLeaseInfo {
            leader: Some(leader_info.clone()),
            lease_register_time: now.as_secs(),
            lease_expire_time: now.as_secs() + lease_timeout,
        };
        put_leader_lease(&leader_info, &lease_info, &mock_meta_store).await;
        assert!(
            manage_term(false, &leader_info, lease_timeout, &mock_meta_store)
                .await
                .unwrap(),
            "Leader should still be in power if follower fails to renew lease"
        );
        let (_, new_lease_info) = get_leader_lease_obj(&mock_meta_store).await.unwrap();
        assert_eq!(
            lease_info.get_lease_expire_time(),
            new_lease_info.get_lease_expire_time(),
            "Lease should not be extended by follower, but was extended by by {}s",
            new_lease_info.get_lease_expire_time() - lease_info.get_lease_expire_time()
        );
    }

    #[tokio::test]
    async fn not_renew_lease() {
        let (lease_timeout, mock_meta_store, ..) = default_setup().await;
        // Leader: If new leader was elected old leader should NOT renew lease
        let other_leader_info = MetaLeaderInfo {
            node_address: "other:1234".into(),
            lease_id: 456,
        };
        assert!(
            !manage_term(true, &other_leader_info, lease_timeout, &mock_meta_store)
                .await
                .unwrap(),
            "Leader: If new leader was elected old leader should NOT renew lease"
        );
        // Follower: If new leader was, start election cycle
        assert!(
            !manage_term(false, &other_leader_info, lease_timeout, &mock_meta_store)
                .await
                .unwrap(),
            "Follower: If new leader was elected, follower should enter election cycle"
        );
    }

    // Nobody can renew leader info does not exist
    #[tokio::test]
    async fn not_renew_lease_2() {
        let mock_meta_store = Arc::new(MemStore::new());

        let other_leader_info = MetaLeaderInfo {
            node_address: "other:1234".into(),
            lease_id: 456,
        };
        assert!(
            !manage_term(true, &other_leader_info, 123, &mock_meta_store)
                .await
                .unwrap(),
            "Leader: If new leader was elected old leader should NOT renew lease"
        );
        assert!(
            !manage_term(false, &other_leader_info, 123, &mock_meta_store)
                .await
                .unwrap(),
            "Follower: If new leader was elected, follower should enter election cycle"
        );
    }

    #[tokio::test]
    async fn lease_outdated() {
        // Follower: If lease is outdated, follower should delete leader and lease
        let lease_timeout = 10;
        let mock_meta_store = Arc::new(MemStore::new());
        let leader_info = MetaLeaderInfo {
            node_address: "localhost:1234".into(),
            lease_id: 123,
        };
        let now = since_epoch();
        // lease is expired
        let lease_info = MetaLeaseInfo {
            leader: Some(leader_info.clone()),
            lease_register_time: now.as_secs() - 2 * lease_timeout,
            lease_expire_time: now.as_secs() - lease_timeout,
        };
        put_leader_lease(&leader_info, &lease_info, &mock_meta_store).await;
        assert!(
            !manage_term(false, &leader_info, lease_timeout, &mock_meta_store)
                .await
                .unwrap(),
            "Should have determined that new election is needed if lease is no longer valid"
        );
        let res = get_leader_lease_obj(&mock_meta_store).await;
        assert!(
            res.is_none(),
            "Expected that leader and lease were deleted after lease expired.  {:?}",
            res
        )
    }

    #[tokio::test]
    async fn test_leader_not_changed() {
        // leader_changed should return false, if leader did not change. Independent of lease
        // changes
        let (lease_timeout, mock_meta_store, leader_info, _, old_lease_reg_time) =
            default_setup().await;
        assert!(
            !leader_changed(&leader_info, &mock_meta_store)
                .await
                .unwrap(),
            "Leader not changed and lease not changed"
        );
        let new_lease = MetaLeaseInfo {
            leader: Some(leader_info.clone()),
            lease_register_time: old_lease_reg_time.as_secs() + lease_timeout / 2,
            lease_expire_time: old_lease_reg_time.as_secs() + lease_timeout / 2 + lease_timeout,
        };
        put_lease_info(&new_lease, &mock_meta_store).await;
        assert!(
            !leader_changed(&leader_info, &mock_meta_store)
                .await
                .unwrap(),
            "Leader not changed"
        );
    }

    #[tokio::test]
    async fn test_leader_changed() {
        // leader_changed should return true, if leader did change. Independent of if lease changed
        let (lease_timeout, mock_meta_store, leader_info, _, old_lease_reg_time) =
            default_setup().await;

        let new_lease = MetaLeaseInfo {
            leader: Some(leader_info.clone()),
            lease_register_time: old_lease_reg_time.as_secs() + lease_timeout / 2,
            lease_expire_time: old_lease_reg_time.as_secs() + lease_timeout / 2 + lease_timeout,
        };
        let new_leader = MetaLeaderInfo {
            node_address: "other:789".to_owned(),
            lease_id: gen_rand_lease_id("other:789"),
        };
        put_leader_info(&new_leader, &mock_meta_store).await;
        assert!(
            leader_changed(&leader_info, &mock_meta_store)
                .await
                .unwrap(),
            "Leader changed and lease not changed"
        );
        put_lease_info(&new_lease, &mock_meta_store).await;
        assert!(
            leader_changed(&leader_info, &mock_meta_store)
                .await
                .unwrap(),
            "Leader changed and lease changed"
        );
    }
}
