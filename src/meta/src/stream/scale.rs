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

use std::cmp::{min, Ordering};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::iter::repeat;

use anyhow::{anyhow, Context};
use futures::future::BoxFuture;
use itertools::Itertools;
use num_integer::Integer;
use num_traits::abs;
use risingwave_common::bail;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::hash::{ActorMapping, ParallelUnitId, VirtualNode};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_pb::common::{ActorInfo, ParallelUnit, WorkerNode};
use risingwave_pb::meta::get_reschedule_plan_request::{Policy, StableResizePolicy};
use risingwave_pb::meta::table_fragments::actor_status::ActorState;
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::{self, ActorStatus, Fragment};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{DispatcherType, FragmentTypeFlag, StreamActor, StreamNode};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, UpdateActorsRequest,
};
use uuid::Uuid;

use crate::barrier::{Command, Reschedule};
use crate::manager::{IdCategory, WorkerId};
use crate::model::{ActorId, DispatcherId, FragmentId, TableFragments};
use crate::storage::{MetaStore, MetaStoreError, MetaStoreRef, Transaction, DEFAULT_COLUMN_FAMILY};
use crate::stream::{GlobalStreamManager, RescheduleOptions, ScaleController};
use crate::{MetaError, MetaResult};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableRevision(u64);

const TABLE_REVISION_KEY: &[u8] = b"table_revision";

impl From<TableRevision> for u64 {
    fn from(value: TableRevision) -> Self {
        value.0
    }
}

impl TableRevision {
    pub async fn get(store: &MetaStoreRef) -> MetaResult<Self> {
        let version = match store
            .get_cf(DEFAULT_COLUMN_FAMILY, TABLE_REVISION_KEY)
            .await
        {
            Ok(byte_vec) => memcomparable::from_slice(&byte_vec).unwrap(),
            Err(MetaStoreError::ItemNotFound(_)) => 0,
            Err(e) => return Err(MetaError::from(e)),
        };

        Ok(Self(version))
    }

    pub fn next(&self) -> Self {
        TableRevision(self.0 + 1)
    }

    pub fn store(&self, txn: &mut Transaction) {
        txn.put(
            DEFAULT_COLUMN_FAMILY.to_string(),
            TABLE_REVISION_KEY.to_vec(),
            memcomparable::to_vec(&self.0).unwrap(),
        );
    }

    pub fn inner(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParallelUnitReschedule {
    pub added_parallel_units: BTreeSet<ParallelUnitId>,
    pub removed_parallel_units: BTreeSet<ParallelUnitId>,
}

/// This function provides an simple balancing method
/// The specific process is as follows
///
/// 1. Calculate the number of target actors, and calculate the average value and the remainder, and
/// use the average value as expected.
///
/// 2. Filter out the actor to be removed and the actor to be retained, and sort them from largest
/// to smallest (according to the number of virtual nodes held).
///
/// 3. Calculate their balance, 1) For the actors to be removed, the number of virtual nodes per
/// actor is the balance. 2) For retained actors, the number of virtual nodes - expected is the
/// balance. 3) For newly created actors, -expected is the balance (always negative).
///
/// 4. Allocate the remainder, high priority to newly created nodes.
///
/// 5. After that, merge removed, retained and created into a queue, with the head of the queue
/// being the source, and move the virtual nodes to the destination at the end of the queue.
///
/// This can handle scale in, scale out, migration, and simultaneous scaling with as much affinity
/// as possible.
///
/// Note that this function can only rebalance actors whose `vnode_bitmap` is not `None`, in other
/// words, for `Fragment` of `FragmentDistributionType::Single`, using this function will cause
/// assert to fail and should be skipped from the upper level.
///
/// The return value is the bitmap distribution after scaling, which covers all virtual node indexes
pub fn rebalance_actor_vnode(
    actors: &[StreamActor],
    actors_to_remove: &BTreeSet<ActorId>,
    actors_to_create: &BTreeSet<ActorId>,
) -> HashMap<ActorId, Bitmap> {
    assert!(actors.len() >= actors_to_remove.len());

    let target_actor_count = actors.len() - actors_to_remove.len() + actors_to_create.len();
    assert!(target_actor_count > 0);

    // represents the balance of each actor, used to sort later
    #[derive(Debug)]
    struct Balance {
        actor_id: ActorId,
        balance: i32,
        builder: BitmapBuilder,
    }
    let (expected, mut remain) = VirtualNode::COUNT.div_rem(&target_actor_count);

    tracing::debug!(
        "expected {}, remain {}, prev actors {}, target actors {}",
        expected,
        remain,
        actors.len(),
        target_actor_count,
    );

    let (mut removed, mut rest): (Vec<_>, Vec<_>) = actors
        .iter()
        .filter_map(|actor| {
            actor
                .vnode_bitmap
                .as_ref()
                .map(|buffer| (actor.actor_id as ActorId, Bitmap::from(buffer)))
        })
        .partition(|(actor_id, _)| actors_to_remove.contains(actor_id));

    let order_by_bitmap_desc =
        |(_, bitmap_a): &(ActorId, Bitmap), (_, bitmap_b): &(ActorId, Bitmap)| -> Ordering {
            bitmap_a.count_ones().cmp(&bitmap_b.count_ones()).reverse()
        };

    let builder_from_bitmap = |bitmap: &Bitmap| -> BitmapBuilder {
        let mut builder = BitmapBuilder::default();
        builder.append_bitmap(bitmap);
        builder
    };

    let (prev_expected, _) = VirtualNode::COUNT.div_rem(&actors.len());

    let prev_remain = removed
        .iter()
        .map(|(_, bitmap)| {
            assert!(bitmap.count_ones() >= prev_expected);
            bitmap.count_ones() - prev_expected
        })
        .sum::<usize>();

    removed.sort_by(order_by_bitmap_desc);
    rest.sort_by(order_by_bitmap_desc);

    let removed_balances = removed.into_iter().map(|(actor_id, bitmap)| Balance {
        actor_id,
        balance: bitmap.count_ones() as i32,
        builder: builder_from_bitmap(&bitmap),
    });

    let mut rest_balances = rest
        .into_iter()
        .map(|(actor_id, bitmap)| Balance {
            actor_id,
            balance: bitmap.count_ones() as i32 - expected as i32,
            builder: builder_from_bitmap(&bitmap),
        })
        .collect_vec();

    let mut created_balances = actors_to_create
        .iter()
        .map(|actor_id| Balance {
            actor_id: *actor_id,
            balance: -(expected as i32),
            builder: BitmapBuilder::zeroed(VirtualNode::COUNT),
        })
        .collect_vec();

    for balance in created_balances
        .iter_mut()
        .rev()
        .take(prev_remain)
        .chain(rest_balances.iter_mut())
    {
        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
    }

    // consume the rest `remain`
    for balance in &mut created_balances {
        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
    }

    assert_eq!(remain, 0);

    let mut v: VecDeque<_> = removed_balances
        .chain(rest_balances)
        .chain(created_balances)
        .collect();

    // We will return the full bitmap here after rebalancing,
    // if we want to return only the changed actors, filter balance = 0 here
    let mut result = HashMap::with_capacity(target_actor_count);

    for balance in &v {
        tracing::debug!(
            "actor {:5}\tbalance {:5}\tR[{:5}]\tC[{:5}]",
            balance.actor_id,
            balance.balance,
            actors_to_remove.contains(&balance.actor_id),
            actors_to_create.contains(&balance.actor_id)
        );
    }

    while !v.is_empty() {
        if v.len() == 1 {
            let single = v.pop_front().unwrap();
            assert_eq!(single.balance, 0);
            if !actors_to_remove.contains(&single.actor_id) {
                result.insert(single.actor_id, single.builder.finish());
            }

            continue;
        }

        let mut src = v.pop_front().unwrap();
        let mut dst = v.pop_back().unwrap();

        let n = min(abs(src.balance), abs(dst.balance));

        let mut moved = 0;
        for idx in (0..VirtualNode::COUNT).rev() {
            if moved >= n {
                break;
            }

            if src.builder.is_set(idx) {
                src.builder.set(idx, false);
                assert!(!dst.builder.is_set(idx));
                dst.builder.set(idx, true);
                moved += 1;
            }
        }

        src.balance -= n;
        dst.balance += n;

        if src.balance != 0 {
            v.push_front(src);
        } else if !actors_to_remove.contains(&src.actor_id) {
            result.insert(src.actor_id, src.builder.finish());
        }

        if dst.balance != 0 {
            v.push_back(dst);
        } else {
            result.insert(dst.actor_id, dst.builder.finish());
        }
    }

    result
}

impl GlobalStreamManager {
    pub async fn reschedule_actors(
        &self,
        reschedules: HashMap<FragmentId, ParallelUnitReschedule>,
        options: RescheduleOptions,
    ) -> MetaResult<()> {
        let mut revert_funcs = vec![];
        if let Err(e) = self
            .reschedule_actors_impl(&mut revert_funcs, reschedules, options)
            .await
        {
            for revert_func in revert_funcs.into_iter().rev() {
                revert_func.await;
            }
            return Err(e);
        }

        Ok(())
    }

    async fn reschedule_actors_impl(
        &self,
        revert_funcs: &mut Vec<BoxFuture<'_, ()>>,
        reschedules: HashMap<FragmentId, ParallelUnitReschedule>,
        options: RescheduleOptions,
    ) -> MetaResult<()> {
        let (reschedule_fragment, applied_reschedules) = self
            .scale_controller
            .prepare_reschedule_command(reschedules, options)
            .await?;

        tracing::debug!("reschedule plan: {:#?}", reschedule_fragment);

        let command = Command::RescheduleFragment {
            reschedules: reschedule_fragment,
        };

        let fragment_manager_ref = self.fragment_manager.clone();

        revert_funcs.push(Box::pin(async move {
            fragment_manager_ref
                .cancel_apply_reschedules(applied_reschedules)
                .await;
        }));

        let _source_pause_guard = self.source_manager.paused.lock().await;

        self.barrier_scheduler
            .run_config_change_command_with_pause(command)
            .await?;

        Ok(())
    }
}
