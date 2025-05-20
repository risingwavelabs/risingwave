// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

use anyhow::{Context, Result, anyhow};

/// Assign items to weighted containers with optional capacity scaling and deterministic tie-breaking.
///
/// Distributes a slice of items (`&[I]`) across a set of containers (`BTreeMap<C, NonZeroUsize>`)
/// using a three-phase algorithm:
///
/// # Type Parameters
/// - `C`: Container identifier. Must implement `Ord + Hash + Eq + Copy + Debug`.
/// - `I`: Item type. Must implement `Hash + Eq + Copy + Debug`.
/// - `S`: Salt type for tie-breaking. Must implement `Hash + Copy`.
///
/// # Parameters
/// - `containers`: Map of containers to their non-zero weights (`BTreeMap<C, NonZeroUsize>`).
/// - `items`: Slice of items (`&[I]`) to distribute.
/// - `salt`: A salt value to vary deterministic tie-breaks between equal remainders.
/// - `capacity_scale_factor_fn`: Callback `(containers, items) -> Option<f64>`:
///     - `Some(f)`: Scale each container’s base quota by `f` (ceiled, but never below base).
///     - `None`: Remove upper bound (capacity = `usize::MAX`).
///
/// # Returns
/// A `BTreeMap<C, Vec<I>>` mapping each container to the list of assigned items.
/// - If `containers` is empty, returns an empty map.
/// - If `items` is empty, returns a map from each container key to an empty `Vec<I>`.
///
/// # Panics
/// - If the sum of all container weights is zero.
/// - If, during weighted rendezvous, no eligible container remains (invariant violation).
///
/// # Complexity
/// Runs in **O(N · M)** time, where N = `containers.len()` and M = `items.len()`.
/// Each item is compared against all containers via a weighted rendezvous hash.
///
/// # Example
/// ```rust
/// # use std::collections::BTreeMap;
/// # use std::num::NonZeroUsize;
/// # use risingwave_meta::stream::assign_items_weighted_with_scale_fn;
///
/// let mut caps = BTreeMap::new();
/// caps.insert("fast", NonZeroUsize::new(3).unwrap());
/// caps.insert("slow", NonZeroUsize::new(1).unwrap());
///
/// let tasks = vec!["task1", "task2", "task3", "task4"];
/// let result =
///     assign_items_weighted_with_scale_fn(&caps, &tasks, 0u8, |_containers, _items| Some(1.0));
///
/// // `fast` should receive roughly 3 tasks, `slow` roughly 1
/// assert_eq!(result.values().map(Vec::len).sum::<usize>(), tasks.len());
/// ```
///
/// # Algorithm
///
/// 1. **Quota Calculation**
///    - Compute `total_weight = sum(w_i)` as `u128`.
///    - For each container `i` with weight `w_i`:
///      ```text
///      ideal_i     = M * w_i
///      base_quota_i = floor(ideal_i / total_weight)
///      rem_i        = ideal_i % total_weight
///      ```
///    - Let `rem_count = M - sum(base_quota_i)` and sort containers by `rem_i` (desc),
///      breaking ties by `stable_hash((container, salt))`.
///    - Give `+1` slot to the first `rem_count` containers.
///
/// 2. **Capacity Scaling**
///    - If `Some(f)`: For each container,
///      `quota_i = max(base_quota_i, ceil(base_quota_i as f64 * f))`.
///    - If `None`: Set `quota_i = usize::MAX`.
///
/// 3. **Weighted Rendezvous Assignment**
///    - For each item `x`, compute for each container `i`:
///      ```text
///      h = stable_hash((x, i, salt))
///      r = (h + 1) / (MAX_HASH + 2)       // 0 < r ≤ 1
///      key_i = -ln(r) / weight_i
///      ```
///    - Assign `x` to the container with the smallest `key_i`.
pub fn assign_items_weighted_with_scale_fn<C, I, S>(
    containers: &BTreeMap<C, NonZeroUsize>,
    items: &[I],
    salt: S,
    capacity_scale_factor_fn: impl Fn(&BTreeMap<C, NonZeroUsize>, &[I]) -> Option<f64>,
) -> BTreeMap<C, Vec<I>>
where
    C: Ord + Hash + Eq + Copy + Clone + Debug,
    I: Hash + Eq + Copy + Clone + Debug,
    S: Hash + Copy,
{
    // Early exit if there is nothing to assign
    if containers.is_empty() || items.is_empty() {
        return BTreeMap::default();
    }

    // Integer-based quota calculation
    let total_weight: u128 = containers.values().map(|w| w.get() as u128).sum();
    assert!(
        total_weight > 0,
        "Sum of container weights must be non-zero"
    );

    struct QuotaInfo<C> {
        container: C,
        quota: usize,
        rem_part: u128,
    }

    let mut infos: Vec<QuotaInfo<C>> = containers
        .iter()
        .map(|(&container, &weight)| {
            let ideal_num = (items.len() as u128).saturating_mul(weight.get() as u128);
            QuotaInfo {
                container,
                quota: (ideal_num / total_weight) as usize,
                rem_part: ideal_num % total_weight,
            }
        })
        .collect();

    let used: usize = infos.iter().map(|info| info.quota).sum();
    let remainder = items.len().saturating_sub(used);

    // Distribute remainder slots
    infos.sort_by(|a, b| {
        b.rem_part
            .cmp(&a.rem_part)
            .then_with(|| stable_hash(&(b.container, salt)).cmp(&stable_hash(&(a.container, salt))))
    });
    for info in infos.iter_mut().take(remainder) {
        info.quota += 1;
    }

    // Apply capacity scaling
    let scale_factor = capacity_scale_factor_fn(containers, items);
    let quotas: HashMap<C, usize> = infos
        .into_iter()
        .map(|info| match scale_factor {
            Some(f) => {
                let scaled = (info.quota as f64 * f).ceil() as usize;
                (info.container, scaled.max(info.quota))
            }
            None => (info.container, usize::MAX),
        })
        .collect();

    // Prepare assignment map
    let mut assignment: BTreeMap<C, Vec<I>> = BTreeMap::new();
    // todo
    // let mut assignment: BTreeMap<C, Vec<I>> = containers.keys().map(|&c| (c, Vec::new())).collect();

    // Assign each item using Weighted Rendezvous
    for &item in items {
        let mut best: Option<(C, f64)> = None;
        for (&container, &weight) in containers {
            let assigned = assignment.get(&container).map(Vec::len).unwrap_or(0);

            debug_assert!(quotas.contains_key(&container));
            let quota = quotas.get(&container).copied().unwrap_or(0);
            if assigned >= quota {
                continue;
            }

            // Generate a pseudorandom float `r` in the range (0, 1]:
            // 1. Compute a stable 64-bit hash for the tuple (item, container).
            // 2. Normalize: `(raw_hash + 1) / (MAX_HASH + 2)` ensures `0 < r <= 1`.
            let raw_hash = stable_hash(&(item, container, salt));
            let r = (raw_hash as f64 + 1.0) / (u64::MAX as f64 + 2.0);

            // Compute weighted rendezvous key:
            // 1. `-ln(r)` maps the interval (0,1] to [0, ∞).
            // 2. Dividing by `w` biases selection towards containers with higher weight,
            //    as smaller keys win in the rendezvous algorithm.
            let key = -r.ln() / (weight.get() as f64);

            match best {
                None => best = Some((container, key)),
                Some((_, best_key)) if key < best_key => best = Some((container, key)),
                _ => {}
            }
        }

        // quotas sum (possibly scaled) always >= items.len(), so best is always Some
        let (winner, _) = best.expect("Invariant violation: no eligible container");
        assignment
            .entry(winner)
            .and_modify(|v| v.push(item))
            .or_insert_with(|| vec![item]);
    }

    assignment
}

/// Stable hash utility
fn stable_hash<T: Hash>(t: &T) -> u64 {
    // let mut hasher = twox_hash::XxHash3_64::new();
    let mut hasher = std::hash::DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

/// A no-op capacity scaling function: always returns `None`.
pub fn unbounded_scale<C, I>(_containers: &BTreeMap<C, NonZeroUsize>, _items: &[I]) -> Option<f64> {
    None
}

/// A unit capacity scaling function: always returns `Some(1.0)`.
pub fn weighted_scale<C, I>(_containers: &BTreeMap<C, NonZeroUsize>, _items: &[I]) -> Option<f64> {
    Some(1.0)
}

/// Defines the capacity assignment strategy for containers.
///
/// - `Weighted`: Distribute items proportionally to container weights, applying any configured scale factor.
/// - `Unbounded`: No capacity limit; containers can receive any number of items.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum CapacityMode {
    /// Use each container’s weight to bound how many items it can receive.
    /// When used for actor-to-worker assignment, this typically means actors are distributed
    /// strictly proportionally to worker weights (i.e., using a scale factor of 1.0).
    Weighted,

    /// Ignore per-container quotas entirely—every container can take an unlimited number of items.
    Unbounded,
}

/// Defines the vnode distribution strategy for hierarchical assignment.
///
/// - `RawWorkerWeights`: Distribute vnodes across workers using the original worker weight values.
/// - `ActorCounts`: Distribute vnodes based on the number of actors assigned to each worker.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum BalancedBy {
    /// Use each worker's raw weight when allocating vnodes.
    RawWorkerWeights,

    /// Use the count of actors per worker as the weight for vnode distribution.
    /// This strategy aims to balance the number of vnodes per actor across workers.
    ActorCounts,
}

/// Hierarchically distributes virtual nodes to actors in two weighted stages with deterministic tie-breaking.
///
/// This function first assigns each actor to a worker, then distributes all virtual nodes among
/// those active workers, and finally partitions each worker’s vnodes among its actors in a simple
/// round-robin fashion.
///
/// # Type Parameters
/// - `W`: Worker identifier. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
/// - `A`: Actor identifier. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
/// - `V`: Virtual node type. Must implement `Hash + Eq + Copy + Clone + Debug`.
/// - `S`: Salt type for deterministic tie-breaking. Must implement `Hash + Copy`.
///
/// # Parameters
/// - `workers`: A `BTreeMap<W, NonZeroUsize>` mapping each worker to its positive weight.
/// - `actors`: A slice of actors (`&[A]`) to place on workers.
/// - `virtual_nodes`: A slice of vnodes (`&[V]`) to distribute across actors.
/// - `salt`: A salt value to break ties in hashing, kept constant per invocation for reproducibility.
/// - `actor_capacity_mode`: A `CapacityMode` deciding how actors are packed onto workers:
///     - `Weighted`: respect `workers` weights when placing actors.
///     - `Unbounded`: ignore capacity limits when placing actors.
/// - `balanced_by`: A `BalancedBy` enum determining vnode distribution strategy:
///     - `RawWorkerWeights`: prioritize original worker weights (with actor count as lower bound).
///     - `ActorCounts`: prioritize equal vnode counts per actor (actor-oriented).
///
/// # Returns
/// A `BTreeMap<W, BTreeMap<A, Vec<V>>>` mapping each worker to its map of actors and their assigned vnodes.
/// - Only workers with at least one actor appear in the result.
/// - Each actor receives at least one vnode (invariant).
///
/// # Errors
/// - Returns an error if `actors` is empty or `virtual_nodes` is empty.
/// - Returns an error if `actors.len() > virtual_nodes.len()`, since each actor must receive at least one vnode.
///
/// # Complexity
/// Runs in **O((W + A + V) · log W + V · W)** time:
/// - Actor → Worker assignment is O(A · W) via weighted rendezvous + O(W + A) map operations.
/// - VNode → Worker assignment is O(V · W) plus quota computation O(W log W).
/// - VNode → Actor partition is O(V).
///
/// # Example
/// ```rust
/// # use std::collections::BTreeMap;
/// # use std::num::NonZeroUsize;
/// # use risingwave_meta::stream::{assign_hierarchical, BalancedBy, CapacityMode};
///
/// // Define two workers with numeric IDs and weights
/// let mut workers: BTreeMap<u8, NonZeroUsize> = BTreeMap::new();
/// workers.insert(1, NonZeroUsize::new(2).unwrap());
/// workers.insert(2, NonZeroUsize::new(3).unwrap());
///
/// // Actors also identified by numbers
/// let actors: Vec<u16> = vec![10, 20, 30];
///
/// // Virtual nodes are simple 0–8
/// let vnodes: Vec<u16> = (0..9).collect();
///
/// let assignment = assign_hierarchical(
///     &workers,
///     &actors,
///     &vnodes,
///     0u8,                          // salt
///     CapacityMode::Weighted,       // actor -> worker mode
///     BalancedBy::RawWorkerWeights, // vnode -> worker mode
/// )
/// .unwrap();
///
/// for (worker_id, actor_map) in assignment {
///     println!("Worker {}:", worker_id);
///     for (actor_id, vn_list) in actor_map {
///         println!("  Actor {} -> {:?}", actor_id, vn_list);
///     }
/// }
/// ```
///
/// # Algorithm
///
/// 1. **Actors → Workers**
///    - Use weighted or unbounded rendezvous hashing to assign each actor to exactly one worker,
///      based on `actor_capacity_mode` and `workers` weights.
///    - Build `actor_to_worker: BTreeMap<W, Vec<A>>`.
///
/// 2. **`VNodes` → Workers**
///    - If `RawWorkerWeights`: compute per-worker quotas with `compute_worker_quotas`, ensuring
///      each active worker’s quota ≥ its actor count and quotas sum = total vnodes.
///    - If `ActorCounts`: set each worker’s weight = its actor count.
///    - Run `assign_items_weighted_with_scale_fn` on vnodes vs. the computed weights,
///      yielding `vnode_to_worker: BTreeMap<W, Vec<V>>`.
///
/// 3. **`VNodes` → Actors**
///    - For each worker, take its vnode list and assign them to actors in simple round-robin:
///      iterate vnodes in order, dispatching index `% actor_list.len()`.
///    - Collect into final `BTreeMap<W, BTreeMap<A, Vec<V>>>`.
pub fn assign_hierarchical<W, A, V, S>(
    workers: &BTreeMap<W, NonZeroUsize>,
    actors: &[A],
    virtual_nodes: &[V],
    salt: S,
    actor_capacity_mode: CapacityMode,
    balanced_by: BalancedBy,
) -> anyhow::Result<BTreeMap<W, BTreeMap<A, Vec<V>>>>
where
    W: Ord + Hash + Eq + Copy + Clone + Debug,
    A: Ord + Hash + Eq + Copy + Clone + Debug,
    V: Hash + Eq + Copy + Clone + Debug,
    S: Hash + Copy,
{
    if actors.is_empty() {
        return Err(anyhow!("no actors to assign"));
    }

    if virtual_nodes.is_empty() {
        return Err(anyhow!("no vnodes to assign"));
    }

    // Validate input: ensure vnode count can cover all actors
    if actors.len() > virtual_nodes.len() {
        return Err(anyhow!(
            "actor count ({}) exceeds vnode count ({})",
            actors.len(),
            virtual_nodes.len()
        ));
    }

    let actor_capacity_fn = match actor_capacity_mode {
        CapacityMode::Weighted => weighted_scale,
        CapacityMode::Unbounded => unbounded_scale,
    };

    // Distribute actors across workers based on their weight
    let actor_to_worker: BTreeMap<W, Vec<A>> =
        assign_items_weighted_with_scale_fn(workers, actors, salt, actor_capacity_fn);

    // Build unit-weight map for active workers (those with assigned actors)
    let mut active_worker_weights: BTreeMap<W, NonZeroUsize> = BTreeMap::new();

    match balanced_by {
        BalancedBy::RawWorkerWeights => {
            // Worker oriented: balanced by raw worker weights
            let mut actor_counts: HashMap<W, usize> = HashMap::new();
            for (&worker, actor_list) in &actor_to_worker {
                if !actor_list.is_empty() {
                    let worker_weight = workers.get(&worker).expect("Worker should exist");
                    active_worker_weights.insert(worker, *worker_weight);
                    actor_counts.insert(worker, actor_list.len());
                }
            }

            // Recalculate the worker weight to prevent actors from being assigned to vnode.
            active_worker_weights = compute_worker_quotas(
                &active_worker_weights,
                &actor_counts,
                virtual_nodes.len(),
                salt,
            );
        }
        BalancedBy::ActorCounts => {
            // Actor oriented: balanced by actor counts
            for (&worker, actor_list) in &actor_to_worker {
                debug_assert!(!actor_list.is_empty());
                if let Some(actor_count) = NonZeroUsize::new(actor_list.len()) {
                    active_worker_weights.insert(worker, actor_count);
                }
            }
        }
    }

    // Distribute vnodes evenly among the active workers
    let vnode_to_worker: BTreeMap<W, Vec<V>> = assign_items_weighted_with_scale_fn(
        &active_worker_weights,
        virtual_nodes,
        salt,
        weighted_scale,
    );

    // Assign each worker's vnodes to its actors in a round-robin fashion
    let mut assignment = BTreeMap::new();
    for (worker, actor_list) in actor_to_worker {
        let assigned_vnodes = vnode_to_worker.get(&worker).cloned().unwrap_or_default();

        // Actors and vnodes can only both be empty at the same time or both be non-empty at the same time.
        assert_eq!(
            assigned_vnodes.is_empty(),
            actor_list.is_empty(),
            "Invariant violation: empty actor list should have empty vnodes"
        );

        debug_assert!(
            assigned_vnodes.len() >= actor_list.len(),
            "Invariant violation: assigned vnodes should be at least as many as actors"
        );

        // Within the same worker, use a simple round-robin approach to distribute vnodes relatively evenly among actors.
        let mut actor_map = BTreeMap::new();
        for (index, vnode) in assigned_vnodes.into_iter().enumerate() {
            let actor = actor_list[index % actor_list.len()];
            actor_map.entry(actor).or_insert(Vec::new()).push(vnode);
        }
        assignment.insert(worker, actor_map);
    }

    Ok(assignment)
}

/// Computes per-worker VNode quotas based on actor counts and worker weights.
///
/// This function allocates virtual nodes to workers such that:
/// - Each active worker receives at least as many virtual nodes as it has actors (`base_quota`).
/// - The remaining virtual nodes (`extra_vnodes`) are distributed proportionally to the original worker weights.
/// - Deterministic tie-breaking on equal remainders uses a hash of (`salt`, `worker_id`).
///
/// # Type Parameters
/// - `W`: Worker identifier type. Must implement `Ord`, `Copy`, `Hash`, `Eq`, and `Debug`.
/// - `S`: Salt type. Used for deterministic hashing. Must implement `Hash` and `Copy`.
///
/// # Parameters
/// - `workers`: A `BTreeMap` mapping each worker ID to its non-zero weight (`NonZeroUsize`).
/// - `actor_counts`: A `HashMap` mapping each worker ID to the number of actors assigned.
/// - `total_vnodes`: The total number of virtual nodes to distribute across all active workers.
/// - `salt`: A salt value for deterministic tie-breaking in remainder sorting.
///
/// # Returns
/// A `BTreeMap` from worker ID to its allocated quota (`NonZeroUsize`), such that the sum of all quotas equals `total_vnodes`.
///
/// # Panics
/// Panics if any computed quota is zero, which should not occur when `total_vnodes >= sum(actor_counts)`.
///
/// # Algorithm
/// 1. Compute `base_total` as the sum of all actor counts.
/// 2. Compute `extra_vnodes = total_vnodes - base_total`.
/// 3. For each active worker:
///    a. Set `base_quota` equal to its actor count.
///    b. Compute `ideal_extra = extra_vnodes * weight / total_weight`.
///    c. Record `extra_floor = floor(ideal_extra)` and `extra_remainder = ideal_extra % total_weight`.
/// 4. Sort workers by descending `extra_remainder`; tie-break by `stable_hash((salt, worker_id))` ascending.
/// 5. Distribute the remaining slots (`extra_vnodes - sum(extra_floor)`) by incrementing `extra_floor` for the top workers.
/// 6. Final quota for each worker is `base_quota + extra_floor`.
fn compute_worker_quotas<W, S>(
    workers: &BTreeMap<W, NonZeroUsize>,
    actor_counts: &HashMap<W, usize>,
    total_vnodes: usize,
    salt: S,
) -> BTreeMap<W, NonZeroUsize>
where
    W: Ord + Copy + Hash + Eq + Debug,
    S: Hash + Copy,
{
    let base_total: usize = actor_counts.values().sum();
    let extra_vnodes = total_vnodes - base_total;

    // Quota calculation is only performed for Workers with actors.
    let active_workers: Vec<W> = actor_counts.keys().copied().collect();
    let total_weight: u128 = active_workers
        .iter()
        .map(|&worker_id| workers[&worker_id].get() as u128)
        .sum();

    assert!(total_weight > 0, "Sum of worker weights must be non-zero");
    assert!(total_vnodes > 0, "Sum of vnodes must be non-zero");

    // Temporary structure: stores calculation information
    struct QuotaInfo<W> {
        worker_id: W,
        base_quota: usize,
        extra_floor: usize,
        extra_remainder: u128,
    }

    // Preliminary calculation of floor and remainder
    let mut quota_list: Vec<QuotaInfo<W>> = active_workers
        .into_iter()
        .map(|worker_id| {
            let base_quota = actor_counts[&worker_id];
            let weight = workers[&worker_id].get() as u128;
            let ideal_extra = extra_vnodes as u128 * weight;
            let extra_floor = (ideal_extra / total_weight) as usize;
            let extra_remainder = ideal_extra % total_weight;
            QuotaInfo {
                worker_id,
                base_quota,
                extra_floor,
                extra_remainder,
            }
        })
        .collect();

    // Distribute the remaining slots (sorted by remainder, the first N get +1)
    let used_extra: usize = quota_list.iter().map(|quota| quota.extra_floor).sum();
    let remaining_slots = extra_vnodes - used_extra;
    quota_list.sort_by(|a, b| {
        // First, sort by remainder in descending order.
        b.extra_remainder
            .cmp(&a.extra_remainder)
            // If remainders are the same, then sort by the hash value of (salt, worker_id) in ascending order.
            .then_with(|| stable_hash(&(salt, a.worker_id)).cmp(&stable_hash(&(salt, b.worker_id))))
    });
    for info in quota_list.iter_mut().take(remaining_slots) {
        info.extra_floor += 1;
    }

    // Construct the final quotas
    let mut quotas = BTreeMap::new();
    for info in quota_list {
        let total = info.base_quota + info.extra_floor;
        quotas.insert(info.worker_id, NonZeroUsize::new(total).unwrap());
    }
    quotas
}

/// Core assigner with configurable strategies.
pub struct Assigner<S> {
    salt: S,
    actor_capacity: CapacityMode,
    balance_strategy: BalancedBy,
}

/// Builder for [`Assigner`].
#[derive(Debug)]
pub struct AssignerBuilder<S> {
    salt: S,
    actor_capacity: CapacityMode,
    balance_strategy: BalancedBy,
}

impl<S: Hash + Copy> AssignerBuilder<S> {
    /// Create a new builder with the given salt.
    pub fn new(salt: S) -> Self {
        Self {
            salt,
            actor_capacity: CapacityMode::Weighted,
            balance_strategy: BalancedBy::RawWorkerWeights,
        }
    }

    /// Use weighted capacity when assigning actors.
    pub fn with_capacity_weighted(&mut self) -> &mut Self {
        self.actor_capacity = CapacityMode::Weighted;
        self
    }

    /// Use unbounded capacity when assigning actors.
    pub fn with_capacity_unbounded(&mut self) -> &mut Self {
        self.actor_capacity = CapacityMode::Unbounded;
        self
    }

    /// Balance vnodes by actor counts (actor‐oriented).
    pub fn with_actor_oriented_balancing(&mut self) -> &mut Self {
        self.balance_strategy = BalancedBy::ActorCounts;
        self
    }

    /// Balance vnodes by raw worker weights (worker‐oriented).
    pub fn with_worker_oriented_balancing(&mut self) -> &mut Self {
        self.balance_strategy = BalancedBy::RawWorkerWeights;
        self
    }

    /// Finalize and build the [`Assigner`].
    pub fn build(&self) -> Assigner<S> {
        Assigner {
            salt: self.salt,
            actor_capacity: self.actor_capacity,
            balance_strategy: self.balance_strategy,
        }
    }
}

impl<S: Hash + Copy> Assigner<S> {
    /// Assigns each actor to a worker according to `CapacityMode`.
    pub fn assign_actors<C, I>(
        &self,
        workers: &BTreeMap<C, NonZeroUsize>,
        actors: &[I],
    ) -> BTreeMap<C, Vec<I>>
    where
        C: Ord + Hash + Eq + Copy + Debug,
        I: Hash + Eq + Copy + Debug,
    {
        let scale_fn = match self.actor_capacity {
            CapacityMode::Weighted => weighted_scale,
            CapacityMode::Unbounded => unbounded_scale,
        };
        assign_items_weighted_with_scale_fn(workers, actors, self.salt, scale_fn)
    }

    /// Returns how many actors each worker would receive for `actor_count` actors.
    pub fn count_actors_per_worker<C>(
        &self,
        workers: &BTreeMap<C, NonZeroUsize>,
        actor_count: usize,
    ) -> BTreeMap<C, usize>
    where
        C: Ord + Hash + Eq + Copy + Debug,
    {
        let synthetic = (0..actor_count).collect::<Vec<_>>();
        vec_len_map(self.assign_actors(workers, &synthetic))
    }

    /// Hierarchical assignment: Actors → Workers → `VNodes` → Actors.
    pub fn assign_hierarchical<W, A, V>(
        &self,
        workers: &BTreeMap<W, NonZeroUsize>,
        actors: &[A],
        vnodes: &[V],
    ) -> Result<BTreeMap<W, BTreeMap<A, Vec<V>>>>
    where
        W: Ord + Hash + Eq + Copy + Debug,
        A: Ord + Hash + Eq + Copy + Debug,
        V: Hash + Eq + Copy + Debug,
    {
        assign_hierarchical(
            workers,
            actors,
            vnodes,
            self.salt,
            self.actor_capacity,
            self.balance_strategy,
        )
        .context("hierarchical assignment failed")
    }

    /// Hierarchical counts: how many vnodes each actor gets.
    pub fn assign_hierarchical_counts<W, A>(
        &self,
        workers: &BTreeMap<W, NonZeroUsize>,
        actor_count: usize,
        vnode_count: usize,
    ) -> Result<BTreeMap<W, BTreeMap<A, usize>>>
    where
        W: Ord + Hash + Eq + Copy + Debug,
        A: Ord + Hash + Eq + Copy + Debug + From<usize>,
    {
        let actors = (0..actor_count).map(A::from).collect::<Vec<_>>();
        let vnodes = (0..vnode_count).collect::<Vec<_>>();
        let full = self.assign_hierarchical(workers, &actors, &vnodes)?;
        Ok(full
            .into_iter()
            .map(|(w, actor_map)| (w, vec_len_map(actor_map)))
            .collect())
    }
}

/// Helper: maps each `Vec<V>` to its length.
fn vec_len_map<K, V>(map: BTreeMap<K, Vec<V>>) -> BTreeMap<K, usize>
where
    K: Ord,
{
    map.into_iter().map(|(k, v)| (k, v.len())).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::num::NonZeroUsize;

    use super::*;

    /// Always returns a scale factor of 1.0 (no-op scaling)
    fn unit_scale<C, I>(_: &BTreeMap<C, NonZeroUsize>, _: &[I]) -> Option<f64> {
        Some(1.0)
    }

    #[test]
    fn empty_containers_or_items_yields_empty_map() {
        let empty_containers: BTreeMap<&str, NonZeroUsize> = BTreeMap::new();
        let items = vec![1, 2, 3];
        let result =
            assign_items_weighted_with_scale_fn(&empty_containers, &items, 0u8, unit_scale);
        assert!(
            result.is_empty(),
            "Expected empty map when containers empty"
        );

        let mut containers = BTreeMap::new();
        containers.insert("c1", NonZeroUsize::new(1).unwrap());
        let empty_items: Vec<i32> = Vec::new();
        let result2 =
            assign_items_weighted_with_scale_fn(&containers, &empty_items, 0u8, unit_scale);
        assert!(result2.is_empty(), "Expected empty map when items empty");
    }

    #[test]
    fn single_container_receives_all_items() {
        let mut containers = BTreeMap::new();
        containers.insert("only", NonZeroUsize::new(5).unwrap());
        let items = vec![10, 20, 30];

        let assignment = assign_items_weighted_with_scale_fn(&containers, &items, 1u8, unit_scale);

        assert_eq!(assignment.len(), 1, "Only one container should be present");
        let assigned = &assignment[&"only"];
        assert_eq!(assigned, &items, "Single container should get all items");
    }

    #[test]
    fn equal_weights_divisible_split_evenly() {
        let mut containers = BTreeMap::new();
        containers.insert("A", NonZeroUsize::new(1).unwrap());
        containers.insert("B", NonZeroUsize::new(1).unwrap());
        let items = vec![1, 2, 3, 4];

        let result = assign_items_weighted_with_scale_fn(&containers, &items, 2u8, unit_scale);
        let a_count = result[&"A"].len();
        let b_count = result[&"B"].len();
        assert_eq!(a_count, 2, "Container A should receive 2 items");
        assert_eq!(b_count, 2, "Container B should receive 2 items");
        assert_eq!(a_count + b_count, items.len(), "All items must be assigned");
    }

    #[test]
    fn equal_weights_non_divisible_split_remainder_assigned() {
        let mut containers = BTreeMap::new();
        containers.insert("X", NonZeroUsize::new(1).unwrap());
        containers.insert("Y", NonZeroUsize::new(1).unwrap());
        let items = vec![1, 2, 3];

        let result = assign_items_weighted_with_scale_fn(&containers, &items, 5u8, unit_scale);
        let x_count = result.get(&"X").map(Vec::len).unwrap_or(0);
        let y_count = result.get(&"Y").map(Vec::len).unwrap_or(0);
        assert_eq!(x_count + y_count, items.len(), "All items must be assigned");
        assert!(
            (x_count == 2 && y_count == 1) || (x_count == 1 && y_count == 2),
            "One container should get 2 items, the other 1, but got {} and {}",
            x_count,
            y_count
        );
    }

    #[test]
    fn unequal_weights_respect_base_quota() {
        let mut containers = BTreeMap::new();
        containers.insert("low", NonZeroUsize::new(1).unwrap());
        containers.insert("high", NonZeroUsize::new(3).unwrap());
        let items = vec![100, 200, 300, 400];

        let result = assign_items_weighted_with_scale_fn(&containers, &items, 7u8, unit_scale);
        let low_count = result[&"low"].len();
        let high_count = result[&"high"].len();
        // low weight should get 1, high weight 3
        assert_eq!(low_count, 1, "Low-weight container should get 1 item");
        assert_eq!(high_count, 3, "High-weight container should get 3 items");
    }

    #[test]
    fn deterministic_given_same_salt() {
        let mut containers = BTreeMap::new();
        containers.insert("A", NonZeroUsize::new(2).unwrap());
        containers.insert("B", NonZeroUsize::new(1).unwrap());
        let items = vec![5, 6, 7, 8];

        let out1 = assign_items_weighted_with_scale_fn(&containers, &items, 42u8, unit_scale);
        let out2 = assign_items_weighted_with_scale_fn(&containers, &items, 42u8, unit_scale);
        assert_eq!(out1, out2, "Same salt should produce identical assignments");
    }

    #[test]
    fn test_compute_worker_quotas_equal_weights() {
        // Three workers, each with weight 1, one actor each, total_vnodes = 6
        let workers: BTreeMap<u8, NonZeroUsize> = vec![
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(1).unwrap()),
            (3, NonZeroUsize::new(1).unwrap()),
        ]
        .into_iter()
        .collect();
        let mut actor_counts = HashMap::new();
        actor_counts.insert(1, 1);
        actor_counts.insert(2, 1);
        actor_counts.insert(3, 1);
        let total_vnodes = 6;
        let salt = 42u64;

        let quotas = compute_worker_quotas(&workers, &actor_counts, total_vnodes, salt);
        // Each worker should have quota = 2
        for (&worker_id, &quota) in &quotas {
            assert_eq!(quota.get(), 2, "Worker {} expected quota 2", worker_id);
        }
        // Sum of quotas equals total_vnodes
        let sum: usize = quotas.values().map(|q| q.get()).sum();
        assert_eq!(sum, total_vnodes);
    }

    #[test]
    fn test_compute_worker_quotas_unequal_weights() {
        // Two workers: id 1 weight 2, id 2 weight 1, one actor each, total_vnodes = 6
        let workers: BTreeMap<u8, NonZeroUsize> = vec![
            (1, NonZeroUsize::new(2).unwrap()),
            (2, NonZeroUsize::new(1).unwrap()),
        ]
        .into_iter()
        .collect();
        let mut actor_counts = HashMap::new();
        actor_counts.insert(1, 1);
        actor_counts.insert(2, 1);
        let total_vnodes = 6;
        let salt = 100u64;

        let quotas = compute_worker_quotas(&workers, &actor_counts, total_vnodes, salt);
        // Worker 1 should get 4, worker 2 should get 2
        assert_eq!(quotas[&1].get(), 4);
        assert_eq!(quotas[&2].get(), 2);
        // Sum of quotas equals total_vnodes
        let sum: usize = quotas.values().map(|q| q.get()).sum();
        assert_eq!(sum, total_vnodes);
    }

    #[test]
    fn test_compute_worker_quotas_minimum_base() {
        // Worker with no actors should not appear in quotas
        let workers: BTreeMap<u8, NonZeroUsize> = vec![
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(1).unwrap()),
        ]
        .into_iter()
        .collect();
        let mut actor_counts = HashMap::new();
        actor_counts.insert(1, 2);
        // worker 2 has zero actors
        let total_vnodes = 5;
        let salt = 7u8;

        let quotas = compute_worker_quotas(&workers, &actor_counts, total_vnodes, salt);
        // Only worker 1 should be present
        assert_eq!(quotas.len(), 1);
        // Its quota should equal total_vnodes
        assert_eq!(quotas[&1].get(), total_vnodes);
    }

    #[test]
    #[should_panic]
    fn test_compute_worker_quotas_invalid_total() {
        // total_vnodes less than sum of actor_counts should panic
        let workers: BTreeMap<u8, NonZeroUsize> = vec![(1, NonZeroUsize::new(1).unwrap())]
            .into_iter()
            .collect();
        let mut actor_counts = HashMap::new();
        actor_counts.insert(1, 3);
        let total_vnodes = 2; // less than base_total = 3
        let salt = 0u16;

        // This should panic due to underflow of extra_vnodes
        let _ = compute_worker_quotas(&workers, &actor_counts, total_vnodes, salt);
    }

    #[test]
    fn error_on_empty_actors() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![(1, NonZeroUsize::new(1).unwrap())]
            .into_iter()
            .collect();
        let actors: Vec<u16> = vec![];
        let vnodes: Vec<u16> = vec![1, 2];

        let err = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            0u8,
            CapacityMode::Weighted,
            BalancedBy::ActorCounts,
        )
        .unwrap_err();

        assert!(err.to_string().contains("no actors to assign"));
    }

    #[test]
    fn error_on_empty_vnodes() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![(1, NonZeroUsize::new(1).unwrap())]
            .into_iter()
            .collect();
        let actors: Vec<u16> = vec![10, 20];
        let vnodes: Vec<u16> = vec![];

        let err = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            1u8,
            CapacityMode::Unbounded,
            BalancedBy::RawWorkerWeights,
        )
        .unwrap_err();

        assert!(err.to_string().contains("no vnodes to assign"));
    }

    #[test]
    fn error_when_more_actors_than_vnodes() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![(1, NonZeroUsize::new(1).unwrap())]
            .into_iter()
            .collect();
        let actors: Vec<u16> = vec![1, 2, 3];
        let vnodes: Vec<u16> = vec![100];

        let err = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            7u8,
            CapacityMode::Weighted,
            BalancedBy::ActorCounts,
        )
        .unwrap_err();

        assert!(err.to_string().contains("exceeds vnode count"));
    }

    #[test]
    fn single_worker_all_actors_and_vnodes() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![(1, NonZeroUsize::new(5).unwrap())]
            .into_iter()
            .collect();
        let actors: Vec<u16> = vec![10, 20, 30];
        let vnodes: Vec<u16> = vec![100, 200, 300];

        let assignment = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            42u8,
            CapacityMode::Weighted,
            BalancedBy::RawWorkerWeights,
        )
        .unwrap();

        // Only one worker should appear
        assert_eq!(assignment.len(), 1);
        let inner = &assignment[&1u8];
        // Each actor must get exactly one vnode
        for &actor in &actors {
            let assigned = inner.get(&actor).unwrap();
            assert_eq!(assigned.len(), 1, "Actor {} should have one vnode", actor);
        }
        // All vnodes assigned
        let total: usize = inner.values().map(Vec::len).sum();
        assert_eq!(total, vnodes.len());
    }

    #[test]
    fn two_workers_balanced_by_actorcounts() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(1).unwrap()),
        ]
        .into_iter()
        .collect();
        let actors: Vec<u16> = vec![10, 20];
        let vnodes: Vec<u16> = vec![0, 1];

        let assignment = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            5u8,
            CapacityMode::Weighted,
            BalancedBy::ActorCounts,
        )
        .unwrap();

        // Both workers should appear
        assert_eq!(assignment.len(), 2);
        for (&w, inner) in &assignment {
            // Each worker has exactly one actor
            assert_eq!(inner.len(), 1, "Worker {} should have one actor", w);
            // That actor has exactly one vnode
            let (_, vlist) = inner.iter().next().unwrap();
            assert_eq!(vlist.len(), 1, "Worker {} actor should have one vnode", w);
        }
    }

    #[test]
    fn rawworkerweights_respects_worker_weight() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(3).unwrap()),
        ]
        .into_iter()
        .collect();
        let actors: Vec<u16> = vec![10, 20, 30, 40];
        let vnodes: Vec<u16> = vec![0, 1, 2, 3, 4, 5, 6];

        let assignment = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            9u8,
            CapacityMode::Weighted,
            BalancedBy::RawWorkerWeights,
        )
        .unwrap();

        let w1_total: usize = assignment.get(&1).unwrap().values().map(Vec::len).sum();
        let w2_total: usize = assignment.get(&2).unwrap().values().map(Vec::len).sum();
        // Worker 2 has triple weight, so should get roughly 3/4 of vnodes
        assert!(
            w2_total > w1_total,
            "Worker 2 should receive more vnodes than Worker 1"
        );
        assert_eq!(
            w1_total + w2_total,
            vnodes.len(),
            "All vnodes must be assigned"
        );
    }

    /// Always returns None (unbounded capacity)
    fn unbounded_scale_fn<C, I>(_: &BTreeMap<C, NonZeroUsize>, _: &[I]) -> Option<f64> {
        None
    }

    /// Returns a specific scale factor
    fn custom_scale_fn(
        factor: f64,
    ) -> impl Fn(&BTreeMap<i32, NonZeroUsize>, &[i32]) -> Option<f64> {
        move |_, _| Some(factor)
    }

    // --- Tests for assign_items_weighted_with_scale_fn ---
    #[test]
    fn assign_items_unbounded_scale_ignores_proportional_quota() {
        let mut containers = BTreeMap::new();
        let container_a_id = "A";
        let container_b_id = "B";
        containers.insert(container_a_id, NonZeroUsize::new(1).unwrap()); // Low weight
        containers.insert(container_b_id, NonZeroUsize::new(100).unwrap()); // High weight
        let items: Vec<i32> = (0..100).collect(); // 100 items
        let salt = 123u8;

        // 1. Assignment with unit_scale (strictly proportional quotas)
        let assignment_unit =
            assign_items_weighted_with_scale_fn(&containers, &items, salt, unit_scale);
        let a_count_unit = assignment_unit.get(container_a_id).map_or(0, Vec::len);
        let b_count_unit = assignment_unit.get(container_b_id).map_or(0, Vec::len);

        assert!(
            a_count_unit < 10,
            "With unit_scale, A ({}) should have few items, got {}",
            container_a_id,
            a_count_unit
        );
        assert!(
            b_count_unit > 90,
            "With unit_scale, B ({}) should have many items, got {}",
            container_b_id,
            b_count_unit
        );
        assert_eq!(
            a_count_unit + b_count_unit,
            items.len(),
            "All items must be assigned in unit_scale"
        );

        // 2. Assignment with unbounded_scale_fn (quotas are usize::MAX)
        let assignment_unbounded =
            assign_items_weighted_with_scale_fn(&containers, &items, salt, unbounded_scale_fn);
        let a_count_unbounded = assignment_unbounded.get(container_a_id).map_or(0, Vec::len);
        let b_count_unbounded = assignment_unbounded.get(container_b_id).map_or(0, Vec::len);

        assert_eq!(
            a_count_unbounded + b_count_unbounded,
            items.len(),
            "All items must be assigned in unbounded_scale"
        );

        // 3. Assertions for unbounded behavior
        // In unbounded mode, quotas are effectively infinite. Distribution is by rendezvous hashing.
        // We expect that the distribution *might* differ from the strictly weighted one.
        // If it does differ, it implies quotas were not the sole determining factor.
        // This is not a strict requirement for the test to pass, as coincidence is possible,
        // but it's an indicator we look for.
        if items.len() >= 20
            && (a_count_unbounded == a_count_unit && b_count_unbounded == b_count_unit)
        {
            // Log if the distributions happen to be identical for a non-trivial case.
            // This doesn't mean the test failed, just that the hashing outcome was coincidental.
            eprintln!(
                "Note: Unbounded distribution ({}:{}, {}:{}) matched unit_scale ({}:{}, {}:{}), which is possible but less likely for many items.",
                container_a_id,
                a_count_unbounded,
                container_b_id,
                b_count_unbounded,
                container_a_id,
                a_count_unit,
                container_b_id,
                b_count_unit
            );
        }
        // The primary check is that the function runs and assigns all items when unbounded_scale_fn is used.
        // The internal mechanism change (quotas -> usize::MAX) is what unbounded_scale_fn achieves.
        // A more direct test of "ignoring quota" would be if a_count_unbounded > a_count_unit,
        // but this is not guaranteed.
        // For example, if a_count_unit was 1, and a_count_unbounded is 5, that's a clear sign.
        // If a_count_unit was 1, and a_count_unbounded is 0, that's also possible with unlucky hashing.
    }

    #[test]
    fn assign_items_custom_scale_increases_quota() {
        let mut containers = BTreeMap::new();
        containers.insert(1, NonZeroUsize::new(1).unwrap());
        containers.insert(2, NonZeroUsize::new(1).unwrap());
        let items: Vec<i32> = (0..10).collect(); // 10 items
        let salt = 0u8;

        // With unit_scale, C1 and C2 would get 5 items each.
        // We'll use a custom scale factor of 1.5.
        // Initial quota for C1: 5. Scaled: ceil(5 * 1.5) = ceil(7.5) = 8.
        // Initial quota for C2: 5. Scaled: ceil(5 * 1.5) = ceil(7.5) = 8.
        // Total scaled quota = 8 + 8 = 16, which is >= items.len().
        let assignment =
            assign_items_weighted_with_scale_fn(&containers, &items, salt, custom_scale_fn(1.5));

        let c1_count = assignment.get(&1).map_or(0, Vec::len);
        let c2_count = assignment.get(&2).map_or(0, Vec::len);

        // Since total items (10) is less than total scaled quota (16),
        // the items will be distributed according to rendezvous hashing UP TO the scaled quotas.
        // The sum must be 10.
        assert_eq!(c1_count + c2_count, items.len());
        // And each container's count should not exceed its scaled quota (8).
        assert!(
            c1_count <= 8,
            "C1 count {} should be <= scaled quota 8",
            c1_count
        );
        assert!(
            c2_count <= 8,
            "C2 count {} should be <= scaled quota 8",
            c2_count
        );
        // It's hard to assert exact distribution without knowing hashes, but they are capped.
        // We know that if unit_scale was used, it would be 5,5.
        // Here, it could be 5,5 or 6,4 or 4,6 etc., up to 8 for one.
    }

    // --- Tests for compute_worker_quotas ---

    #[test]
    fn test_compute_worker_quotas_remainder_determinism() {
        // Two workers, equal weight, one actor each.
        // extra_vnodes = 1. One of them gets the remainder.
        let workers_setup: BTreeMap<u8, NonZeroUsize> = vec![
            (1, NonZeroUsize::new(1).unwrap()), // Worker 1
            (2, NonZeroUsize::new(1).unwrap()), // Worker 2
        ]
        .into_iter()
        .collect();
        let mut actor_counts_setup = HashMap::new();
        actor_counts_setup.insert(1, 1);
        actor_counts_setup.insert(2, 1);
        let total_vnodes_setup = 3; // base_total = 2, extra_vnodes = 1

        // Run with salt1
        let quotas1 = compute_worker_quotas(
            &workers_setup,
            &actor_counts_setup,
            total_vnodes_setup,
            10u8, // salt1
        );
        let worker1_quota1 = quotas1[&1].get();
        let worker2_quota1 = quotas1[&2].get();
        assert!(
            (worker1_quota1 == 2 && worker2_quota1 == 1)
                || (worker1_quota1 == 1 && worker2_quota1 == 2)
        );

        // Run with salt2 (different salt)
        let quotas2 = compute_worker_quotas(
            &workers_setup,
            &actor_counts_setup,
            total_vnodes_setup,
            20u8, // salt2
        );
        let worker1_quota2 = quotas2[&1].get();
        let worker2_quota2 = quotas2[&2].get();
        assert!(
            (worker1_quota2 == 2 && worker2_quota2 == 1)
                || (worker1_quota2 == 1 && worker2_quota2 == 2)
        );

        // If stable_hash is truly stable and sensitive to salt/id,
        // and if the hash values for (10,1) vs (10,2) differ from (20,1) vs (20,2)
        // in a way that changes the sort order of remainders (if remainders were equal),
        // then the assignment of the single extra vnode *might* switch.
        // This test primarily ensures it's deterministic for a given salt (covered by other tests)
        // and that one of them gets the remainder.
        // To make it more specific, one would need to mock or know stable_hash behavior.
        // For now, we assume that if the hashes differ sufficiently, the order *could* change.
        // A stronger test: if we knew worker 1 hashes lower than worker 2 for salt1,
        // and worker 2 hashes lower than worker 1 for salt2 (for the tie-breaking part),
        // we could predict who gets the remainder.
        // This test is more about exercising the path.
    }

    #[test]
    fn test_compute_worker_quotas_no_extra_vnodes() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(3).unwrap()),
        ]
        .into_iter()
        .collect();
        let mut actor_counts = HashMap::new();
        actor_counts.insert(1, 2); // Worker 1, 2 actors
        actor_counts.insert(2, 1); // Worker 2, 1 actor
        let total_vnodes = 3; // base_total = 2 + 1 = 3. So extra_vnodes = 0.
        let salt = 0u8;

        let quotas = compute_worker_quotas(&workers, &actor_counts, total_vnodes, salt);
        assert_eq!(quotas.len(), 2);
        assert_eq!(
            quotas[&1].get(),
            2,
            "Worker 1 quota should be its base_quota"
        );
        assert_eq!(
            quotas[&2].get(),
            1,
            "Worker 2 quota should be its base_quota"
        );
        let sum: usize = quotas.values().map(|q| q.get()).sum();
        assert_eq!(sum, total_vnodes);
    }

    #[test]
    #[should_panic] // Or expect specific error if compute_worker_quotas returns Result
    fn test_compute_worker_quotas_empty_actors_with_vnodes() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![(1, NonZeroUsize::new(1).unwrap())]
            .into_iter()
            .collect();
        let actor_counts: HashMap<u8, usize> = HashMap::new(); // No active workers
        let total_vnodes = 5; // But vnodes exist
        let salt = 0u8;

        // This scenario: extra_vnodes = 5, active_workers is empty, total_weight = 0.
        // Division by zero in `ideal_extra / total_weight`.
        let _ = compute_worker_quotas(&workers, &actor_counts, total_vnodes, salt);
    }

    // --- Tests for assign_hierarchical ---

    #[test]
    fn assign_hierarchical_capacity_unbounded() {
        let mut workers: BTreeMap<u8, NonZeroUsize> = BTreeMap::new();
        workers.insert(1, NonZeroUsize::new(1).unwrap()); // Low weight worker
        workers.insert(2, NonZeroUsize::new(100).unwrap()); // High weight worker
        let actors: Vec<u16> = (0..10).collect(); // 10 actors
        let vnodes: Vec<u16> = (0..10).collect(); // 10 vnodes
        let salt = 33u8;

        // With CapacityMode::Weighted, worker 1 would get very few (or 0) actors.
        // With CapacityMode::Unbounded, actor assignment is by rendezvous hash only.
        let assignment = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            salt,
            CapacityMode::Unbounded, // Key change here
            BalancedBy::ActorCounts, // VNode balancing doesn't matter as much if actors are skewed
        )
        .unwrap();

        let actors_on_w1 = assignment.get(&1).map_or(0, |amap| amap.len());
        let actors_on_w2 = assignment.get(&2).map_or(0, |amap| amap.len());

        assert_eq!(
            actors_on_w1 + actors_on_w2,
            actors.len(),
            "All actors must be assigned"
        );
        // It's hard to predict exact distribution without knowing hashes.
        // But, it's possible worker 1 (low weight) gets a non-trivial number of actors
        // if it wins some rendezvous hashes.
        // A weak assertion: the distribution might not be extremely skewed like 0:10 or 1:9.
        // If actors_on_w1 > 1 (more than its "fair share" if weights were strictly followed for small N),
        // it indicates unbounded mode is having an effect.
        // For this test, we'll just ensure all actors are assigned and the structure is valid.
        // A more robust test would involve items known to hash to specific workers.
        let total_assigned_vnodes: usize = assignment
            .values()
            .flat_map(|amap| amap.values().map(Vec::len))
            .sum();
        assert_eq!(
            total_assigned_vnodes,
            vnodes.len(),
            "All vnodes must be assigned"
        );
    }

    #[test]
    fn assign_hierarchical_compare_balanced_by_modes() {
        let mut workers: BTreeMap<u8, NonZeroUsize> = BTreeMap::new();
        workers.insert(1, NonZeroUsize::new(1).unwrap()); // Worker 1, low raw weight
        workers.insert(2, NonZeroUsize::new(9).unwrap()); // Worker 2, high raw weight
        // Actors will be distributed somewhat according to worker weights (1:9)
        // Let's say 2 actors on W1, 18 on W2 for a total of 20 actors.
        // For simplicity, let's make actor distribution more even for the test.
        // We'll use Unbounded to try and get a mix of actors on both.
        // Or, use enough actors so both get some with Weighted mode.
        let actors: Vec<u16> = (0..10).collect(); // 10 actors
        let vnodes: Vec<u16> = (0..100).collect(); // 100 vnodes, plenty to show distribution
        let salt = 77u8;

        // Assign actors first (CapacityMode::Weighted to see effect of worker weights)
        // W1 gets ~1 actor, W2 gets ~9 actors.
        let actor_assignment_for_setup =
            assign_items_weighted_with_scale_fn(&workers, &actors, salt, unit_scale);
        let actors_on_w1_count = actor_assignment_for_setup.get(&1).map_or(0, Vec::len);
        let actors_on_w2_count = actor_assignment_for_setup.get(&2).map_or(0, Vec::len);

        // Scenario 1: BalancedBy::RawWorkerWeights
        let assignment_raw = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            salt,
            CapacityMode::Weighted, // Actors distributed by worker weight (1:9)
            BalancedBy::RawWorkerWeights,
        )
        .unwrap();

        let vnodes_on_w1_raw: usize = assignment_raw
            .get(&1)
            .map_or(0, |amap| amap.values().map(Vec::len).sum());
        let vnodes_on_w2_raw: usize = assignment_raw
            .get(&2)
            .map_or(0, |amap| amap.values().map(Vec::len).sum());
        assert_eq!(vnodes_on_w1_raw + vnodes_on_w2_raw, vnodes.len());
        // With RawWorkerWeights, vnode distribution should also be skewed towards W2 (original 1:9 weights)
        // after base actor counts are met.
        // W1 has actors_on_w1_count, W2 has actors_on_w2_count.
        // Base vnodes: actors_on_w1_count for W1, actors_on_w2_count for W2.
        // Extra vnodes = 100 - (actors_on_w1_count + actors_on_w2_count) = 100 - 10 = 90.
        // These 90 extra vnodes are split 1:9. W1 gets 90*1/10=9. W2 gets 90*9/10=81.
        // Total for W1_raw = actors_on_w1_count + 9.
        // Total for W2_raw = actors_on_w2_count + 81.
        // Since actors_on_w1_count is small (e.g. 1) and actors_on_w2_count is large (e.g. 9),
        // W1_raw ~ 1+9=10. W2_raw ~ 9+81=90. Ratio is 1:9.
        if vnodes_on_w1_raw > 0 && vnodes_on_w2_raw > 0 {
            // Avoid division by zero if one worker gets no vnodes
            let ratio_raw = vnodes_on_w2_raw as f64 / vnodes_on_w1_raw as f64;
            assert!(
                ratio_raw > 5.0 && ratio_raw < 15.0,
                "Expected RawWorkerWeights ratio around 9, got {}",
                ratio_raw
            ); // Roughly 9x
        } else if vnodes_on_w2_raw > 0 {
            assert!(
                actors_on_w1_count == 0 || vnodes_on_w1_raw >= actors_on_w1_count,
                "W1 raw vnodes check"
            );
        }

        // Scenario 2: BalancedBy::ActorCounts
        let assignment_actors = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            salt,
            CapacityMode::Weighted, // Actors distributed by worker weight (1:9)
            BalancedBy::ActorCounts,
        )
        .unwrap();

        let vnodes_on_w1_actors: usize = assignment_actors
            .get(&1)
            .map_or(0, |amap| amap.values().map(Vec::len).sum());
        let vnodes_on_w2_actors: usize = assignment_actors
            .get(&2)
            .map_or(0, |amap| amap.values().map(Vec::len).sum());
        assert_eq!(vnodes_on_w1_actors + vnodes_on_w2_actors, vnodes.len());
        // With ActorCounts, vnode distribution is weighted by number of actors on each worker.
        // Weights for vnode dist: actors_on_w1_count vs actors_on_w2_count.
        // Ratio of vnodes should be actors_on_w2_count / actors_on_w1_count.
        // E.g. if actors are 1 on W1, 9 on W2, then vnodes should also be ~1:9.
        if actors_on_w1_count > 0
            && actors_on_w2_count > 0
            && vnodes_on_w1_actors > 0
            && vnodes_on_w2_actors > 0
        {
            let expected_actor_ratio = actors_on_w2_count as f64 / actors_on_w1_count as f64;
            let actual_vnode_ratio_actors = vnodes_on_w2_actors as f64 / vnodes_on_w1_actors as f64;
            // Check if actual ratio is close to expected actor ratio
            assert!(
                (actual_vnode_ratio_actors - expected_actor_ratio).abs() < 2.0, /* Allow some leeway due to integer division */
                "Expected ActorCounts vnode ratio around {}, got {}",
                expected_actor_ratio,
                actual_vnode_ratio_actors
            );
        } else if vnodes_on_w2_actors > 0 {
            assert!(
                actors_on_w1_count == 0 || vnodes_on_w1_actors >= actors_on_w1_count,
                "W1 actorcount vnodes check"
            );
        }
        // The key insight is that if actor distribution is itself skewed (e.g. 1:9 due to worker weights),
        // then BalancedBy::ActorCounts will *also* result in a 1:9 vnode distribution.
        // If actors were distributed 5:5 (e.g. using Unbounded mode and lucky hashing),
        // then ActorCounts would lead to a 5:5 vnode distribution, while RawWorkerWeights would still aim for 1:9.
        // This test, as written, shows both modes might produce similar *vnode* ratios if the *actor* ratio matches the *raw worker weight* ratio.
        // A better test for contrast would be:
        // 1. Workers with very different raw weights (e.g., W1:1, W2:9).
        // 2. Distribute actors such that they are *evenly* spread (e.g., 5 actors on W1, 5 on W2). This might require CapacityMode::Unbounded and careful salt/ID selection, or just manually setting up `actor_to_worker` for a more direct test of the second stage.
        // 3. Then, BalancedBy::RawWorkerWeights should distribute vnodes 1:9.
        // 4. And, BalancedBy::ActorCounts should distribute vnodes 5:5 (i.e., 1:1).
    }

    #[test]
    fn assign_hierarchical_vnode_to_actor_round_robin() {
        let workers: BTreeMap<u8, NonZeroUsize> = vec![(1, NonZeroUsize::new(1).unwrap())]
            .into_iter()
            .collect();
        // Worker 1 has 3 actors
        let actors: Vec<u16> = vec![10, 20, 30];
        // Worker 1 is assigned 5 vnodes
        let vnodes: Vec<u16> = vec![100, 200, 300, 400, 500];

        let assignment = assign_hierarchical(
            &workers,
            &actors,
            &vnodes,
            0u8,
            CapacityMode::Weighted,  // All actors go to the single worker
            BalancedBy::ActorCounts, // All vnodes go to the single worker
        )
        .unwrap();

        assert_eq!(assignment.len(), 1, "Should be one worker");
        let actor_map = assignment.get(&1).unwrap();
        assert_eq!(actor_map.len(), 3, "Should be three actors on the worker");

        let vnodes_actor10 = actor_map.get(&10).unwrap(); // actor_list[0]
        let vnodes_actor20 = actor_map.get(&20).unwrap(); // actor_list[1]
        let vnodes_actor30 = actor_map.get(&30).unwrap(); // actor_list[2]

        // Expected distribution for 5 vnodes over 3 actors (round-robin):
        // vnode 100 -> actor 10 (index 0 % 3 = 0)
        // vnode 200 -> actor 20 (index 1 % 3 = 1)
        // vnode 300 -> actor 30 (index 2 % 3 = 2)
        // vnode 400 -> actor 10 (index 3 % 3 = 0)
        // vnode 500 -> actor 20 (index 4 % 3 = 1)
        // So, actor 10 gets [100, 400] (2 vnodes)
        // actor 20 gets [200, 500] (2 vnodes)
        // actor 30 gets [300]      (1 vnode)
        assert_eq!(vnodes_actor10.len(), 2);
        assert_eq!(vnodes_actor20.len(), 2);
        assert_eq!(vnodes_actor30.len(), 1);

        // Check actual items if vnode order is preserved by assign_items_weighted_with_scale_fn
        // (it is, as items are iterated in input order).
        // And if actor_to_worker preserves actor order from input (it does due to BTreeMap from items).
        // And if vnode_to_worker preserves vnode order from input.
        // This part is tricky because the exact vnodes assigned depend on hashing if there were multiple workers.
        // Here, all vnodes go to worker 1, so their order *should* be preserved.
        // The actors on worker 1 will be [10, 20, 30] if `assign_items_weighted_with_scale_fn` preserves order of items when forming Vec<I>.
        // Let's assume `assign_items_weighted_with_scale_fn` appends items as they are assigned.
        // The order of items in `assignment.get(container).unwrap()` is the order they were assigned by rendezvous.
        // The vnodes in `vnode_to_worker.get(&worker).cloned().unwrap_or_default()` are also in rendezvous assignment order.
        // So, the `assigned_vnodes.into_iter().enumerate()` will iterate them in that specific order.
        // The `actor_list` is `actor_to_worker.get(&worker).unwrap()`, which is also in rendezvous assignment order for actors.

        // To make this test robust for content, we need to ensure the vnodes [100..500] are indeed assigned in that order
        // to the single worker, and actors [10,20,30] are also assigned in that order.
        // With a single container, assign_items_weighted_with_scale_fn should preserve item order.
        assert!(vnodes_actor10.contains(&100) && vnodes_actor10.contains(&400));
        assert!(vnodes_actor20.contains(&200) && vnodes_actor20.contains(&500));
        assert!(vnodes_actor30.contains(&300));
    }
}
