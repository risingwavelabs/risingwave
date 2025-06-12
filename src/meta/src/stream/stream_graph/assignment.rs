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

use anyhow::{Context, Result, anyhow, ensure};

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
/// - `capacity_scale_factor_fn`: Callback `(containers, items) -> Option<ScaleFactor>`:
///     - `Some(f)`: Scale each container’s base quota by `f` (ceiled, but never below base).
///     - `None`: Remove upper bound (capacity = `usize::MAX`).
///
/// # Returns
/// A `BTreeMap<C, Vec<I>>` mapping each container to the list of assigned items.
/// - If either `containers` or `items` is empty, return an empty map.
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
/// # use risingwave_meta::stream::{assign_items_weighted_with_scale_fn, weighted_scale};
///
/// let mut caps = BTreeMap::new();
/// caps.insert("fast", NonZeroUsize::new(3).unwrap());
/// caps.insert("slow", NonZeroUsize::new(1).unwrap());
///
/// let tasks = vec!["task1", "task2", "task3", "task4"];
/// let result = assign_items_weighted_with_scale_fn(&caps, &tasks, 0u8, weighted_scale);
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
    capacity_scale_factor_fn: impl Fn(&BTreeMap<C, NonZeroUsize>, &[I]) -> Option<ScaleFactor>,
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
            // Use saturating multiplication to prevent overflow, even though saturation is highly unlikely in practice.
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
                let scaled_f64 = (info.quota as f64 * f.get()).ceil();
                let scaled = if scaled_f64 >= usize::MAX as f64 {
                    usize::MAX
                } else {
                    scaled_f64 as usize
                };
                (info.container, scaled.max(info.quota))
            }
            None => (info.container, usize::MAX),
        })
        .collect();

    // Prepare assignment map
    let mut assignment: BTreeMap<C, Vec<I>> = BTreeMap::new();

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
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    t.hash(&mut hasher);
    hasher.finish()
}

/// A validated, non-negative, finite scale factor.
#[derive(Debug, Copy, Clone)]
pub struct ScaleFactor(f64);

impl ScaleFactor {
    /// Creates a new `ScaleFactor` if the value is valid.
    ///
    /// A valid scale factor must be finite and non-negative.
    pub fn new(value: f64) -> Option<Self> {
        if value.is_finite() && value >= 0.0 {
            Some(ScaleFactor(value))
        } else {
            None
        }
    }

    /// Gets the inner f64 value.
    pub fn get(&self) -> f64 {
        self.0
    }
}

/// A no-op capacity scaling function: always returns `None`.
pub fn unbounded_scale<C, I>(
    _containers: &BTreeMap<C, NonZeroUsize>,
    _items: &[I],
) -> Option<ScaleFactor> {
    None
}

/// A unit capacity scaling function: always returns `Some(1.0)`.
pub fn weighted_scale<C, I>(
    _containers: &BTreeMap<C, NonZeroUsize>,
    _items: &[I],
) -> Option<ScaleFactor> {
    ScaleFactor::new(1.0)
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
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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

    assert!(
        base_total <= total_vnodes,
        "Total vnodes ({}) must be greater than or equal to the sum of actor counts ({})",
        total_vnodes,
        base_total
    );

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

/// A lightweight struct to represent a chunk of `VNodes` during assignment.
/// This is an internal implementation detail.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct VnodeChunk(u32);

impl From<usize> for VnodeChunk {
    fn from(id: usize) -> Self {
        // Assuming VNode IDs do not exceed u32::MAX for simplicity.
        Self(id as u32)
    }
}

impl VnodeChunk {
    fn id(&self) -> usize {
        // Convert back to usize for external use.
        self.0 as usize
    }
}

/// Defines the VNode chunking strategy for assignment.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum VnodeChunkingStrategy {
    /// Each VNode is assigned individually. This is the default.
    NoChunking,

    /// The chunk size is automatically determined to maximize VNode contiguity,
    /// ensuring that the number of chunks is at least the number of actors.
    MaximizeContiguity,
}

/// Core assigner with configurable strategies.
pub struct Assigner<S> {
    salt: S,
    actor_capacity: CapacityMode,
    balance_strategy: BalancedBy,
    vnode_chunking_strategy: VnodeChunkingStrategy,
}

/// Builder for [`Assigner`].
#[derive(Debug)]
pub struct AssignerBuilder<S> {
    salt: S,
    actor_capacity: CapacityMode,
    balance_strategy: BalancedBy,
    vnode_chunking_strategy: VnodeChunkingStrategy,
}

impl<S: Hash + Copy> AssignerBuilder<S> {
    /// Create a new builder with the given salt.
    pub fn new(salt: S) -> Self {
        Self {
            salt,
            actor_capacity: CapacityMode::Weighted,
            balance_strategy: BalancedBy::RawWorkerWeights,
            vnode_chunking_strategy: VnodeChunkingStrategy::NoChunking,
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

    /// Sets the vnode chunking strategy.
    pub fn with_vnode_chunking_strategy(&mut self, strategy: VnodeChunkingStrategy) -> &mut Self {
        self.vnode_chunking_strategy = strategy;
        self
    }

    /// Finalize and build the [`Assigner`].
    pub fn build(&self) -> Assigner<S> {
        Assigner {
            salt: self.salt,
            actor_capacity: self.actor_capacity,
            balance_strategy: self.balance_strategy,
            vnode_chunking_strategy: self.vnode_chunking_strategy,
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
        ensure!(
            !workers.is_empty(),
            "no workers to assign; assignment is meaningless"
        );
        ensure!(
            !actors.is_empty(),
            "no actors to assign; assignment is meaningless"
        );
        ensure!(
            !vnodes.is_empty(),
            "no vnodes to assign; assignment is meaningless"
        );
        ensure!(
            vnodes.len() >= actors.len(),
            "not enough vnodes ({}) for actors ({}); each actor needs at least one vnode",
            vnodes.len(),
            actors.len()
        );

        let chunk_size = match self.vnode_chunking_strategy {
            VnodeChunkingStrategy::NoChunking => {
                return assign_hierarchical(
                    workers,
                    actors,
                    vnodes,
                    self.salt,
                    self.actor_capacity,
                    self.balance_strategy,
                )
                .context("hierarchical assignment failed");
            }

            VnodeChunkingStrategy::MaximizeContiguity => {
                // Automatically calculate chunk size to be as large as possible
                // while ensuring every actor can receive at least one chunk.
                // The `.max(1)` ensures the chunk size is at least 1.
                (vnodes.len() / actors.len()).max(1)
            }
        };

        // Calculate the number of chunks using ceiling division.
        let num_chunks = vnodes.len().div_ceil(chunk_size);

        // The `MaximizeContiguity` strategy inherently ensures `num_chunks >= actors.len()`.
        // This assertion serves as a sanity check for our logic.
        debug_assert!(
            num_chunks >= actors.len(),
            "Invariant violation: MaximizeContiguity should always produce enough chunks."
        );

        // Create VNode chunks to be used as the items for assignment.
        let chunks: Vec<VnodeChunk> = (0..num_chunks).map(VnodeChunk::from).collect();

        // Call the underlying hierarchical assignment function with chunks as items.
        let chunk_assignment = assign_hierarchical(
            workers,
            actors,
            &chunks,
            self.salt,
            self.actor_capacity,
            self.balance_strategy,
        )
        .context("hierarchical assignment of chunks failed")?;

        // Convert the assignment of `VnodeChunk` back to an assignment of the original `V`.
        let mut final_assignment = BTreeMap::new();
        for (worker, actor_map) in chunk_assignment {
            let mut new_actor_map = BTreeMap::new();
            for (actor, assigned_chunks) in actor_map {
                // Expand the list of chunks into a flat list of VNodes.
                let assigned_vnodes: Vec<V> = assigned_chunks
                    .into_iter()
                    .flat_map(|chunk| {
                        let start_idx = chunk.id() * chunk_size;
                        // Ensure the end index does not go out of bounds.
                        let end_idx = (start_idx + chunk_size).min(vnodes.len());
                        // Get the corresponding VNodes from the original slice.
                        vnodes[start_idx..end_idx].iter().copied()
                    })
                    .collect();

                if !assigned_vnodes.is_empty() {
                    new_actor_map.insert(actor, assigned_vnodes);
                }
            }
            if !new_actor_map.is_empty() {
                final_assignment.insert(worker, new_actor_map);
            }
        }

        Ok(final_assignment)
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

    // --- Tests for `assign_items_weighted_with_scale_fn` ---

    #[test]
    fn empty_containers_or_items_yields_empty_map() {
        let empty_containers: BTreeMap<&str, NonZeroUsize> = BTreeMap::new();
        let items = vec![1, 2, 3];
        let result =
            assign_items_weighted_with_scale_fn(&empty_containers, &items, 0u8, weighted_scale);
        assert!(
            result.is_empty(),
            "Expected empty map when containers empty"
        );

        let mut containers = BTreeMap::new();
        containers.insert("c1", NonZeroUsize::new(1).unwrap());
        let empty_items: Vec<i32> = Vec::new();
        let result2 =
            assign_items_weighted_with_scale_fn(&containers, &empty_items, 0u8, weighted_scale);
        assert!(result2.is_empty(), "Expected empty map when items empty");
    }

    #[test]
    fn single_container_receives_all_items() {
        let mut containers = BTreeMap::new();
        containers.insert("only", NonZeroUsize::new(5).unwrap());
        let items = vec![10, 20, 30];

        let assignment =
            assign_items_weighted_with_scale_fn(&containers, &items, 1u8, weighted_scale);

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

        let result = assign_items_weighted_with_scale_fn(&containers, &items, 2u8, weighted_scale);
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

        let result = assign_items_weighted_with_scale_fn(&containers, &items, 5u8, weighted_scale);
        let x_count = result.get(&"X").map(Vec::len).unwrap_or(0);
        let y_count = result.get(&"Y").map(Vec::len).unwrap_or(0);
        assert_eq!(x_count + y_count, items.len(), "All items must be assigned");
        assert!(
            x_count == 1 && y_count == 2,
            "Container X should get 1 items, the other 2, but got {} and {}",
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

        let result = assign_items_weighted_with_scale_fn(&containers, &items, 7u8, weighted_scale);
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

        let out1 = assign_items_weighted_with_scale_fn(&containers, &items, 42u8, weighted_scale);
        let out2 = assign_items_weighted_with_scale_fn(&containers, &items, 42u8, weighted_scale);
        assert_eq!(out1, out2, "Same salt should produce identical assignments");
    }

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
            assign_items_weighted_with_scale_fn(&containers, &items, salt, weighted_scale);
        let a_count_unit = assignment_unit.get(container_a_id).map_or(0, Vec::len);
        let b_count_unit = assignment_unit.get(container_b_id).map_or(0, Vec::len);

        assert!(
            a_count_unit < 10,
            "With weighted_scale, A ({}) should have few items, got {}",
            container_a_id,
            a_count_unit
        );
        assert!(
            b_count_unit > 90,
            "With weighted_scale, B ({}) should have many items, got {}",
            container_b_id,
            b_count_unit
        );
        assert_eq!(
            a_count_unit + b_count_unit,
            items.len(),
            "All items must be assigned in weighted_scale"
        );

        // 2. Assignment with unbounded_scale_fn (quotas are usize::MAX)
        let assignment_unbounded =
            assign_items_weighted_with_scale_fn(&containers, &items, salt, unbounded_scale);
        let a_count_unbounded = assignment_unbounded.get(container_a_id).map_or(0, Vec::len);
        let b_count_unbounded = assignment_unbounded.get(container_b_id).map_or(0, Vec::len);

        assert_eq!(
            a_count_unbounded + b_count_unbounded,
            items.len(),
            "All items must be assigned in unbounded_scale"
        );
    }

    // --- Tests for `compute_worker_quotas` ---

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

    /// This test verifies the core invariant of `compute_worker_quotas`:
    /// the sum of all calculated quotas must exactly equal the total number of vnodes provided.
    /// It runs through multiple scenarios to ensure this property holds under various conditions.
    #[test]
    fn test_compute_worker_quotas_sum_is_preserved() {
        // Helper function to run a single test case and assert the invariant.
        fn run_quota_sum_test<W, S>(
            scenario_name: &str,
            workers: &BTreeMap<W, NonZeroUsize>,
            actor_counts: &HashMap<W, usize>,
            total_vnodes: usize,
            salt: S,
        ) where
            W: Ord + Copy + Hash + Eq + Debug,
            S: Hash + Copy,
        {
            let quotas = compute_worker_quotas(workers, actor_counts, total_vnodes, salt);
            let sum_of_quotas: usize = quotas.values().map(|q| q.get()).sum();

            assert_eq!(
                sum_of_quotas, total_vnodes,
                "Scenario '{}' failed: Sum of quotas ({}) does not equal total_vnodes ({})",
                scenario_name, sum_of_quotas, total_vnodes
            );
        }

        // --- Scenario 1: Even split with no remainder ---
        let workers1: BTreeMap<_, _> = [
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(1).unwrap()),
            (3, NonZeroUsize::new(1).unwrap()),
        ]
        .into();
        let actor_counts1: HashMap<_, _> = [(1, 2), (2, 2), (3, 2)].into();
        run_quota_sum_test(
            "Even split, no remainder",
            &workers1,
            &actor_counts1,
            12,
            0u8,
        );

        // --- Scenario 2: Uneven split with remainder ---
        // This is the most common case, testing the remainder distribution logic.
        let workers2: BTreeMap<_, _> = [
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(2).unwrap()),
            (3, NonZeroUsize::new(3).unwrap()),
        ]
        .into();
        let actor_counts2: HashMap<_, _> = [(1, 1), (2, 5), (3, 2)].into();
        run_quota_sum_test(
            "Uneven split with remainder",
            &workers2,
            &actor_counts2,
            101,
            42u64,
        );

        // --- Scenario 3: No extra vnodes to distribute ---
        // The total vnodes exactly match the sum of base actor counts.
        let workers3: BTreeMap<_, _> = [
            (1, NonZeroUsize::new(10).unwrap()),
            (2, NonZeroUsize::new(20).unwrap()),
        ]
        .into();
        let actor_counts3: HashMap<_, _> = [(1, 5), (2, 10)].into();
        run_quota_sum_test("No extra vnodes", &workers3, &actor_counts3, 15, 0u8);

        // --- Scenario 4: Only one active worker ---
        // All vnodes should be assigned to the single worker with actors.
        let workers4: BTreeMap<_, _> = [
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(1).unwrap()),
        ]
        .into();
        let actor_counts4: HashMap<_, _> = [(1, 10)].into();
        run_quota_sum_test("Single active worker", &workers4, &actor_counts4, 100, 0u8);

        // --- Scenario 5: Large and complex numbers ---
        // Stress test with larger, non-trivial numbers.
        let workers5: BTreeMap<_, _> = [
            (1, NonZeroUsize::new(7).unwrap()),
            (2, NonZeroUsize::new(13).unwrap()),
            (3, NonZeroUsize::new(19).unwrap()),
            (4, NonZeroUsize::new(23).unwrap()),
        ]
        .into();
        let actor_counts5: HashMap<_, _> = [(1, 111), (2, 222), (3, 33), (4, 4)].into();
        run_quota_sum_test(
            "Large and complex numbers",
            &workers5,
            &actor_counts5,
            99991,
            12345u64,
        );
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

    // --- Tests for `assign_hierarchical` ---

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
    fn two_workers_balanced_by_actor_counts() {
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
    fn raw_worker_weights_respects_worker_weight() {
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
            assign_items_weighted_with_scale_fn(&workers, &actors, salt, weighted_scale);
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
    }
}

#[cfg(test)]
mod extra_tests_for_scale_factor {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    use super::*;

    #[test]
    fn test_scale_factor_constructor_rejects_invalid_values() {
        assert!(ScaleFactor::new(f64::NAN).is_none(), "Should reject NaN");
        assert!(
            ScaleFactor::new(f64::INFINITY).is_none(),
            "Should reject Infinity"
        );
        assert!(
            ScaleFactor::new(f64::NEG_INFINITY).is_none(),
            "Should reject Negative Infinity"
        );
        assert!(
            ScaleFactor::new(-1.0).is_none(),
            "Should reject negative numbers"
        );
        assert!(ScaleFactor::new(1.0).is_some(), "Should accept valid value");
    }

    #[test]
    fn test_assign_items_scale_factor_less_than_one() {
        // A scale factor < 1.0 should not reduce quotas below their base value.
        // Therefore, the distribution should be identical to a scale factor of 1.0.
        let mut containers = BTreeMap::new();
        containers.insert("A", NonZeroUsize::new(3).unwrap());
        containers.insert("B", NonZeroUsize::new(1).unwrap());
        let items: Vec<i32> = (0..4).collect(); // 4 items

        // Base quotas: A gets 3, B gets 1.
        let result_scale_one =
            assign_items_weighted_with_scale_fn(&containers, &items, 0u8, weighted_scale);

        fn custom_scale_fn(_: &BTreeMap<&str, NonZeroUsize>, _: &[i32]) -> Option<ScaleFactor> {
            ScaleFactor::new(0.5)
        }
        let result_scale_half =
            assign_items_weighted_with_scale_fn(&containers, &items, 0u8, custom_scale_fn);

        assert_eq!(
            result_scale_one[&"A"].len(),
            3,
            "With scale=1.0, A should get 3"
        );
        assert_eq!(
            result_scale_half[&"A"].len(),
            3,
            "With scale=0.5, A's quota should not be reduced, still gets 3"
        );
        assert_eq!(
            result_scale_one, result_scale_half,
            "Distributions should be identical"
        );
    }

    #[test]
    fn test_assign_items_large_scale_factor_does_not_panic() {
        // This test ensures that a very large scale factor, which would cause
        // `quota * factor` to exceed `usize::MAX`, is clamped correctly and does not panic.
        let mut containers = BTreeMap::new();
        containers.insert("A", NonZeroUsize::new(1).unwrap());
        let items: Vec<i32> = vec![100]; // A single item, quota is 100.

        // A huge scale factor that would definitely overflow if not clamped.
        fn huge_scale_fn(_: &BTreeMap<&str, NonZeroUsize>, _: &[i32]) -> Option<ScaleFactor> {
            ScaleFactor::new(f64::MAX)
        }

        // The test passes if this function call does not panic.
        let assignment =
            assign_items_weighted_with_scale_fn(&containers, &items, 0u8, huge_scale_fn);

        // All items should still be assigned correctly.
        assert_eq!(assignment[&"A"].len(), items.len());
    }
}

#[cfg(test)]
mod affinity_tests {
    use std::collections::{BTreeMap, HashMap};
    use std::num::NonZeroUsize;

    use super::*;

    // --- Helper function to analyze affinity ---

    /// Flattens the hierarchical assignment into a simple VNode -> Worker map.
    pub(crate) fn get_vnode_to_worker_map<W, A, V>(
        assignment: &BTreeMap<W, BTreeMap<A, Vec<V>>>,
    ) -> HashMap<V, W>
    where
        W: Copy + Eq + Hash,
        A: Copy + Eq + Hash,
        V: Copy + Eq + Hash,
    {
        let mut map = HashMap::new();
        for (&worker, actor_map) in assignment {
            for vnode_list in actor_map.values() {
                for &vnode in vnode_list {
                    map.insert(vnode, worker);
                }
            }
        }
        map
    }

    /// Helper to run the affinity test for worker weight changes.
    fn run_weight_change_affinity_test(capacity_mode: CapacityMode, balanced_by: BalancedBy) {
        let initial_workers: BTreeMap<u8, _> = [
            (1, NonZeroUsize::new(5).unwrap()),
            (2, NonZeroUsize::new(5).unwrap()),
        ]
        .into();
        let actors: Vec<u16> = (0..100).collect();
        let vnodes: Vec<u32> = (0..1000).collect();
        let salt = 123u8;

        let initial_assignment = assign_hierarchical(
            &initial_workers,
            &actors,
            &vnodes,
            salt,
            capacity_mode,
            balanced_by,
        )
        .unwrap();
        let initial_map = get_vnode_to_worker_map(&initial_assignment);

        let mut changed_workers: BTreeMap<u8, _> = BTreeMap::new();
        changed_workers.insert(1, NonZeroUsize::new(2).unwrap());
        changed_workers.insert(2, NonZeroUsize::new(8).unwrap());

        let new_assignment = assign_hierarchical(
            &changed_workers,
            &actors,
            &vnodes,
            salt,
            capacity_mode,
            balanced_by,
        )
        .unwrap();
        let new_map = get_vnode_to_worker_map(&new_assignment);

        let stable_vnodes = initial_map
            .iter()
            .filter(|(v, w)| new_map.get(v) == Some(w))
            .count();
        let stability_percentage = (stable_vnodes as f64 / vnodes.len() as f64) * 100.0;

        println!(
            "Affinity for {:?}/{:?}: {:.2}% of vnodes remained stable when weights changed from {:?} to {:?}.",
            capacity_mode, balanced_by, stability_percentage, initial_workers, changed_workers
        );

        assert!(
            stability_percentage < 100.0,
            "Expected some vnodes to move for {:?}/{:?}",
            capacity_mode,
            balanced_by
        );
        assert!(
            stability_percentage > 50.0,
            "Expected a majority of vnodes to have affinity for {:?}/{:?}",
            capacity_mode,
            balanced_by
        );
    }

    /// Helper to run the affinity test for actor count changes.
    fn run_actor_count_change_affinity_test(capacity_mode: CapacityMode, balanced_by: BalancedBy) {
        let workers: BTreeMap<u8, _> = [
            (1, NonZeroUsize::new(5).unwrap()),
            (2, NonZeroUsize::new(5).unwrap()),
        ]
        .into();
        let initial_actors: Vec<u16> = (0..100).collect();
        let vnodes: Vec<u32> = (0..1000).collect();
        let salt = 123u8;

        let initial_assignment = assign_hierarchical(
            &workers,
            &initial_actors,
            &vnodes,
            salt,
            capacity_mode,
            balanced_by,
        )
        .unwrap();
        let initial_map = get_vnode_to_worker_map(&initial_assignment);

        let changed_actors: Vec<u16> = (0..120).collect();
        let new_assignment = assign_hierarchical(
            &workers,
            &changed_actors,
            &vnodes,
            salt,
            capacity_mode,
            balanced_by,
        )
        .unwrap();
        let new_map = get_vnode_to_worker_map(&new_assignment);

        let stable_vnodes = initial_map
            .iter()
            .filter(|(v, w)| new_map.get(v) == Some(w))
            .count();
        let stability_percentage = (stable_vnodes as f64 / vnodes.len() as f64) * 100.0;

        println!(
            "Affinity for {:?}/{:?}: {:.2}% of vnodes remained stable when actor count changed from {} to {}.",
            capacity_mode,
            balanced_by,
            stability_percentage,
            initial_actors.len(),
            changed_actors.len(),
        );

        // The expected affinity depends heavily on the balancing strategy.
        match balanced_by {
            BalancedBy::RawWorkerWeights => {
                // Actor count change has minimal impact on vnode distribution.
                assert!(
                    stability_percentage > 90.0,
                    "Expected very high affinity for RawWorkerWeights"
                );
            }
            BalancedBy::ActorCounts => {
                // Actor count change directly impacts vnode distribution weights, causing more churn.
                assert!(
                    stability_percentage > 50.0,
                    "Expected moderate affinity for ActorCounts"
                );
            }
        }
    }

    #[test]
    fn test_affinity_when_worker_weights_change_all_modes() {
        let modes = [
            (CapacityMode::Weighted, BalancedBy::RawWorkerWeights),
            (CapacityMode::Weighted, BalancedBy::ActorCounts),
            (CapacityMode::Unbounded, BalancedBy::RawWorkerWeights),
            (CapacityMode::Unbounded, BalancedBy::ActorCounts),
        ];
        for (capacity_mode, balanced_by) in modes {
            run_weight_change_affinity_test(capacity_mode, balanced_by);
        }
    }

    #[test]
    fn test_affinity_when_actor_count_changes_all_modes() {
        let modes = [
            (CapacityMode::Weighted, BalancedBy::RawWorkerWeights),
            (CapacityMode::Weighted, BalancedBy::ActorCounts),
            (CapacityMode::Unbounded, BalancedBy::RawWorkerWeights),
            (CapacityMode::Unbounded, BalancedBy::ActorCounts),
        ];
        for (capacity_mode, balanced_by) in modes {
            run_actor_count_change_affinity_test(capacity_mode, balanced_by);
        }
    }
}

#[cfg(test)]
mod affinity_tests_horizon_scaling {
    use std::cmp::Ordering;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::num::NonZeroUsize;

    use affinity_tests::get_vnode_to_worker_map;

    use super::*;

    /// A struct to hold the results of a generic affinity analysis.
    #[derive(Debug)]
    struct AffinityAnalysis {
        /// Percentage of vnodes on surviving workers that remained stable.
        stability_on_survivors_pct: f64,
        /// Percentage of total vnodes that moved to newly added workers.
        moved_to_new_workers_pct: f64,
        /// Percentage of total vnodes that did not change their worker assignment.
        overall_stability_pct: f64,
    }

    /// A generic function to analyze affinity between two states.
    /// It can handle scale-out, scale-in, and no-change scenarios.
    fn analyze_cluster_change(
        initial_map: &HashMap<u32, u8>,
        new_map: &HashMap<u32, u8>,
        initial_workers: &BTreeMap<u8, NonZeroUsize>,
        new_workers: &BTreeMap<u8, NonZeroUsize>,
    ) -> AffinityAnalysis {
        let initial_keys: HashSet<_> = initial_workers.keys().copied().collect();
        let new_keys: HashSet<_> = new_workers.keys().copied().collect();

        let surviving_workers: HashSet<_> = initial_keys.intersection(&new_keys).copied().collect();
        let added_workers: HashSet<_> = new_keys.difference(&initial_keys).copied().collect();

        let total_vnodes = initial_map.len();
        if total_vnodes == 0 {
            return AffinityAnalysis {
                stability_on_survivors_pct: 100.0,
                moved_to_new_workers_pct: 0.0,
                overall_stability_pct: 100.0,
            };
        }

        let mut stable_vnodes_overall = 0;
        let mut moved_to_new_worker_count = 0;

        for (vnode, &initial_worker) in initial_map {
            if let Some(&new_worker) = new_map.get(vnode) {
                if initial_worker == new_worker {
                    stable_vnodes_overall += 1;
                } else if added_workers.contains(&new_worker) {
                    moved_to_new_worker_count += 1;
                }
            }
        }

        let vnodes_on_survivors_initially = initial_map
            .values()
            .filter(|w| surviving_workers.contains(w))
            .count();
        let stable_on_survivors = initial_map
            .iter()
            .filter(|(_, w)| surviving_workers.contains(w))
            .filter(|(v, w)| new_map.get(v) == Some(w))
            .count();

        AffinityAnalysis {
            stability_on_survivors_pct: if vnodes_on_survivors_initially > 0 {
                (stable_on_survivors as f64 / vnodes_on_survivors_initially as f64) * 100.0
            } else {
                100.0 // No survivors, so stability is vacuously 100%
            },
            moved_to_new_workers_pct: (moved_to_new_worker_count as f64 / total_vnodes as f64)
                * 100.0,
            overall_stability_pct: (stable_vnodes_overall as f64 / total_vnodes as f64) * 100.0,
        }
    }

    #[test]
    fn test_generic_cluster_resize_affinity_all_modes() {
        struct TestCase {
            name: &'static str,
            initial_size: usize,
            final_size: usize,
        }

        let test_cases = [
            TestCase {
                name: "Scale In (3 -> 2)",
                initial_size: 3,
                final_size: 2,
            },
            TestCase {
                name: "Scale Out (2 -> 3)",
                initial_size: 2,
                final_size: 3,
            },
            TestCase {
                name: "Scale In (5 -> 4)",
                initial_size: 5,
                final_size: 4,
            },
            TestCase {
                name: "Scale Out (4 -> 5)",
                initial_size: 4,
                final_size: 5,
            },
            TestCase {
                name: "No Change (3 -> 3)",
                initial_size: 3,
                final_size: 3,
            },
            TestCase {
                name: "Scale Double (4 -> 8)",
                initial_size: 4,
                final_size: 8,
            },
            TestCase {
                name: "Scale Half (8 -> 4)",
                initial_size: 8,
                final_size: 4,
            },
        ];

        let modes = [
            (CapacityMode::Weighted, BalancedBy::RawWorkerWeights),
            (CapacityMode::Weighted, BalancedBy::ActorCounts),
            (CapacityMode::Unbounded, BalancedBy::RawWorkerWeights),
            (CapacityMode::Unbounded, BalancedBy::ActorCounts),
        ];

        let actors: Vec<u16> = (0..100).collect();
        let vnodes: Vec<u32> = (0..1000).collect();
        let salt = 123u8;

        for case in &test_cases {
            for (capacity_mode, balanced_by) in modes {
                println!(
                    "--- Running Test: {} with {:?}/{:?} ---",
                    case.name, capacity_mode, balanced_by
                );

                let initial_workers: BTreeMap<_, _> = (1..=case.initial_size as u8)
                    .map(|i| (i, NonZeroUsize::new(5).unwrap()))
                    .collect();
                let final_workers: BTreeMap<_, _> = (1..=case.final_size as u8)
                    .map(|i| (i, NonZeroUsize::new(5).unwrap()))
                    .collect();

                let initial_assignment = assign_hierarchical(
                    &initial_workers,
                    &actors,
                    &vnodes,
                    salt,
                    capacity_mode,
                    balanced_by,
                )
                .unwrap();
                let initial_map = get_vnode_to_worker_map(&initial_assignment);

                let new_assignment = assign_hierarchical(
                    &final_workers,
                    &actors,
                    &vnodes,
                    salt,
                    capacity_mode,
                    balanced_by,
                )
                .unwrap();
                let new_map = get_vnode_to_worker_map(&new_assignment);

                let analysis = analyze_cluster_change(
                    &initial_map,
                    &new_map,
                    &initial_workers,
                    &final_workers,
                );

                match case.final_size.cmp(&case.initial_size) {
                    Ordering::Less => {
                        // Scale In
                        println!(
                            "  Result: Stability on survivors = {:.2}%",
                            analysis.stability_on_survivors_pct
                        );
                        assert!(
                            analysis.stability_on_survivors_pct > 90.0,
                            "Expected very high stability on surviving nodes during scale-in"
                        );
                    }
                    Ordering::Equal => {
                        // No Change
                        println!(
                            "  Result: Overall stability = {:.2}%",
                            analysis.overall_stability_pct
                        );
                        assert_eq!(
                            analysis.overall_stability_pct, 100.0,
                            "Expected 100% stability when cluster size does not change"
                        );
                    }
                    Ordering::Greater => {
                        // Scale Out
                        let expected_move_rate = (case.final_size - case.initial_size) as f64
                            / case.final_size as f64
                            * 100.0;
                        let expected_stability_rate =
                            case.initial_size as f64 / case.final_size as f64 * 100.0;

                        println!(
                            "  Result: Overall stability = {:.2}% (Expected ~{:.2}%), Moved to new = {:.2}% (Expected ~{:.2}%)",
                            analysis.overall_stability_pct,
                            expected_stability_rate,
                            analysis.moved_to_new_workers_pct,
                            expected_move_rate
                        );

                        // Assert that the actual values are within a reasonable tolerance of the expected values.
                        assert!(
                            (analysis.moved_to_new_workers_pct - expected_move_rate).abs() < 10.0,
                            "Move rate to new nodes is outside expected tolerance"
                        );
                        assert!(
                            (analysis.overall_stability_pct - expected_stability_rate).abs() < 10.0,
                            "Overall stability is outside expected tolerance"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod affinity_tests_vertical_scaling {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    use affinity_tests::get_vnode_to_worker_map;
    use risingwave_common::util::iter_util::ZipEqFast;

    use super::*;

    /// A struct to define a test case for worker weight changes.
    struct WeightChangeTestCase {
        name: &'static str,
        initial_weights: Vec<usize>,
        final_weights: Vec<usize>,
    }

    /// A generic helper that runs a single weight change test case for a given mode.
    fn run_weight_change_test_case(
        case: &WeightChangeTestCase,
        capacity_mode: CapacityMode,
        balanced_by: BalancedBy,
    ) {
        let actors: Vec<u16> = (0..100).collect();
        let vnodes: Vec<u32> = (0..1000).collect();
        let salt = 123u8;

        let initial_workers: BTreeMap<u8, _> = case
            .initial_weights
            .iter()
            .enumerate()
            .map(|(i, &w)| (i as u8 + 1, NonZeroUsize::new(w).unwrap()))
            .collect();
        let final_workers: BTreeMap<u8, _> = case
            .final_weights
            .iter()
            .enumerate()
            .map(|(i, &w)| (i as u8 + 1, NonZeroUsize::new(w).unwrap()))
            .collect();

        let initial_assignment = assign_hierarchical(
            &initial_workers,
            &actors,
            &vnodes,
            salt,
            capacity_mode,
            balanced_by,
        )
        .unwrap();
        let initial_map = get_vnode_to_worker_map(&initial_assignment);
        let new_assignment = assign_hierarchical(
            &final_workers,
            &actors,
            &vnodes,
            salt,
            capacity_mode,
            balanced_by,
        )
        .unwrap();
        let new_map = get_vnode_to_worker_map(&new_assignment);

        let stable_vnodes = initial_map
            .iter()
            .filter(|(v, w)| new_map.get(v) == Some(w))
            .count();
        let actual_stability_pct = (stable_vnodes as f64 / vnodes.len() as f64) * 100.0;

        println!(
            "--- Running Test: '{}' with {:?}/{:?} ---",
            case.name, capacity_mode, balanced_by
        );
        println!(
            "  Result: {:.2}% of vnodes remained stable.",
            actual_stability_pct
        );

        // --- Dynamic Assertions ---
        let expected_stability_pct = match balanced_by {
            BalancedBy::RawWorkerWeights => {
                // For RawWorkerWeights, churn is based on the change in worker weight ratios.
                let initial_total_weight: usize = case.initial_weights.iter().sum();
                let final_total_weight: usize = case.final_weights.iter().sum();
                let expected_moved: f64 = case
                    .initial_weights
                    .iter()
                    .zip_eq_fast(case.final_weights.iter())
                    .map(|(&iw, &fw)| {
                        let initial_share =
                            vnodes.len() as f64 * (iw as f64 / initial_total_weight as f64);
                        let final_share =
                            vnodes.len() as f64 * (fw as f64 / final_total_weight as f64);
                        (final_share - initial_share).max(0.0)
                    })
                    .sum();
                (vnodes.len() as f64 - expected_moved) / vnodes.len() as f64 * 100.0
            }
            BalancedBy::ActorCounts => {
                // For ActorCounts, churn is based on the change in actor distribution,
                // which itself is determined by worker weights (if CapacityMode is Weighted).
                let initial_actor_dist = AssignerBuilder::new(salt)
                    .build()
                    .count_actors_per_worker(&initial_workers, actors.len());
                let final_actor_dist = AssignerBuilder::new(salt)
                    .build()
                    .count_actors_per_worker(&final_workers, actors.len());

                let initial_total_actors: usize = initial_actor_dist.values().sum();
                let final_total_actors: usize = final_actor_dist.values().sum();

                let expected_moved: f64 = (1..=initial_workers.len() as u8)
                    .map(|worker_id| {
                        let initial_actors_on_worker =
                            *initial_actor_dist.get(&worker_id).unwrap_or(&0);
                        let final_actors_on_worker =
                            *final_actor_dist.get(&worker_id).unwrap_or(&0);
                        let initial_share = vnodes.len() as f64
                            * (initial_actors_on_worker as f64 / initial_total_actors as f64);
                        let final_share = vnodes.len() as f64
                            * (final_actors_on_worker as f64 / final_total_actors as f64);
                        (final_share - initial_share).max(0.0)
                    })
                    .sum();
                (vnodes.len() as f64 - expected_moved) / vnodes.len() as f64 * 100.0
            }
        };

        println!(
            "  Expectation for this mode: ~{:.2}% stability.",
            expected_stability_pct
        );

        assert!(
            (actual_stability_pct - expected_stability_pct).abs() < 10.0,
            "Stability is outside the expected tolerance for this mode."
        );
    }

    #[test]
    fn test_generic_weight_change_affinity_all_modes() {
        let test_cases = [
            WeightChangeTestCase {
                name: "Uniform Scaling (No relative change) #1",
                initial_weights: vec![5, 5],
                final_weights: vec![10, 10],
            },
            WeightChangeTestCase {
                name: "Uniform Scaling (No relative change) #2",
                initial_weights: vec![8, 8],
                final_weights: vec![4, 4],
            },
            WeightChangeTestCase {
                name: "Single Worker Weight Decrease",
                initial_weights: vec![5, 5, 5],
                final_weights: vec![2, 5, 5],
            },
            WeightChangeTestCase {
                name: "Single Worker Weight Increase",
                initial_weights: vec![5, 5, 5],
                final_weights: vec![8, 5, 5],
            },
            WeightChangeTestCase {
                name: "Complex Rebalance",
                initial_weights: vec![5, 5],
                final_weights: vec![2, 8],
            },
        ];

        let modes = [
            (CapacityMode::Weighted, BalancedBy::RawWorkerWeights),
            (CapacityMode::Weighted, BalancedBy::ActorCounts),
            (CapacityMode::Unbounded, BalancedBy::RawWorkerWeights),
            (CapacityMode::Unbounded, BalancedBy::ActorCounts),
        ];

        for case in &test_cases {
            for (capacity_mode, balanced_by) in modes {
                run_weight_change_test_case(case, capacity_mode, balanced_by);
            }
        }
    }
}

#[cfg(test)]
mod assigner_test {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    use super::*;

    // Helper function to create a BTreeMap of workers
    fn create_workers(weights: &[(u8, usize)]) -> BTreeMap<u8, NonZeroUsize> {
        weights
            .iter()
            .map(|(id, w)| (*id, NonZeroUsize::new(*w).unwrap()))
            .collect()
    }

    #[test]
    fn test_maximize_contiguity_basic_assignment() {
        // 2 workers, 4 actors, 100 vnodes.
        // Expected chunk_size = floor(100 / 4) = 25.
        // Expected num_chunks = ceil(100 / 25) = 4.
        let workers = create_workers(&[(1, 1), (2, 1)]);
        let actors: Vec<u16> = (0..4).collect();
        let vnodes: Vec<u32> = (0..100).collect();

        let assigner = AssignerBuilder::new(0u8)
            .with_vnode_chunking_strategy(VnodeChunkingStrategy::MaximizeContiguity)
            .build();

        let assignment = assigner
            .assign_hierarchical(&workers, &actors, &vnodes)
            .unwrap();

        let mut total_vnodes_assigned = 0;
        let mut all_assigned_vnodes = BTreeMap::new();

        for (_, actor_map) in assignment {
            for (actor_id, vnodes) in actor_map {
                total_vnodes_assigned += vnodes.len();
                // Each actor should receive exactly one chunk of 25 vnodes.
                assert_eq!(
                    vnodes.len(),
                    25,
                    "Actor {} should get a full chunk",
                    actor_id
                );
                // Verify that the assigned vnodes are contiguous.
                for i in 0..(vnodes.len() - 1) {
                    assert_eq!(vnodes[i] + 1, vnodes[i + 1], "VNodes should be contiguous");
                }
                all_assigned_vnodes.insert(vnodes[0], vnodes);
            }
        }

        assert_eq!(
            total_vnodes_assigned,
            vnodes.len(),
            "All vnodes must be assigned"
        );
        // Check if the chunks are correct: 0-24, 25-49, 50-74, 75-99
        assert!(all_assigned_vnodes.contains_key(&0));
        assert!(all_assigned_vnodes.contains_key(&25));
        assert!(all_assigned_vnodes.contains_key(&50));
        assert!(all_assigned_vnodes.contains_key(&75));
    }

    #[test]
    fn test_maximize_contiguity_non_divisible_vnodes() {
        // 4 actors, 103 vnodes.
        // Expected chunk_size = floor(103 / 4) = 25.
        // Expected num_chunks = ceil(103 / 25) = 5.
        let workers = create_workers(&[(1, 1)]);
        let actors: Vec<u16> = (0..4).collect();
        let vnodes: Vec<u32> = (0..103).collect();

        let assigner = AssignerBuilder::new(0u8)
            .with_vnode_chunking_strategy(VnodeChunkingStrategy::MaximizeContiguity)
            .build();

        let assignment = assigner
            .assign_hierarchical(&workers, &actors, &vnodes)
            .unwrap();
        let actor_map = assignment.get(&1).unwrap();

        // Collect the number of vnodes assigned to each actor.
        let vnode_counts: Vec<usize> = actor_map.values().map(Vec::len).collect();

        // 1. Verify the total number of assigned vnodes.
        let total_assigned: usize = vnode_counts.iter().sum();
        assert_eq!(total_assigned, vnodes.len(), "All vnodes must be assigned");
        assert_eq!(
            vnode_counts.len(),
            actors.len(),
            "Each actor must have an entry"
        );

        // 2. Verify the distribution of vnode counts.
        // The final counts depend on how the chunks {25, 25, 25, 25, 3} are distributed.
        // The actor who gets two chunks could get:
        //   25 + 25 = 50
        //   25 + 3 = 28
        // The other actors get one chunk, so their counts will be 25 or 3.

        // We can count how many actors got each possible number of vnodes.
        let mut counts_distribution = BTreeMap::new();
        for count in vnode_counts {
            *counts_distribution.entry(count).or_insert(0) += 1;
        }

        assert_eq!(
            counts_distribution,
            BTreeMap::from([(25, 3), (28, 1)]),
            "The distribution of vnode counts is unexpected. Got: {:?}",
            counts_distribution
        );
    }

    #[test]
    fn test_actors_gt_vnodes_fails() {
        // 10 actors, 5 vnodes. This should fail at the top level.
        let workers = create_workers(&[(1, 1)]);
        let actors: Vec<u16> = (0..10).collect();
        let vnodes: Vec<u32> = (0..5).collect();

        let assigner = AssignerBuilder::new(0u8).build();

        let result = assigner.assign_hierarchical(&workers, &actors, &vnodes);
        assert!(result.is_err());
        // The error comes from the inner `assign_hierarchical` call.
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not enough vnodes (5) for actors (10)")
        );
    }

    #[test]
    fn test_maximize_contiguity_actors_eq_vnodes() {
        // 10 actors, 10 vnodes.
        // Expected chunk_size = floor(10 / 10) = 1.
        // This should behave identically to NoChunking.
        let workers = create_workers(&[(1, 1), (2, 1)]);
        let actors: Vec<u16> = (0..10).collect();
        let vnodes: Vec<u32> = (0..10).collect();

        let assigner_contiguity = AssignerBuilder::new(42u8)
            .with_vnode_chunking_strategy(VnodeChunkingStrategy::MaximizeContiguity)
            .build();
        let assignment_contiguity = assigner_contiguity
            .assign_hierarchical(&workers, &actors, &vnodes)
            .unwrap();

        let assigner_no_chunking = AssignerBuilder::new(42u8)
            .with_vnode_chunking_strategy(VnodeChunkingStrategy::NoChunking)
            .build();
        let assignment_no_chunking = assigner_no_chunking
            .assign_hierarchical(&workers, &actors, &vnodes)
            .unwrap();

        // With the same salt, the results should be identical.
        assert_eq!(assignment_contiguity, assignment_no_chunking);

        // Also verify each actor gets exactly one vnode.
        let total_vnodes: usize = assignment_contiguity
            .values()
            .flat_map(|amap| amap.values().map(Vec::len))
            .sum();
        assert_eq!(total_vnodes, vnodes.len());
        assert!(
            assignment_contiguity
                .values()
                .all(|amap| amap.values().all(|v| v.len() == 1))
        );
    }

    #[test]
    fn test_maximize_contiguity_single_actor() {
        // 1 actor, 1000 vnodes.
        // Expected chunk_size = floor(1000 / 1) = 1000.
        // All vnodes should go to the single actor.
        let workers = create_workers(&[(1, 1)]);
        let actors: Vec<u16> = vec![100];
        let vnodes: Vec<u32> = (0..1000).collect();

        let assigner = AssignerBuilder::new(0u8)
            .with_vnode_chunking_strategy(VnodeChunkingStrategy::MaximizeContiguity)
            .build();

        let assignment = assigner
            .assign_hierarchical(&workers, &actors, &vnodes)
            .unwrap();

        // Check that only one worker has assignments.
        assert_eq!(assignment.len(), 1);
        let actor_map = assignment.get(&1).unwrap();

        // Check that only the single actor has assignments.
        assert_eq!(actor_map.len(), 1);
        let assigned_vnodes = actor_map.get(&100).unwrap();

        // Check that the actor received all vnodes.
        assert_eq!(assigned_vnodes.len(), vnodes.len());
        // Check that they are the correct vnodes.
        assert_eq!(*assigned_vnodes, vnodes);
    }

    #[test]
    fn test_maximize_contiguity_differs_from_no_chunking() {
        // Use a setup where the difference will be clear.
        // 2 workers, 2 actors, 4 vnodes.
        // MaximizeContiguity: chunk_size = floor(4/2) = 2. Two chunks: [0,1], [2,3].
        // Each actor gets one chunk. Actor 0 might get [0,1] and Actor 1 [2,3].
        // NoChunking: vnodes 0,1,2,3 are assigned independently. It's highly likely
        // they will be distributed between the two actors, not in contiguous blocks.
        let workers = create_workers(&[(1, 1)]);
        let actors: Vec<u16> = vec![0, 1];
        let vnodes: Vec<u32> = vec![0, 1, 2, 3];
        let salt = 123u8; // A fixed salt to make it deterministic.

        let assigner_contiguity = AssignerBuilder::new(salt)
            .with_vnode_chunking_strategy(VnodeChunkingStrategy::MaximizeContiguity)
            .build();
        let assignment_contiguity = assigner_contiguity
            .assign_hierarchical(&workers, &actors, &vnodes)
            .unwrap();

        let assigner_no_chunking = AssignerBuilder::new(salt)
            .with_vnode_chunking_strategy(VnodeChunkingStrategy::NoChunking)
            .build();
        let assignment_no_chunking = assigner_no_chunking
            .assign_hierarchical(&workers, &actors, &vnodes)
            .unwrap();

        // The assignments should be different.
        assert_ne!(
            assignment_contiguity, assignment_no_chunking,
            "Expected different assignments for the two strategies"
        );

        // Verify contiguity for the MaximizeContiguity result.
        let actor_map_contiguity = assignment_contiguity.get(&1).unwrap();
        for vnodes in actor_map_contiguity.values() {
            assert_eq!(vnodes.len(), 2, "Each actor should get a chunk of size 2");
            assert_eq!(vnodes[0] + 1, vnodes[1], "VNodes must be contiguous");
        }
    }
}
