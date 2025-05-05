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

use anyhow::{Result, anyhow};

/// Assign items to containers without any capacity limit.
///
/// # Parameters
/// - `containers`: Map of container identifiers to their non-zero weights.
/// - `items`: Slice of items to distribute.
/// - `salt`: Hashing salt to vary the distribution of remainder buckets and weighted rendezvous.
///
/// # Returns
/// A map from each container to its assigned items.
pub fn assign_items_unbounded<C, I, S>(
    containers: &BTreeMap<C, NonZeroUsize>,
    items: &[I],
    salt: S,
) -> HashMap<C, Vec<I>>
where
    C: Ord + Hash + Eq + Copy + Clone + Debug,
    I: Hash + Eq + Copy + Clone + Debug,
    S: Hash + Copy,
{
    assign_items_weighted_with_scale_fn(containers, items, salt, |_, _| None)
}

/// Assign items to containers proportionally by weight with a default scale factor of 1.0.
///
/// # Parameters
/// - `containers`: Map of container identifiers to their non-zero weights.
/// - `items`: Slice of items to distribute.
/// - `salt`: Hashing salt to vary the distribution of remainder buckets and weighted rendezvous.
///
/// # Returns
/// A map from each container to its assigned items.
pub fn assign_items_weighted<C, I, S>(
    containers: &BTreeMap<C, NonZeroUsize>,
    items: &[I],
    salt: S,
) -> HashMap<C, Vec<I>>
where
    C: Ord + Hash + Eq + Copy + Clone + Debug,
    I: Hash + Eq + Copy + Clone + Debug,
    S: Hash + Copy,
{
    assign_items_weighted_with_scale_fn(containers, items, salt, |_, _| Some(1.0))
}

/// Assign items to containers based on their weight proportion and a salting mechanism.
///
/// This function distributes a slice of items (`&[I]`) among a set of containers (`BTreeMap<C, NonZeroUsize>`)
/// according to a two-phase algorithm:
///
/// 1. **Quota Calculation (integer-based)**:
///    - Compute the total weight of all containers as u128.
///    - For each container with weight `w`:
///      - `ideal_num = items.len() as u128 * w.get() as u128`
///      - `base_quota = ideal_num / total_weight`
///      - `remainder_part = ideal_num % total_weight`
///    - Sum all `base_quota` as initial assignment; let `rem_count = items.len() - sum(base_quota)`.
///    - Distribute `rem_count` extra slots to containers by descending `remainder_part`. Ties broken by hashing `(container, salt)`.
///
/// 2. **Capacity Scaling**:
///    - Apply an optional scale factor (`Option<f64>`) to each `base_quota`:
///      - If `Some(f)`, `scaled = ceil(base_quota as f64 * f)` but at least `base_quota`.
///      - If `None`, unlimited capacity (`usize::MAX`).
///
/// 3. **Weighted Rendezvous Assignment**:
///    - For each item, compute `raw = stable_hash((item, container, salt))`,
///      normalize to `r = (raw + 1) / (MAX+2)`, and `key = -ln(r) / weight`.
///    - Assign to the container with the smallest `key`.
///
/// # Panics
/// Panics if the sum of weights is zero or if an invariant is violated (no eligible container).
pub fn assign_items_weighted_with_scale_fn<C, I, S>(
    containers: &BTreeMap<C, NonZeroUsize>,
    items: &[I],
    salt: S,
    capacity_scale_factor_fn: impl Fn(&BTreeMap<C, NonZeroUsize>, &[I]) -> Option<f64>,
) -> HashMap<C, Vec<I>>
where
    C: Ord + Hash + Eq + Copy + Clone + Debug,
    I: Hash + Eq + Copy + Clone + Debug,
    S: Hash + Copy,
{
    // Early exit if there is nothing to assign
    if containers.is_empty() || items.is_empty() {
        return containers.keys().map(|&c| (c, vec![])).collect();
    }

    // Integer-based quota calculation
    let total_weight: u128 = containers.values().map(|w| w.get() as u128).sum();
    assert!(
        total_weight > 0,
        "Sum of container weights must be non-zero"
    );

    struct Info<C> {
        container: C,
        quota: usize,
        rem_part: u128,
    }

    let mut infos: Vec<Info<C>> = containers
        .iter()
        .map(|(&container, &weight)| {
            let ideal_num = (items.len() as u128).saturating_mul(weight.get() as u128);
            Info {
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
    let mut assignment: HashMap<C, Vec<I>> = containers.keys().map(|&c| (c, Vec::new())).collect();

    // Assign each item using Weighted Rendezvous
    for &item in items {
        let mut best: Option<(C, f64)> = None;
        for (&container, &weight) in containers {
            let assigned = assignment.get(&container).map(Vec::len).unwrap_or(0);
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    use super::*;

    const SALT_FOR_TEST: u64 = 42;

    macro_rules! nz {
        ($e:expr) => {
            NonZeroUsize::new($e).unwrap()
        };
    }

    #[test]
    fn evenly_distributes_equal_weights() {
        let mut containers: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
        containers.insert(1, nz!(1));
        containers.insert(2, nz!(1));
        containers.insert(3, nz!(1));
        let items: Vec<u32> = (0..9).collect();
        let result = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
        for (&c, v) in &result {
            assert_eq!(v.len(), 3, "Container {:?} expected 3 items", c);
        }
    }

    #[test]
    fn respects_weight_ratios() {
        let mut containers: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
        containers.insert(10, nz!(1));
        containers.insert(20, nz!(3));
        let items: Vec<u32> = (0..40).collect();
        let result = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
        let c1 = result.get(&10).unwrap().len();
        let c2 = result.get(&20).unwrap().len();
        assert!(
            c2 > c1 * 2 && c2 < c1 * 4,
            "Weight ratio violated: c1={} c2={}",
            c1,
            c2
        );
    }

    #[test]
    fn handles_empty_inputs() {
        let empty: BTreeMap<u32, NonZeroUsize> = BTreeMap::new();
        let items = vec![1, 2, 3];
        assert!(assign_items_weighted(&empty, &items, SALT_FOR_TEST).is_empty());

        let mut one: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
        one.insert(42, nz!(10));
        let none: Vec<u32> = Vec::new();
        let result = assign_items_weighted(&one, &none, SALT_FOR_TEST);
        assert_eq!(result.get(&42).unwrap().len(), 0);
    }

    #[test]
    fn scale_factor_increases_capacity() {
        let mut containers: BTreeMap<char, NonZeroUsize> = BTreeMap::new();
        containers.insert('A', nz!(1));
        containers.insert('B', nz!(1));
        let items: Vec<u32> = (0..4).collect();
        let result =
            assign_items_weighted_with_scale_fn(&containers, &items, SALT_FOR_TEST, |_, _| {
                Some(2.0)
            });
        assert_eq!(
            result.get(&'A').unwrap().len() + result.get(&'B').unwrap().len(),
            4
        );
    }

    #[test]
    fn exact_quota_match_for_small_weights() {
        let mut containers: BTreeMap<char, NonZeroUsize> = BTreeMap::new();
        containers.insert('A', nz!(1));
        containers.insert('B', nz!(2));
        let items: Vec<u8> = (0..3).collect();
        let result = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
        let a_count = result.get(&'A').unwrap().len();
        let b_count = result.get(&'B').unwrap().len();
        assert_eq!(a_count, 1, "Expected A to get exactly 1 item");
        assert_eq!(b_count, 2, "Expected B to get exactly 2 items");
    }

    #[test]
    fn handles_more_containers_than_items() {
        let mut containers: BTreeMap<&str, NonZeroUsize> = BTreeMap::new();
        containers.insert("X", nz!(1));
        containers.insert("Y", nz!(1));
        containers.insert("Z", nz!(1));
        let items = vec![100, 200];
        let result = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
        let non_empty = result.values().filter(|v| !v.is_empty()).count();
        assert_eq!(
            non_empty, 2,
            "Exactly two containers should receive an item"
        );
        let zero = result.values().filter(|v| v.is_empty()).count();
        assert_eq!(zero, 1, "Exactly one container should receive zero items");
    }

    #[test]
    fn deterministic_across_invocations() {
        let mut containers: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
        containers.insert(10, nz!(3));
        containers.insert(20, nz!(5));
        let items: Vec<i32> = (0..20).collect();
        let first = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
        let second = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
        assert_eq!(first, second, "Assignment must be deterministic");
    }

    #[test]
    fn scale_less_than_one_behaves_as_one() {
        let mut containers: BTreeMap<char, NonZeroUsize> = BTreeMap::new();
        containers.insert('A', nz!(1));
        containers.insert('B', nz!(1));
        let items: Vec<u8> = (0..6).collect();
        let default = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
        let scaled_down =
            assign_items_weighted_with_scale_fn(&containers, &items, SALT_FOR_TEST, |_, _| {
                Some(0.5)
            });
        assert_eq!(default, scaled_down, "scale < 1 should not alter quotas");
    }

    #[test]
    fn scale_factor_limits_are_respected() {
        let mut containers: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
        containers.insert(1, nz!(1));
        containers.insert(2, nz!(1));
        let items: Vec<u32> = (0..4).collect();
        let result =
            assign_items_weighted_with_scale_fn(&containers, &items, SALT_FOR_TEST, |_, _| {
                Some(1.5)
            });
        for (&c, v) in &result {
            assert!(
                v.len() <= 3,
                "Container {:?} exceeded scaled quota of 3, got {}",
                c,
                v.len()
            );
        }
        let total_assigned: usize = result.values().map(|v| v.len()).sum();
        assert_eq!(total_assigned, 4, "All items should be assigned");
    }

    #[test]
    fn empty_inputs_return_empty_vectors() {
        let empty: BTreeMap<u32, NonZeroUsize> = BTreeMap::new();
        let items = vec![1, 2, 3];
        assert!(assign_items_weighted(&empty, &items, SALT_FOR_TEST).is_empty());

        let mut one: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
        one.insert(99, nz!(5));
        let none: Vec<i32> = Vec::new();
        let result = assign_items_weighted(&one, &none, SALT_FOR_TEST);
        assert_eq!(result.get(&99).unwrap().len(), 0);
    }
}

/// Distributes virtual nodes to actors via a two-layered weighted assignment:
///
/// 1. **Actors → Workers (Weighted)**
///    - Distribute actors across workers according to their provided weights.
/// 2. **`VNodes` → Active Workers (Equal Weight)**
///    - Consider only workers assigned at least one actor; treat each with weight = 1.
///    - Distribute vnodes uniformly across these active workers.
/// 3. **`VNodes` → Actors within Workers (Round-Robin)**
///    - For each worker, assign its vnodes evenly to its actors in a round-robin fashion.
///
/// # Type Parameters
/// - `W`: Worker identifier type. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
/// - `A`: Actor identifier type. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
/// - `V`: Virtual node type. Must implement `Hash + Eq + Copy + Clone + Debug`.
/// - `S`: Salt type for hashing tie-breakers. Must implement `Hash + Copy`.
///
/// # Parameters
/// - `workers`: A `BTreeMap<W, NonZeroUsize>` mapping each worker to its weight.
/// - `actors`: A slice of actors to distribute among workers.
/// - `vnodes`: A slice of virtual nodes to assign to actors.
/// - `salt`: A hashing salt to ensure deterministic yet varied tie-breaking.
///
/// # Returns
/// A nested `HashMap<W, HashMap<A, Vec<V>>>` mapping each worker to a map of actors and their assigned vnodes.
///
/// # Errors
/// Returns an error if the number of actors exceeds the number of vnodes.
///
/// # Example
/// ```rust
/// # use std::collections::BTreeMap;
/// # use std::num::NonZeroUsize;
///
/// # use crate::risingwave_meta::stream::assign_hierarchical_worker_oriented;
///
/// // Define worker weights
/// let mut workers = BTreeMap::new();
/// workers.insert("worker1", NonZeroUsize::new(3).unwrap());
/// workers.insert("worker2", NonZeroUsize::new(2).unwrap());
///
/// // Actors and virtual nodes
/// let actors = vec!["actorA", "actorB", "actorC"];
/// let vnodes = (0..6).collect::<Vec<_>>();
///
/// // Perform hierarchical assignment with salt = 42
/// let assignment =
///     assign_hierarchical_worker_oriented(&workers, &actors, &vnodes, 42u64).unwrap();
///
/// // Inspect results
/// for (worker, map) in assignment {
///     println!("{}:", worker);
///     for (actor, vlist) in map {
///         println!("  {} -> {:?}", actor, vlist);
///     }
/// }
/// ```
pub fn assign_hierarchical_worker_oriented<W, A, V, S>(
    workers: &BTreeMap<W, NonZeroUsize>,
    actors: &[A],
    vnodes: &[V],
    salt: S,
) -> Result<HashMap<W, HashMap<A, Vec<V>>>>
where
    W: Ord + Hash + Eq + Copy + Clone + Debug,
    A: Ord + Hash + Eq + Copy + Clone + Debug,
    V: Hash + Eq + Copy + Clone + Debug,
    S: Hash + Copy,
{
    assign_hierarchical(workers, actors, vnodes, salt, BalancedBy::RawWorkerWeights)
}

/// Assigns virtual nodes to actors with actor‑oriented balancing.
///
/// This convenience wrapper calls `assign_hierarchical` with `BalancedBy::ActorCounts`,
/// resulting in:
///
/// 1. **Actors → Workers (Weighted by worker weight)**
/// 2. **`VNodes` → Workers (Weighted by number of actors per worker)**
/// 3. **`VNodes` → Actors (Round‑robin within each worker)**
///
/// # Type Parameters
/// - `W`: Worker identifier. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
/// - `A`: Actor identifier. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
/// - `V`: Virtual node type. Must implement `Hash + Eq + Copy + Clone + Debug`.
/// - `S`: Salt type for hashing tie‑breakers. Must implement `Hash + Copy`.
///
/// # Parameters
/// - `workers`: A `BTreeMap<W, NonZeroUsize>` mapping each worker to its weight.
/// - `actors`: A slice of actors to assign.
/// - `vnodes`: A slice of virtual nodes to distribute.
/// - `salt`: A hashing salt to ensure deterministic yet varied tie‑breaking.
///
/// # Returns
/// A `HashMap<W, HashMap<A, Vec<V>>>` mapping each worker to its actors and assigned vnodes,
/// with balancing based on actor counts.
///
/// # Errors
/// Returns an error if `actors.len() > vnodes.len()`.
///
/// # Example
/// ```rust
/// # use std::collections::BTreeMap;
/// # use std::num::NonZeroUsize;
///
/// # use crate::risingwave_meta::stream::assign_hierarchical_actor_oriented;
///
/// // Define worker weights
/// let mut workers = BTreeMap::new();
/// workers.insert("w1", NonZeroUsize::new(4).unwrap());
/// workers.insert("w2", NonZeroUsize::new(2).unwrap());
///
/// // Actors and virtual nodes
/// let actors = vec!["a1", "a2", "a3"];
/// let vnodes = (0..6).collect::<Vec<_>>();
///
/// // Distribute with actor‑oriented balancing, salt = 99
/// let assignment = assign_hierarchical_actor_oriented(&workers, &actors, &vnodes, 99u64).unwrap();
///
/// // Validate structure
/// assert_eq!(assignment.len(), 2);
/// for (worker, actor_map) in &assignment {
///     // Each actor under this worker has at most one more vnode than any other
///     let counts: Vec<usize> = actor_map.values().map(Vec::len).collect();
///     let min = *counts.iter().min().unwrap();
///     let max = *counts.iter().max().unwrap();
///     assert!(max - min <= 1);
/// }
/// ```
pub fn assign_hierarchical_actor_oriented<W, A, V, S>(
    workers: &BTreeMap<W, NonZeroUsize>,
    actors: &[A],
    vnodes: &[V],
    salt: S,
) -> Result<HashMap<W, HashMap<A, Vec<V>>>>
where
    W: Ord + Hash + Eq + Copy + Clone + Debug,
    A: Ord + Hash + Eq + Copy + Clone + Debug,
    V: Hash + Eq + Copy + Clone + Debug,
    S: Hash + Copy,
{
    assign_hierarchical(workers, actors, vnodes, salt, BalancedBy::ActorCounts)
}

/// Defines the vnode distribution strategy for hierarchical assignment.
///
/// - `RawWorkerWeights`: Distribute vnodes across workers using the original worker weight values.
/// - `ActorCounts`: Distribute vnodes based on the number of actors assigned to each worker.
pub enum BalancedBy {
    /// Use each worker's raw weight when allocating vnodes.
    RawWorkerWeights,

    /// Use the count of actors per worker as the weight for vnode distribution.
    ActorCounts,
}

/// Hierarchically distributes virtual nodes to actors via two stages of weighted assignment.
///
/// This core function performs:
/// 1. **Actors → Workers**: Assigns each actor to a worker based on either raw worker weights or actor counts.
/// 2. **`VNodes` → Workers**: Distributes virtual nodes among the resulting active workers according to the selected strategy.
/// 3. **`VNodes` → Actors**: Within each worker, assigns its vnodes to its actors in a round-robin fashion.
///
/// # Type Parameters
/// - `W`: Worker identifier. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
/// - `A`: Actor identifier. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
/// - `V`: Virtual node type. Must implement `Hash + Eq + Copy + Clone + Debug`.
/// - `S`: Salt type for deterministic tie-breaking. Must implement `Hash + Copy`.
///
/// # Parameters
/// - `workers`: A `BTreeMap<W, NonZeroUsize>` mapping each worker to its weight.
/// - `actors`: A slice of actors to assign.
/// - `virtual_nodes`: A slice of virtual nodes to distribute.
/// - `salt`: A hashing salt to vary tie-break decisions per invocation.
/// - `balanced_by`: A `BalancedBy` enum deciding whether to balance vnodes by raw worker weights or by actor counts.
///
/// # Returns
/// A nested `HashMap<W, HashMap<A, Vec<V>>>` where each worker key maps to another map of its actors and their assigned vnodes.
///
/// # Errors
/// Returns an error if `actors.len() > virtual_nodes.len()`, since each actor must receive at least one vnode.
///
/// # Example
/// ```rust
/// # use std::collections::BTreeMap;
/// # use std::num::NonZeroUsize;
///
/// # use crate::risingwave_meta::stream::assign_hierarchical;
/// # use crate::risingwave_meta::stream::BalancedBy;
///
/// let mut workers = BTreeMap::new();
/// workers.insert("w1", NonZeroUsize::new(2).unwrap());
/// workers.insert("w2", NonZeroUsize::new(3).unwrap());
///
/// let actors = vec!["a1", "a2", "a3"];
/// let vnodes = (0..9).collect::<Vec<_>>();
///
/// // Balance vnodes by raw worker weight, salt = 7
/// let assignment = assign_hierarchical(
///     &workers,
///     &actors,
///     &vnodes,
///     7u8,
///     BalancedBy::RawWorkerWeights,
/// )
/// .unwrap();
///
/// for (worker, actor_map) in assignment {
///     println!("Worker {}: {:?}", worker, actor_map);
/// }
/// ```
pub fn assign_hierarchical<W, A, V, S>(
    workers: &BTreeMap<W, NonZeroUsize>,
    actors: &[A],
    virtual_nodes: &[V],
    salt: S,
    balanced_by: BalancedBy,
) -> Result<HashMap<W, HashMap<A, Vec<V>>>>
where
    W: Ord + Hash + Eq + Copy + Clone + Debug,
    A: Ord + Hash + Eq + Copy + Clone + Debug,
    V: Hash + Eq + Copy + Clone + Debug,
    S: Hash + Copy,
{
    // Validate input: ensure vnode count can cover all actors
    if actors.len() > virtual_nodes.len() {
        return Err(anyhow!(
            "actor count ({}) exceeds vnode count ({})",
            actors.len(),
            virtual_nodes.len()
        ));
    }

    // Distribute actors across workers based on their weight
    let actor_to_worker: HashMap<W, Vec<A>> = assign_items_weighted(workers, actors, salt);

    // Build unit-weight map for active workers (those with assigned actors)
    let mut worker_weights: BTreeMap<W, NonZeroUsize> = BTreeMap::new();

    match balanced_by {
        BalancedBy::RawWorkerWeights => {
            // Worker oriented: balanced by raw worker weights
            for (&worker, actor_list) in &actor_to_worker {
                if !actor_list.is_empty() {
                    let worker_weight = workers.get(&worker).expect("Worker should exist");
                    worker_weights.insert(worker, *worker_weight);
                }
            }
        }
        BalancedBy::ActorCounts => {
            // Actor oriented: balanced by actor counts
            for (&worker, actor_list) in &actor_to_worker {
                if let Some(worker_weight) = NonZeroUsize::new(actor_list.len()) {
                    worker_weights.insert(worker, worker_weight);
                }
            }
        }
    }

    // Distribute vnodes evenly among the active workers
    let vnode_to_worker: HashMap<W, Vec<V>> =
        assign_items_weighted(&worker_weights, virtual_nodes, salt);

    // Assign each worker's vnodes to its actors in a round-robin fashion
    let mut result: HashMap<W, HashMap<A, Vec<V>>> = HashMap::new();
    for (worker, actor_list) in actor_to_worker {
        let assigned_vnodes = vnode_to_worker.get(&worker).cloned().unwrap_or_default();
        let mut actor_map: HashMap<A, Vec<V>> = HashMap::with_capacity(actor_list.len());
        for (index, vnode) in assigned_vnodes.into_iter().enumerate() {
            let actor = actor_list[index % actor_list.len()];
            actor_map.entry(actor).or_default().push(vnode);
        }
        result.insert(worker, actor_map);
    }

    Ok(result)
}

#[cfg(test)]
mod test_worker_oriented {
    use std::collections::{BTreeMap, HashSet};
    use std::num::NonZeroUsize;

    use super::*;

    fn build_hierarchy(
        worker_count: usize,
        actor_count: usize,
        vnode_count: usize,
    ) -> HashMap<i32, HashMap<i32, Vec<i32>>> {
        let workers: BTreeMap<i32, NonZeroUsize> = (0..worker_count)
            .map(|i| (i as i32, NonZeroUsize::new(1).unwrap()))
            .collect();
        let actors: Vec<i32> = (0..actor_count).map(|i| i as i32).collect();
        let vnodes: Vec<i32> = (0..vnode_count).map(|i| i as i32).collect();
        assign_hierarchical_worker_oriented(&workers, &actors, &vnodes, 0_i32)
            .unwrap_or_else(|e| panic!("build_hierarchy failed: {}", e))
    }

    #[test]
    fn test_balanced_small() {
        let hierarchy = build_hierarchy(4, 10, 16);

        let vnode_counts: Vec<usize> = hierarchy
            .values()
            .map(|actor_map| actor_map.values().map(Vec::len).sum())
            .collect();
        let min_v = *vnode_counts.iter().min().unwrap();
        let max_v = *vnode_counts.iter().max().unwrap();
        assert!(
            max_v - min_v <= 1,
            "Vnodes per worker not balanced: min={}, max={}",
            min_v,
            max_v
        );

        for actor_map in hierarchy.values() {
            let counts: Vec<usize> = actor_map.values().map(Vec::len).collect();
            let min_a = *counts.iter().min().unwrap();
            let max_a = *counts.iter().max().unwrap();
            assert!(
                max_a - min_a <= 1,
                "Vnodes per actor not balanced: min={}, max={}",
                min_a,
                max_a
            );
        }

        let assigned: HashSet<i32> = hierarchy
            .values()
            .flat_map(|actor_map| actor_map.values().flatten().cloned())
            .collect();
        assert_eq!(assigned.len(), 16, "Some vnodes were not assigned");
    }

    fn check_balanced(worker_count: usize, actor_count: usize, vnode_count: usize) {
        let workers: BTreeMap<i32, NonZeroUsize> = (0..worker_count)
            .map(|i| (i as i32, NonZeroUsize::new(1).unwrap()))
            .collect();
        let actors: Vec<i32> = (0..actor_count).map(|i| i as i32).collect();
        let vnodes: Vec<i32> = (0..vnode_count).map(|i| i as i32).collect();

        check_worker_vnode_balance(&workers, &actors, &vnodes);
    }

    #[test]
    fn test_various_scenarios() {
        // (worker_count, actor_count, vnode_count)
        let scenarios = vec![
            (1, 1, 1),
            (1, 1, 5),
            (5, 1, 5),
            (5, 1, 10),
            (5, 5, 5),
            (5, 5, 10),
            (5, 10, 10),
            (5, 10, 20),
            (10, 1, 10),
            (10, 5, 10),
            (10, 5, 20),
            (10, 10, 20),
            (10, 20, 20),
            (3, 2, 3),
            (3, 3, 5),
            (4, 3, 4),
            (4, 10, 40),
            (20, 10, 20),
        ];

        for &(w, a, v) in &scenarios {
            println!(
                "worker count: {}, actor count: {}, vnode count: {}",
                w, a, v
            );
            check_balanced(w, a, v);
        }

        let bad = (5, 10, 5);
        assert!(
            assign_hierarchical_worker_oriented(
                &BTreeMap::from_iter((0..bad.0).map(|i| (i, NonZeroUsize::new(1).unwrap()))),
                &(0..bad.1).collect::<Vec<_>>(),
                &(0..bad.2).collect::<Vec<_>>(),
                0_i32
            )
            .is_err(),
            "Expected error when actors > vnodes"
        );
    }

    #[test]
    fn test_enum_scenarios() {
        for worker_count in 1..20 {
            let vnode_count = 256;
            for actor_count in 1..=vnode_count {
                check_balanced(worker_count, actor_count, vnode_count);
            }
        }
    }

    #[test]
    fn test_worker_eq_actor() {
        for worker_count in 1..200 {
            let vnode_count = 256;
            let actor_count = worker_count;
            check_balanced(worker_count, actor_count, vnode_count);
        }
    }

    #[test]
    fn test_unbalance() {
        for weight in 1..1024 {
            let actor_count = 2;
            let vnode_count = 50;

            let mut workers: BTreeMap<i32, NonZeroUsize> = BTreeMap::new();
            workers.insert(1, NonZeroUsize::new(weight).unwrap());
            workers.insert(1, NonZeroUsize::new(1).unwrap());

            let actors: Vec<i32> = (0..actor_count).collect();
            let vnodes: Vec<i32> = (0..vnode_count).collect();

            check_worker_vnode_balance(&workers, &actors, &vnodes);
        }
    }

    fn check_worker_vnode_balance(
        workers: &BTreeMap<i32, NonZeroUsize>,
        actors: &[i32],
        vnodes: &[i32],
    ) {
        let worker_count = workers.len();
        let actor_count = actors.len();
        let vnode_count = vnodes.len();

        let hierarchy = assign_hierarchical_worker_oriented(workers, actors, vnodes, 42_i32)
            .expect("should succeed");

        let hierarchy: HashMap<_, _> = hierarchy
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();

        let vnode_count_per_worker: Vec<usize> = hierarchy
            .values()
            .map(|m| m.values().map(Vec::len).sum())
            .collect();

        let min_count = *vnode_count_per_worker.iter().min().unwrap_or(&0);
        let max_count = *vnode_count_per_worker.iter().max().unwrap_or(&0);
        assert!(
            max_count - min_count <= 1,
            "Vnodes per worker not balanced (workers={}, vnodes={}): min={}, max={}",
            worker_count,
            vnode_count,
            min_count,
            max_count
        );

        let assigned_actors: HashSet<i32> =
            hierarchy.values().flat_map(|m| m.keys().cloned()).collect();
        assert_eq!(
            assigned_actors.len(),
            actor_count,
            "Some actors were not assigned (workers={}, actors={})",
            worker_count,
            actor_count
        );

        let assigned_vnodes: HashSet<i32> = hierarchy
            .values()
            .flat_map(|m| m.values().flatten().cloned())
            .collect();
        assert_eq!(
            assigned_vnodes.len(),
            vnode_count,
            "Some vnodes were not assigned (workers={}, vnodes={})",
            worker_count,
            vnode_count
        );
    }
}

#[cfg(test)]
mod test_actor_oriented {
    use std::collections::{BTreeMap, HashSet};
    use std::num::NonZeroUsize;

    use anyhow::Result;

    use super::*;

    /// Helper: collect all actors assigned exactly once
    fn collect_assigned_actors<W, A, V>(map: &HashMap<W, HashMap<A, Vec<V>>>) -> Vec<A>
    where
        W: Eq + std::hash::Hash,
        A: Eq + std::hash::Hash + Copy,
    {
        map.values()
            .flat_map(|actor_map| actor_map.keys().cloned())
            .collect()
    }

    /// Helper: collect all vnodes assigned exactly once
    fn collect_assigned_vnodes<W, A, V>(map: &HashMap<W, HashMap<A, Vec<V>>>) -> Vec<V>
    where
        W: Eq + std::hash::Hash,
        A: Eq + std::hash::Hash,
        V: Eq + std::hash::Hash + Copy,
    {
        map.values()
            .flat_map(|actor_map| actor_map.values().flat_map(|vns| vns.iter().cloned()))
            .collect()
    }

    #[test]
    fn test_balanced_equal_weights() -> Result<()> {
        let workers = BTreeMap::from([
            ("w1", NonZeroUsize::new(1).unwrap()),
            ("w2", NonZeroUsize::new(1).unwrap()),
        ]);
        let actors = vec!["a1", "a2", "a3", "a4"];
        let vnodes: Vec<_> = (1..=8).collect();

        let result = assign_hierarchical_actor_oriented(&workers, &actors, &vnodes, 42_u64)?;

        // 1. The difference in the number of actors assigned to each worker should be ≤ 1
        let actor_counts: Vec<_> = result.values().map(|actor_map| actor_map.len()).collect();
        let max_a = *actor_counts.iter().max().unwrap();
        let min_a = *actor_counts.iter().min().unwrap();
        assert!(max_a - min_a <= 1, "actors per worker not balanced");

        // 2. All actors are assigned without any duplicates.
        let assigned_actors = collect_assigned_actors(&result);
        let unique_actors: HashSet<_> = assigned_actors.iter().collect();
        assert_eq!(
            unique_actors.len(),
            actors.len(),
            "actor missing or duplicated"
        );

        // 3. The difference in the number of vnodes on each worker with vnodes should be ≤ 1.
        let vnode_counts_per_worker: Vec<_> = result
            .values()
            .map(|actor_map| actor_map.values().map(Vec::len).sum::<usize>())
            .collect();
        let max_vw = *vnode_counts_per_worker.iter().max().unwrap();
        let min_vw = *vnode_counts_per_worker.iter().min().unwrap();
        assert!(max_vw - min_vw <= 1, "vnodes per worker not balanced");

        // 4. All vnodes are assigned without any duplicates.
        let assigned_vnodes = collect_assigned_vnodes(&result);
        assert_eq!(
            assigned_vnodes.len(),
            vnodes.len(),
            "some vnodes missing or extra"
        );
        let unique_vnodes: HashSet<_> = assigned_vnodes.iter().collect();
        assert_eq!(unique_vnodes.len(), vnodes.len(), "vnodes duplicated");

        // 5. The difference in vnode allocation between actors should be ≤ 1, and each actor must have at least 1 vnode.
        let vnode_counts_per_actor: Vec<_> = result
            .values()
            .flat_map(|actor_map| actor_map.values().map(Vec::len))
            .collect();
        let max_va = *vnode_counts_per_actor.iter().max().unwrap();
        let min_va = *vnode_counts_per_actor.iter().min().unwrap();
        assert!(max_va - min_va <= 1, "vnodes per actor not balanced");
        assert!(min_va >= 1, "some actor got zero vnodes");

        Ok(())
    }

    #[test]
    fn test_odd_numbers_distribution() -> Result<()> {
        let workers = BTreeMap::from([
            ("w1", NonZeroUsize::new(1).unwrap()),
            ("w2", NonZeroUsize::new(1).unwrap()),
        ]);
        let actors = vec!["x", "y", "z"];
        let vnodes: Vec<_> = (1..=5).collect();

        let result = assign_hierarchical_actor_oriented(&workers, &actors, &vnodes, 99_u64)?;

        // actor per worker: 3 actors, 2 workers → {2,1} 差 1
        let actor_counts: Vec<_> = result.values().map(|m| m.len()).collect();
        let max_a = *actor_counts.iter().max().unwrap();
        let min_a = *actor_counts.iter().min().unwrap();
        assert_eq!(max_a - min_a, 1, "unexpected actor balance");

        // vnodes per worker: 5 vnodes, 2 workers → 差 1
        let vnode_counts: Vec<_> = result
            .values()
            .map(|m| m.values().map(Vec::len).sum::<usize>())
            .collect();
        let max_vw = *vnode_counts.iter().max().unwrap();
        let min_vw = *vnode_counts.iter().min().unwrap();
        assert_eq!(max_vw - min_vw, 1, "unexpected vnode-per-worker balance");

        // Each actor must have at least 1 vnode, and the difference in vnodes among actors should be ≤ 1.
        let vnode_per_actor: Vec<_> = result
            .values()
            .flat_map(|m| m.values().map(Vec::len))
            .collect();
        let max_va = *vnode_per_actor.iter().max().unwrap();
        let min_va = *vnode_per_actor.iter().min().unwrap();
        assert!(min_va >= 1, "some actor got zero vnodes");
        assert!(max_va - min_va <= 1, "unexpected vnode-per-actor balance");

        // All vnodes are assigned without any duplicates.
        let assigned: Vec<_> = collect_assigned_vnodes(&result);
        let unique: HashSet<_> = assigned.iter().collect();
        assert_eq!(assigned.len(), vnodes.len());
        assert_eq!(unique.len(), vnodes.len());

        Ok(())
    }

    /// Helper to unwrap the hierarchy or panic in tests
    fn build_hierarchy(
        workers: &BTreeMap<i32, NonZeroUsize>,
        actors: &[i32],
        vnodes: &[i32],
        salt: i32,
    ) -> HashMap<i32, HashMap<i32, Vec<i32>>> {
        assign_hierarchical_actor_oriented(workers, actors, vnodes, salt)
            .unwrap_or_else(|e| panic!("Failed to build hierarchy: {}", e))
    }

    #[test]
    fn actor_distribution_balanced() {
        let workers = BTreeMap::from([
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(1).unwrap()),
            (3, NonZeroUsize::new(1).unwrap()),
        ]);
        let actors: Vec<i32> = (0..10).collect();
        let vnodes: Vec<i32> = (0..20).collect();

        let hierarchy = build_hierarchy(&workers, &actors, &vnodes, 0);

        // Check the number of actors assigned to each worker, max - min <= 1.
        let counts: Vec<usize> = hierarchy
            .values()
            .map(|actor_map| actor_map.len())
            .collect();
        let min = *counts.iter().min().unwrap();
        let max = *counts.iter().max().unwrap();
        assert!(
            max - min <= 1,
            "Actor distribution across workers is not balanced: min={}, max={}",
            min,
            max
        );
    }

    #[test]
    fn vnode_assignment_and_balance() {
        let workers = BTreeMap::from([
            (1, NonZeroUsize::new(1).unwrap()),
            (2, NonZeroUsize::new(1).unwrap()),
        ]);
        let actors = vec![0, 1, 2, 3];
        let vnodes: Vec<i32> = (0..8).collect();

        let hierarchy = build_hierarchy(&workers, &actors, &vnodes, 42);

        // 1) All actors have been assigned.
        let mut assigned_actors: Vec<i32> = hierarchy
            .iter()
            .flat_map(|(_, actor_map)| actor_map.keys().cloned())
            .collect();
        assigned_actors.sort_unstable();
        assigned_actors.dedup();
        assert_eq!(assigned_actors, actors, "Not all actors were assigned");

        // 2) Each actor must have at least one vnode, and the number of vnodes among actors should be balanced (max - min <= 1).
        let mut vnode_counts: Vec<usize> = Vec::new();
        for actor_map in hierarchy.values() {
            for vns in actor_map.values() {
                assert!(!vns.is_empty(), "Actor has no vnodes assigned");
                vnode_counts.push(vns.len());
            }
        }
        let min_v = *vnode_counts.iter().min().unwrap();
        let max_v = *vnode_counts.iter().max().unwrap();
        assert!(
            max_v - min_v <= 1,
            "VNode distribution among actors not balanced: min={}, max={}",
            min_v,
            max_v
        );

        // 3) The number of vnodes on each worker with vnodes should also be balanced.
        let worker_vnode_counts: Vec<usize> = hierarchy
            .values()
            .map(|actor_map| actor_map.values().map(Vec::len).sum())
            .collect();
        let min_wv = *worker_vnode_counts.iter().min().unwrap();
        let max_wv = *worker_vnode_counts.iter().max().unwrap();
        assert!(
            max_wv - min_wv <= 1,
            "VNode distribution among workers not balanced: min={}, max={}",
            min_wv,
            max_wv
        );

        // 4) No vnodes are missing.
        let mut all_vnodes: Vec<i32> = hierarchy
            .values()
            .flat_map(|actor_map| actor_map.values().cloned())
            .flatten()
            .collect();
        all_vnodes.sort_unstable();
        all_vnodes.dedup();
        let mut original = vnodes.clone();
        original.sort_unstable();
        assert_eq!(all_vnodes, original, "Some vnodes were not assigned");
    }

    fn check_hierarchy_balanced(worker_count: usize, actor_count: usize, vnode_count: usize) {
        // 构造输入
        let workers: BTreeMap<i32, NonZeroUsize> = (0..worker_count)
            .map(|i| (i as i32, NonZeroUsize::new(1).unwrap()))
            .collect();
        let actors: Vec<i32> = (0..actor_count).map(|i| i as i32).collect();
        let vnodes: Vec<i32> = (0..vnode_count).map(|i| i as i32).collect();
        let salt = 0_i32;

        let hierarchy = assign_hierarchical_worker_oriented(&workers, &actors, &vnodes, salt)
            .expect("assign_hierarchical should succeed");

        println!("h {:#?}", hierarchy);

        // 1. Verify that actors are distributed evenly among workers.
        let actor_counts: Vec<usize> = hierarchy.values().map(|m| m.len()).collect();
        let min_actors = *actor_counts.iter().min().unwrap_or(&0);
        let max_actors = *actor_counts.iter().max().unwrap_or(&0);
        assert!(
            max_actors - min_actors <= 1,
            "Actors per worker not balanced: min={}, max={}",
            min_actors,
            max_actors
        );

        // 2. Verify that vnodes are distributed evenly among workers.
        let vnode_counts_per_worker: Vec<usize> = hierarchy
            .values()
            .map(|m| m.values().map(Vec::len).sum())
            .collect();
        let min_vpw = *vnode_counts_per_worker.iter().min().unwrap_or(&0);
        let max_vpw = *vnode_counts_per_worker.iter().max().unwrap_or(&0);
        assert!(
            max_vpw - min_vpw <= 1,
            "Vnodes per worker not balanced: min={}, max={}",
            min_vpw,
            max_vpw
        );

        // 3. Verify that all vnodes are assigned.
        let assigned: HashSet<i32> = hierarchy
            .values()
            .flat_map(|m| m.values().flatten().cloned())
            .collect();
        assert_eq!(
            assigned.len(),
            vnode_count,
            "Some vnodes were not assigned: assigned={}, expected={}",
            assigned.len(),
            vnode_count
        );
    }

    #[test]
    fn test_small_balanced() {
        check_hierarchy_balanced(3, 5, 8); // 3 workers, 5 actors, 8 vnodes
    }

    #[test]
    fn test_medium_balanced() {
        check_hierarchy_balanced(4, 10, 16); // 4 workers, 10 actors, 16 vnodes
    }

    #[test]
    fn test_equal_counts() {
        check_hierarchy_balanced(5, 5, 5); // 5 workers, 5 actors, 5 vnodes
    }

    fn check_balanced(worker_count: usize, actor_count: usize, vnode_count: usize) {
        let workers: BTreeMap<i32, NonZeroUsize> = (0..worker_count)
            .map(|i| (i as i32, NonZeroUsize::new(1).unwrap()))
            .collect();
        let actors: Vec<i32> = (0..actor_count).map(|i| i as i32).collect();
        let vnodes: Vec<i32> = (0..vnode_count).map(|i| i as i32).collect();

        let hierarchy = assign_hierarchical_actor_oriented(&workers, &actors, &vnodes, 42_i32)
            .expect("should succeed");

        let hierarchy: HashMap<_, _> = hierarchy
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();

        println!("result {:?}", hierarchy);

        // 1. Actors are distributed evenly among workers.
        let actor_per_worker: Vec<usize> = hierarchy.values().map(|m| m.len()).collect();
        let min_awo = *actor_per_worker.iter().min().unwrap_or(&0);
        let max_awo = *actor_per_worker.iter().max().unwrap_or(&0);
        assert!(
            max_awo - min_awo <= 1,
            "Actors per worker not balanced (workers={}, actors={}): min={}, max={}",
            worker_count,
            actor_count,
            min_awo,
            max_awo
        );

        // 2. Vnodes are distributed evenly among workers.
        // let vnode_per_worker: Vec<usize> = hierarchy
        //     .values()
        //     .map(|m| m.values().map(Vec::len).sum())
        //     .collect();
        // let min_vwo = *vnode_per_worker.iter().min().unwrap_or(&0);
        // let max_vwo = *vnode_per_worker.iter().max().unwrap_or(&0);
        // assert!(
        //     max_vwo - min_vwo <= 1,
        //     "Vnodes per worker not balanced (workers={}, vnodes={}): min={}, max={}",
        //     worker_count,
        //     vnode_count,
        //     min_vwo,
        //     max_vwo
        // );

        // 3. Vnodes are distributed evenly among actors.
        let mut vnode_per_actor = Vec::new();
        for actor_map in hierarchy.values() {
            for vns in actor_map.values() {
                vnode_per_actor.push(vns.len());
            }
        }
        let min_vao = *vnode_per_actor.iter().min().unwrap_or(&0);
        let max_vao = *vnode_per_actor.iter().max().unwrap_or(&0);
        assert!(
            max_vao - min_vao <= 1,
            "Vnodes per actor not balanced (actors={}, vnodes={}): min={}, max={}",
            actor_count,
            vnode_count,
            min_vao,
            max_vao
        );

        // 4. All actors are assigned.
        let assigned_actors: HashSet<i32> =
            hierarchy.values().flat_map(|m| m.keys().cloned()).collect();
        assert_eq!(
            assigned_actors.len(),
            actor_count,
            "Some actors were not assigned (workers={}, actors={})",
            worker_count,
            actor_count
        );

        // 5. All vnodes are assigned.
        let assigned_vnodes: HashSet<i32> = hierarchy
            .values()
            .flat_map(|m| m.values().flatten().cloned())
            .collect();
        assert_eq!(
            assigned_vnodes.len(),
            vnode_count,
            "Some vnodes were not assigned (workers={}, vnodes={})",
            worker_count,
            vnode_count
        );
    }

    #[test]
    fn test_various_scenarios() {
        // (worker_count, actor_count, vnode_count)
        let scenarios = vec![
            (1, 1, 1),
            (1, 1, 5),
            (5, 1, 5),
            (5, 1, 10),
            (5, 5, 5),
            (5, 5, 10),
            (5, 10, 10),
            (5, 10, 20),
            (10, 1, 10),
            (10, 5, 10),
            (10, 5, 20),
            (10, 10, 20),
            (10, 20, 20),
            (3, 2, 3),
            (3, 3, 5),
            (4, 3, 4),
            (4, 10, 40),
            (20, 10, 20),
        ];

        for &(w, a, v) in &scenarios {
            println!(
                "worker count: {}, actor count: {}, vnode count: {}",
                w, a, v
            );
            check_balanced(w, a, v);
        }

        // Test invalid scenario: actor_count > vnode_count.
        let bad = (5, 10, 5);
        assert!(
            assign_hierarchical_worker_oriented(
                &BTreeMap::from_iter((0..bad.0).map(|i| (i, NonZeroUsize::new(1).unwrap()))),
                &(0..bad.1).collect::<Vec<_>>(),
                &(0..bad.2).collect::<Vec<_>>(),
                0_i32
            )
            .is_err(),
            "Expected error when actors > vnodes"
        );
    }
}
