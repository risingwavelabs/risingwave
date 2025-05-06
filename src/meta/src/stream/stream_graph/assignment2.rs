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

use anyhow::{Context, Result};

use crate::stream::assign_hierarchical;
use crate::stream::stream_graph::assignment;
use crate::stream::stream_graph::assignment::{BalancedBy, CapacityMode};

pub struct Assigner<S> {
    salt: S,
    actor_capacity_mode: CapacityMode,
    vnode_capacity_mode: CapacityMode,
    balanced_by: BalancedBy,
}

/// Builder for `Assigner`
pub struct AssignerBuilder<S> {
    salt: S,
    actor_capacity_mode: CapacityMode,
    vnode_capacity_mode: CapacityMode,
    balanced_by: BalancedBy,
}

impl<S: Hash + Copy> AssignerBuilder<S> {
    pub fn new(salt: S) -> Self {
        Self {
            salt,
            actor_capacity_mode: CapacityMode::Weighted,
            vnode_capacity_mode: CapacityMode::Weighted,
            balanced_by: BalancedBy::RawWorkerWeights,
        }
    }

    pub fn actor_capacity_weighted(mut self) -> Self {
        self.actor_capacity_mode = CapacityMode::Weighted;
        self
    }

    pub fn actor_capacity_unbounded(mut self) -> Self {
        self.actor_capacity_mode = CapacityMode::Unbounded;
        self
    }

    pub fn vnode_capacity_weighted(mut self) -> Self {
        self.vnode_capacity_mode = CapacityMode::Weighted;
        self
    }

    pub fn vnode_capacity_unbounded(mut self) -> Self {
        self.actor_capacity_mode = CapacityMode::Unbounded;
        self
    }

    pub fn actor_oriented_balancing(mut self) -> Self {
        self.balanced_by = BalancedBy::ActorCounts;
        self
    }

    pub fn worker_oriented_balancing(mut self) -> Self {
        self.balanced_by = BalancedBy::RawWorkerWeights;
        self
    }

    pub fn build(self) -> Assigner<S> {
        Assigner {
            salt: self.salt,
            actor_capacity_mode: self.actor_capacity_mode,
            vnode_capacity_mode: self.vnode_capacity_mode,
            balanced_by: self.balanced_by,
        }
    }
}

impl<S: Hash + Copy> Assigner<S> {
    /// Core: assign items → containers according to capacity mode.
    pub fn assign_actors<C, I>(
        &self,
        workers: &BTreeMap<C, NonZeroUsize>,
        actors: &[I],
    ) -> BTreeMap<C, Vec<I>>
    where
        C: Ord + Hash + Eq + Copy + Clone + Debug,
        I: Hash + Eq + Copy + Clone + Debug,
    {
        // let scale_fn = match self.actor_capacity_mode {
        //     CapacityMode::Weighted => assignment::weighted_scale,
        //     CapacityMode::Unbounded => assignment::unbounded_scale,
        // };
        //
        // assignment::assign_items_weighted_with_scale_fn(workers, actors, self.salt, scale_fn)
        todo!()
    }

    /// Assign a sequence of `actor_count` placeholders and return
    /// how many each container received.
    pub fn assign_actors_counts<C>(
        &self,
        workers: &BTreeMap<C, NonZeroUsize>,
        actor_count: usize,
    ) -> BTreeMap<C, usize>
    where
        C: Ord + Hash + Eq + Copy + Clone + Debug,
    {
        // let synthetic_items: Vec<usize> = (0..actor_count).collect();
        // Self::map_values_to_len(self.assign_actors(workers, &synthetic_items))
        todo!()
    }

    /// Hierarchical assign: workers → actors → vnodes
    pub fn assign_hierarchical<W, A, V>(
        &self,
        workers: &BTreeMap<W, NonZeroUsize>,
        actors: &[A],
        vnodes: &[V],
    ) -> Result<BTreeMap<W, BTreeMap<A, Vec<V>>>>
    where
        W: Ord + Hash + Eq + Copy + Clone + Debug,
        A: Ord + Hash + Eq + Copy + Clone + Debug,
        V: Hash + Eq + Copy + Clone + Debug,
    {
        // assign_hierarchical(
        //     workers,
        //     actors,
        //     vnodes,
        //     self.salt,
        //     self.actor_capacity_mode, // assignment2::BalancedBy
        // )
        // .with_context(|| "hierarchical assignment failed")
        todo!()
    }

    /// Hierarchical assign of `actor_count` & `vnode_count`, returning
    /// how many vnodes each actor gets under each worker.
    pub fn assign_hierarchical_counts<W, A>(
        &self,
        workers: &BTreeMap<W, NonZeroUsize>,
        actor_count: usize,
        vnode_count: usize,
    ) -> anyhow::Result<BTreeMap<W, BTreeMap<A, usize>>>
    where
        W: Ord + Hash + Eq + Copy + Clone + Debug,
        A: Ord + Hash + Eq + Copy + Clone + Debug + From<usize>,
    {
        let actors: Vec<A> = (0..actor_count).map(A::from).collect();
        let vnodes: Vec<usize> = (0..vnode_count).collect();
        let full = self.assign_hierarchical(workers, &actors, &vnodes)?;
        Ok(full
            .into_iter()
            .map(|(w, actor_map)| (w, Self::map_values_to_len(actor_map)))
            .collect())
    }

    /// Helper: turn `HashMap<K, Vec<V>>` into `HashMap<K, usize>` by taking each vec’s length.
    fn map_values_to_len<K, V>(map: BTreeMap<K, Vec<V>>) -> BTreeMap<K, usize>
    where
        K: Eq + Hash + Ord,
    {
        map.into_iter().map(|(k, v)| (k, v.len())).collect()
    }
}
// /// Assign items to containers without any capacity limit.
// ///
// /// # Parameters
// /// - `containers`: Map of container identifiers to their non-zero weights.
// /// - `items`: Slice of items to distribute.
// /// - `salt`: Hashing salt to vary the distribution of remainder buckets and weighted rendezvous.
// ///
// /// # Returns
// /// A map from each container to its assigned items.
// pub fn assign_items_unbounded<C, I, S>(
//     containers: &BTreeMap<C, NonZeroUsize>,
//     items: &[I],
//     salt: S,
// ) -> BTreeMap<C, Vec<I>>
// where
//     C: Ord + Hash + Eq + Copy + Clone + Debug,
//     I: Hash + Eq + Copy + Clone + Debug,
//     S: Hash + Copy,
// {
//     assignment::assign_items_weighted_with_scale_fn(
//         containers,
//         items,
//         salt,
//         assignment::unbounded_scale,
//     )
// }
//
// /// Assign items to containers proportionally by weight with a default scale factor of 1.0.
// ///
// /// # Parameters
// /// - `containers`: Map of container identifiers to their non-zero weights.
// /// - `items`: Slice of items to distribute.
// /// - `salt`: Hashing salt to vary the distribution of remainder buckets and weighted rendezvous.
// ///
// /// # Returns
// /// A map from each container to its assigned items.
// pub fn assign_items_weighted<C, I, S>(
//     containers: &BTreeMap<C, NonZeroUsize>,
//     items: &[I],
//     salt: S,
// ) -> BTreeMap<C, Vec<I>>
// where
//     C: Ord + Hash + Eq + Copy + Clone + Debug,
//     I: Hash + Eq + Copy + Clone + Debug,
//     S: Hash + Copy,
// {
//     assignment::assign_items_weighted_with_scale_fn(
//         containers,
//         items,
//         salt,
//         assignment::weighted_scale,
//     )
// }
//
// /// Distributes virtual nodes to actors via a two-layered weighted assignment:
// ///
// /// 1. **Actors → Workers (Weighted)**
// ///    - Distribute actors across workers according to their provided weights.
// /// 2. **`VNodes` → Active Workers (Equal Weight)**
// ///    - Consider only workers assigned at least one actor; treat each with weight = 1.
// ///    - Distribute vnodes uniformly across these active workers.
// /// 3. **`VNodes` → Actors within Workers (Round-Robin)**
// ///    - For each worker, assign its vnodes evenly to its actors in a round-robin fashion.
// ///
// /// # Type Parameters
// /// - `W`: Worker identifier type. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
// /// - `A`: Actor identifier type. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
// /// - `V`: Virtual node type. Must implement `Hash + Eq + Copy + Clone + Debug`.
// /// - `S`: Salt type for hashing tie-breakers. Must implement `Hash + Copy`.
// ///
// /// # Parameters
// /// - `workers`: A `BTreeMap<W, NonZeroUsize>` mapping each worker to its weight.
// /// - `actors`: A slice of actors to distribute among workers.
// /// - `vnodes`: A slice of virtual nodes to assign to actors.
// /// - `salt`: A hashing salt to ensure deterministic yet varied tie-breaking.
// ///
// /// # Returns
// /// A nested `BTreeMap<W, BTreeMap<A, Vec<V>>>` mapping each worker to a map of actors and their assigned vnodes.
// ///
// /// # Errors
// /// Returns an error if the number of actors exceeds the number of vnodes.
// ///
// /// # Example
// /// ```rust
// /// # use std::collections::BTreeMap;
// /// # use std::num::NonZeroUsize;
// ///
// /// # use crate::risingwave_meta::stream::assign_hierarchical_worker_oriented;
// ///
// /// // Define worker weights
// /// let mut workers = BTreeMap::new();
// /// workers.insert("worker1", NonZeroUsize::new(3).unwrap());
// /// workers.insert("worker2", NonZeroUsize::new(2).unwrap());
// ///
// /// // Actors and virtual nodes
// /// let actors = vec!["actorA", "actorB", "actorC"];
// /// let vnodes = (0..6).collect::<Vec<_>>();
// ///
// /// // Perform hierarchical assignment with salt = 42
// /// let assignment =
// ///     assign_hierarchical_worker_oriented(&workers, &actors, &vnodes, 42u64).unwrap();
// ///
// /// // Inspect results
// /// for (worker, map) in assignment {
// ///     println!("{}:", worker);
// ///     for (actor, vlist) in map {
// ///         println!("  {} -> {:?}", actor, vlist);
// ///     }
// /// }
// /// ```
// pub fn assign_hierarchical_worker_oriented<W, A, V, S>(
//     workers: &BTreeMap<W, NonZeroUsize>,
//     actors: &[A],
//     vnodes: &[V],
//     salt: S,
// ) -> Result<BTreeMap<W, BTreeMap<A, Vec<V>>>>
// where
//     W: Ord + Hash + Eq + Copy + Clone + Debug,
//     A: Ord + Hash + Eq + Copy + Clone + Debug,
//     V: Hash + Eq + Copy + Clone + Debug,
//     S: Hash + Copy,
// {
//     assign_hierarchical(workers, actors, vnodes, salt, BalancedBy::RawWorkerWeights)
// }
//
// /// Assigns virtual nodes to actors with actor‑oriented balancing.
// ///
// /// This convenience wrapper calls `assign_hierarchical` with `BalancedBy::ActorCounts`,
// /// resulting in:
// ///
// /// 1. **Actors → Workers (Weighted by worker weight)**
// /// 2. **`VNodes` → Workers (Weighted by number of actors per worker)**
// /// 3. **`VNodes` → Actors (Round‑robin within each worker)**
// ///
// /// # Type Parameters
// /// - `W`: Worker identifier. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
// /// - `A`: Actor identifier. Must implement `Ord + Hash + Eq + Copy + Clone + Debug`.
// /// - `V`: Virtual node type. Must implement `Hash + Eq + Copy + Clone + Debug`.
// /// - `S`: Salt type for hashing tie‑breakers. Must implement `Hash + Copy`.
// ///
// /// # Parameters
// /// - `workers`: A `BTreeMap<W, NonZeroUsize>` mapping each worker to its weight.
// /// - `actors`: A slice of actors to assign.
// /// - `vnodes`: A slice of virtual nodes to distribute.
// /// - `salt`: A hashing salt to ensure deterministic yet varied tie‑breaking.
// ///
// /// # Returns
// /// A `BTreeMap<W, BTreeMap<A, Vec<V>>>` mapping each worker to its actors and assigned vnodes,
// /// with balancing based on actor counts.
// ///
// /// # Errors
// /// Returns an error if `actors.len() > vnodes.len()`.
// ///
// /// # Example
// /// ```rust
// /// # use std::collections::BTreeMap;
// /// # use std::num::NonZeroUsize;
// ///
// /// # use crate::risingwave_meta::stream::assign_hierarchical_actor_oriented;
// ///
// /// // Define worker weights
// /// let mut workers = BTreeMap::new();
// /// workers.insert("w1", NonZeroUsize::new(4).unwrap());
// /// workers.insert("w2", NonZeroUsize::new(2).unwrap());
// ///
// /// // Actors and virtual nodes
// /// let actors = vec!["a1", "a2", "a3"];
// /// let vnodes = (0..6).collect::<Vec<_>>();
// ///
// /// // Distribute with actor‑oriented balancing, salt = 99
// /// let assignment = assign_hierarchical_actor_oriented(&workers, &actors, &vnodes, 99u64).unwrap();
// ///
// /// // Validate structure
// /// assert_eq!(assignment.len(), 2);
// /// for (worker, actor_map) in &assignment {
// ///     // Each actor under this worker has at most one more vnode than any other
// ///     let counts: Vec<usize> = actor_map.values().map(Vec::len).collect();
// ///     let min = *counts.iter().min().unwrap();
// ///     let max = *counts.iter().max().unwrap();
// ///     assert!(max - min <= 1);
// /// }
// /// ```
// pub fn assign_hierarchical_actor_oriented<W, A, V, S>(
//     workers: &BTreeMap<W, NonZeroUsize>,
//     actors: &[A],
//     vnodes: &[V],
//     salt: S,
// ) -> Result<BTreeMap<W, BTreeMap<A, Vec<V>>>>
// where
//     W: Ord + Hash + Eq + Copy + Clone + Debug,
//     A: Ord + Hash + Eq + Copy + Clone + Debug,
//     V: Hash + Eq + Copy + Clone + Debug,
//     S: Hash + Copy,
// {
//     assign_hierarchical(workers, actors, vnodes, salt, BalancedBy::ActorCounts)
// }
//
// #[cfg(test)]
// mod tests {
//     use std::collections::BTreeMap;
//     use std::num::NonZeroUsize;
//
//     use super::*;
//     use crate::stream::stream_graph::assignment::assign_items_weighted_with_scale_fn;
//
//     const SALT_FOR_TEST: u64 = 42;
//
//     macro_rules! nz {
//         ($e:expr) => {
//             NonZeroUsize::new($e).unwrap()
//         };
//     }
//
//     #[test]
//     fn evenly_distributes_equal_weights() {
//         let mut containers: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
//         containers.insert(1, nz!(1));
//         containers.insert(2, nz!(1));
//         containers.insert(3, nz!(1));
//         let items: Vec<u32> = (0..9).collect();
//         let result = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
//         for (&c, v) in &result {
//             assert_eq!(v.len(), 3, "Container {:?} expected 3 items", c);
//         }
//     }
//
//     #[test]
//     fn respects_weight_ratios() {
//         let mut containers: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
//         containers.insert(10, nz!(1));
//         containers.insert(20, nz!(3));
//         let items: Vec<u32> = (0..40).collect();
//         let result = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
//         let c1 = result.get(&10).unwrap().len();
//         let c2 = result.get(&20).unwrap().len();
//         assert!(
//             c2 > c1 * 2 && c2 < c1 * 4,
//             "Weight ratio violated: c1={} c2={}",
//             c1,
//             c2
//         );
//     }
//
//     #[test]
//     fn handles_empty_inputs() {
//         let empty: BTreeMap<u32, NonZeroUsize> = BTreeMap::new();
//         let items = vec![1, 2, 3];
//         assert!(assign_items_weighted(&empty, &items, SALT_FOR_TEST).is_empty());
//
//         let mut one: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
//         one.insert(42, nz!(10));
//         let none: Vec<u32> = Vec::new();
//         let result = assign_items_weighted(&one, &none, SALT_FOR_TEST);
//         assert_eq!(result.get(&42).unwrap().len(), 0);
//     }
//
//     #[test]
//     fn scale_factor_increases_capacity() {
//         let mut containers: BTreeMap<char, NonZeroUsize> = BTreeMap::new();
//         containers.insert('A', nz!(1));
//         containers.insert('B', nz!(1));
//         let items: Vec<u32> = (0..4).collect();
//         let result =
//             assign_items_weighted_with_scale_fn(&containers, &items, SALT_FOR_TEST, |_, _| {
//                 Some(2.0)
//             });
//         assert_eq!(
//             result.get(&'A').unwrap().len() + result.get(&'B').unwrap().len(),
//             4
//         );
//     }
//
//     #[test]
//     fn exact_quota_match_for_small_weights() {
//         let mut containers: BTreeMap<char, NonZeroUsize> = BTreeMap::new();
//         containers.insert('A', nz!(1));
//         containers.insert('B', nz!(2));
//         let items: Vec<u8> = (0..3).collect();
//         let result = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
//         let a_count = result.get(&'A').unwrap().len();
//         let b_count = result.get(&'B').unwrap().len();
//         assert_eq!(a_count, 1, "Expected A to get exactly 1 item");
//         assert_eq!(b_count, 2, "Expected B to get exactly 2 items");
//     }
//
//     #[test]
//     fn handles_more_containers_than_items() {
//         let mut containers: BTreeMap<&str, NonZeroUsize> = BTreeMap::new();
//         containers.insert("X", nz!(1));
//         containers.insert("Y", nz!(1));
//         containers.insert("Z", nz!(1));
//         let items = vec![100, 200];
//         let result = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
//         let non_empty = result.values().filter(|v| !v.is_empty()).count();
//         assert_eq!(
//             non_empty, 2,
//             "Exactly two containers should receive an item"
//         );
//         let zero = result.values().filter(|v| v.is_empty()).count();
//         assert_eq!(zero, 1, "Exactly one container should receive zero items");
//     }
//
//     #[test]
//     fn deterministic_across_invocations() {
//         let mut containers: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
//         containers.insert(10, nz!(3));
//         containers.insert(20, nz!(5));
//         let items: Vec<i32> = (0..20).collect();
//         let first = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
//         let second = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
//         assert_eq!(first, second, "Assignment must be deterministic");
//     }
//
//     #[test]
//     fn scale_less_than_one_behaves_as_one() {
//         let mut containers: BTreeMap<char, NonZeroUsize> = BTreeMap::new();
//         containers.insert('A', nz!(1));
//         containers.insert('B', nz!(1));
//         let items: Vec<u8> = (0..6).collect();
//         let default = assign_items_weighted(&containers, &items, SALT_FOR_TEST);
//         let scaled_down =
//             assign_items_weighted_with_scale_fn(&containers, &items, SALT_FOR_TEST, |_, _| {
//                 Some(0.5)
//             });
//         assert_eq!(default, scaled_down, "scale < 1 should not alter quotas");
//     }
//
//     #[test]
//     fn scale_factor_limits_are_respected() {
//         let mut containers: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
//         containers.insert(1, nz!(1));
//         containers.insert(2, nz!(1));
//         let items: Vec<u32> = (0..4).collect();
//         let result =
//             assign_items_weighted_with_scale_fn(&containers, &items, SALT_FOR_TEST, |_, _| {
//                 Some(1.5)
//             });
//         for (&c, v) in &result {
//             assert!(
//                 v.len() <= 3,
//                 "Container {:?} exceeded scaled quota of 3, got {}",
//                 c,
//                 v.len()
//             );
//         }
//         let total_assigned: usize = result.values().map(|v| v.len()).sum();
//         assert_eq!(total_assigned, 4, "All items should be assigned");
//     }
//
//     #[test]
//     fn empty_inputs_return_empty_vectors() {
//         let empty: BTreeMap<u32, NonZeroUsize> = BTreeMap::new();
//         let items = vec![1, 2, 3];
//         assert!(assign_items_weighted(&empty, &items, SALT_FOR_TEST).is_empty());
//
//         let mut one: BTreeMap<usize, NonZeroUsize> = BTreeMap::new();
//         one.insert(99, nz!(5));
//         let none: Vec<i32> = Vec::new();
//         let result = assign_items_weighted(&one, &none, SALT_FOR_TEST);
//         assert_eq!(result.get(&99).unwrap().len(), 0);
//     }
// }
//
// #[cfg(test)]
// mod test_worker_oriented {
//     use std::collections::{BTreeMap, HashSet};
//     use std::num::NonZeroUsize;
//
//     use super::*;
//
//     fn build_hierarchy(
//         worker_count: usize,
//         actor_count: usize,
//         vnode_count: usize,
//     ) -> BTreeMap<i32, BTreeMap<i32, Vec<i32>>> {
//         let workers: BTreeMap<i32, NonZeroUsize> = (0..worker_count)
//             .map(|i| (i as i32, NonZeroUsize::new(1).unwrap()))
//             .collect();
//         let actors: Vec<i32> = (0..actor_count).map(|i| i as i32).collect();
//         let vnodes: Vec<i32> = (0..vnode_count).map(|i| i as i32).collect();
//         assign_hierarchical_worker_oriented(&workers, &actors, &vnodes, 0_i32)
//             .unwrap_or_else(|e| panic!("build_hierarchy failed: {}", e))
//     }
//
//     #[test]
//     fn test_balanced_small() {
//         let hierarchy = build_hierarchy(4, 10, 16);
//
//         let vnode_counts: Vec<usize> = hierarchy
//             .values()
//             .map(|actor_map| actor_map.values().map(Vec::len).sum())
//             .collect();
//         let min_v = *vnode_counts.iter().min().unwrap();
//         let max_v = *vnode_counts.iter().max().unwrap();
//         assert!(
//             max_v - min_v <= 1,
//             "Vnodes per worker not balanced: min={}, max={}",
//             min_v,
//             max_v
//         );
//
//         for actor_map in hierarchy.values() {
//             let counts: Vec<usize> = actor_map.values().map(Vec::len).collect();
//             let min_a = *counts.iter().min().unwrap();
//             let max_a = *counts.iter().max().unwrap();
//             assert!(
//                 max_a - min_a <= 1,
//                 "Vnodes per actor not balanced: min={}, max={}",
//                 min_a,
//                 max_a
//             );
//         }
//
//         let assigned: HashSet<i32> = hierarchy
//             .values()
//             .flat_map(|actor_map| actor_map.values().flatten().cloned())
//             .collect();
//         assert_eq!(assigned.len(), 16, "Some vnodes were not assigned");
//     }
//
//     fn check_balanced(worker_count: usize, actor_count: usize, vnode_count: usize) {
//         let workers: BTreeMap<i32, NonZeroUsize> = (0..worker_count)
//             .map(|i| (i as i32, NonZeroUsize::new(1).unwrap()))
//             .collect();
//         let actors: Vec<i32> = (0..actor_count).map(|i| i as i32).collect();
//         let vnodes: Vec<i32> = (0..vnode_count).map(|i| i as i32).collect();
//
//         check_worker_vnode_balance(&workers, &actors, &vnodes);
//     }
//
//     #[test]
//     fn test_various_scenarios() {
//         // (worker_count, actor_count, vnode_count)
//         let scenarios = vec![
//             (1, 1, 1),
//             (1, 1, 5),
//             (5, 1, 5),
//             (5, 1, 10),
//             (5, 5, 5),
//             (5, 5, 10),
//             (5, 10, 10),
//             (5, 10, 20),
//             (10, 1, 10),
//             (10, 5, 10),
//             (10, 5, 20),
//             (10, 10, 20),
//             (10, 20, 20),
//             (3, 2, 3),
//             (3, 3, 5),
//             (4, 3, 4),
//             (4, 10, 40),
//             (20, 10, 20),
//         ];
//
//         for &(w, a, v) in &scenarios {
//             println!(
//                 "worker count: {}, actor count: {}, vnode count: {}",
//                 w, a, v
//             );
//             check_balanced(w, a, v);
//         }
//
//         let bad = (5, 10, 5);
//         assert!(
//             assign_hierarchical_worker_oriented(
//                 &BTreeMap::from_iter((0..bad.0).map(|i| (i, NonZeroUsize::new(1).unwrap()))),
//                 &(0..bad.1).collect::<Vec<_>>(),
//                 &(0..bad.2).collect::<Vec<_>>(),
//                 0_i32
//             )
//             .is_err(),
//             "Expected error when actors > vnodes"
//         );
//     }
//
//     #[test]
//     fn test_enum_scenarios() {
//         for worker_count in 1..20 {
//             let vnode_count = 256;
//             for actor_count in 1..=vnode_count {
//                 check_balanced(worker_count, actor_count, vnode_count);
//             }
//         }
//     }
//
//     #[test]
//     fn test_worker_eq_actor() {
//         for worker_count in 1..200 {
//             let vnode_count = 256;
//             let actor_count = worker_count;
//             check_balanced(worker_count, actor_count, vnode_count);
//         }
//     }
//
//     #[test]
//     fn test_unbalance() {
//         for weight in 1..1024 {
//             let actor_count = 2;
//             let vnode_count = 50;
//
//             let mut workers: BTreeMap<i32, NonZeroUsize> = BTreeMap::new();
//             workers.insert(1, NonZeroUsize::new(weight).unwrap());
//             workers.insert(1, NonZeroUsize::new(1).unwrap());
//
//             let actors: Vec<i32> = (0..actor_count).collect();
//             let vnodes: Vec<i32> = (0..vnode_count).collect();
//
//             check_worker_vnode_balance(&workers, &actors, &vnodes);
//         }
//     }
//
//     fn check_worker_vnode_balance(
//         workers: &BTreeMap<i32, NonZeroUsize>,
//         actors: &[i32],
//         vnodes: &[i32],
//     ) {
//         let worker_count = workers.len();
//         let actor_count = actors.len();
//         let vnode_count = vnodes.len();
//
//         let hierarchy = assign_hierarchical_worker_oriented(workers, actors, vnodes, 42_i32)
//             .expect("should succeed");
//
//         let hierarchy: HashMap<_, _> = hierarchy
//             .into_iter()
//             .filter(|(_, v)| !v.is_empty())
//             .collect();
//
//         let vnode_count_per_worker: Vec<usize> = hierarchy
//             .values()
//             .map(|m| m.values().map(Vec::len).sum())
//             .collect();
//
//         let min_count = *vnode_count_per_worker.iter().min().unwrap_or(&0);
//         let max_count = *vnode_count_per_worker.iter().max().unwrap_or(&0);
//         assert!(
//             max_count - min_count <= 1,
//             "Vnodes per worker not balanced (workers={}, vnodes={}): min={}, max={}",
//             worker_count,
//             vnode_count,
//             min_count,
//             max_count
//         );
//
//         let assigned_actors: HashSet<i32> =
//             hierarchy.values().flat_map(|m| m.keys().cloned()).collect();
//         assert_eq!(
//             assigned_actors.len(),
//             actor_count,
//             "Some actors were not assigned (workers={}, actors={})",
//             worker_count,
//             actor_count
//         );
//
//         let assigned_vnodes: HashSet<i32> = hierarchy
//             .values()
//             .flat_map(|m| m.values().flatten().cloned())
//             .collect();
//         assert_eq!(
//             assigned_vnodes.len(),
//             vnode_count,
//             "Some vnodes were not assigned (workers={}, vnodes={})",
//             worker_count,
//             vnode_count
//         );
//     }
// }
//
// #[cfg(test)]
// mod test_actor_oriented {
//     use std::collections::{BTreeMap, HashSet};
//     use std::num::NonZeroUsize;
//
//     use anyhow::Result;
//
//     use super::*;
//
//     /// Helper: collect all actors assigned exactly once
//     fn collect_assigned_actors<W, A, V>(map: &BTreeMap<W, BTreeMap<A, Vec<V>>>) -> Vec<A>
//     where
//         W: Eq + std::hash::Hash,
//         A: Eq + std::hash::Hash + Copy,
//     {
//         map.values()
//             .flat_map(|actor_map| actor_map.keys().cloned())
//             .collect()
//     }
//
//     /// Helper: collect all vnodes assigned exactly once
//     fn collect_assigned_vnodes<W, A, V>(map: &BTreeMap<W, BTreeMap<A, Vec<V>>>) -> Vec<V>
//     where
//         W: Eq + std::hash::Hash,
//         A: Eq + std::hash::Hash,
//         V: Eq + std::hash::Hash + Copy,
//     {
//         map.values()
//             .flat_map(|actor_map| actor_map.values().flat_map(|vns| vns.iter().cloned()))
//             .collect()
//     }
//
//     #[test]
//     fn test_balanced_equal_weights() -> Result<()> {
//         let workers = BTreeMap::from([
//             ("w1", NonZeroUsize::new(1).unwrap()),
//             ("w2", NonZeroUsize::new(1).unwrap()),
//         ]);
//         let actors = vec!["a1", "a2", "a3", "a4"];
//         let vnodes: Vec<_> = (1..=8).collect();
//
//         let result = assign_hierarchical_actor_oriented(&workers, &actors, &vnodes, 42_u64)?;
//
//         // 1. The difference in the number of actors assigned to each worker should be ≤ 1
//         let actor_counts: Vec<_> = result.values().map(|actor_map| actor_map.len()).collect();
//         let max_a = *actor_counts.iter().max().unwrap();
//         let min_a = *actor_counts.iter().min().unwrap();
//         assert!(max_a - min_a <= 1, "actors per worker not balanced");
//
//         // 2. All actors are assigned without any duplicates.
//         let assigned_actors = collect_assigned_actors(&result);
//         let unique_actors: HashSet<_> = assigned_actors.iter().collect();
//         assert_eq!(
//             unique_actors.len(),
//             actors.len(),
//             "actor missing or duplicated"
//         );
//
//         // 3. The difference in the number of vnodes on each worker with vnodes should be ≤ 1.
//         let vnode_counts_per_worker: Vec<_> = result
//             .values()
//             .map(|actor_map| actor_map.values().map(Vec::len).sum::<usize>())
//             .collect();
//         let max_vw = *vnode_counts_per_worker.iter().max().unwrap();
//         let min_vw = *vnode_counts_per_worker.iter().min().unwrap();
//         assert!(max_vw - min_vw <= 1, "vnodes per worker not balanced");
//
//         // 4. All vnodes are assigned without any duplicates.
//         let assigned_vnodes = collect_assigned_vnodes(&result);
//         assert_eq!(
//             assigned_vnodes.len(),
//             vnodes.len(),
//             "some vnodes missing or extra"
//         );
//         let unique_vnodes: HashSet<_> = assigned_vnodes.iter().collect();
//         assert_eq!(unique_vnodes.len(), vnodes.len(), "vnodes duplicated");
//
//         // 5. The difference in vnode allocation between actors should be ≤ 1, and each actor must have at least 1 vnode.
//         let vnode_counts_per_actor: Vec<_> = result
//             .values()
//             .flat_map(|actor_map| actor_map.values().map(Vec::len))
//             .collect();
//         let max_va = *vnode_counts_per_actor.iter().max().unwrap();
//         let min_va = *vnode_counts_per_actor.iter().min().unwrap();
//         assert!(max_va - min_va <= 1, "vnodes per actor not balanced");
//         assert!(min_va >= 1, "some actor got zero vnodes");
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_odd_numbers_distribution() -> Result<()> {
//         let workers = BTreeMap::from([
//             ("w1", NonZeroUsize::new(1).unwrap()),
//             ("w2", NonZeroUsize::new(1).unwrap()),
//         ]);
//         let actors = vec!["x", "y", "z"];
//         let vnodes: Vec<_> = (1..=5).collect();
//
//         let result = assign_hierarchical_actor_oriented(&workers, &actors, &vnodes, 99_u64)?;
//
//         // actor per worker: 3 actors, 2 workers → {2,1} 差 1
//         let actor_counts: Vec<_> = result.values().map(|m| m.len()).collect();
//         let max_a = *actor_counts.iter().max().unwrap();
//         let min_a = *actor_counts.iter().min().unwrap();
//         assert_eq!(max_a - min_a, 1, "unexpected actor balance");
//
//         // vnodes per worker: 5 vnodes, 2 workers → 差 1
//         let vnode_counts: Vec<_> = result
//             .values()
//             .map(|m| m.values().map(Vec::len).sum::<usize>())
//             .collect();
//         let max_vw = *vnode_counts.iter().max().unwrap();
//         let min_vw = *vnode_counts.iter().min().unwrap();
//         assert_eq!(max_vw - min_vw, 1, "unexpected vnode-per-worker balance");
//
//         // Each actor must have at least 1 vnode, and the difference in vnodes among actors should be ≤ 1.
//         let vnode_per_actor: Vec<_> = result
//             .values()
//             .flat_map(|m| m.values().map(Vec::len))
//             .collect();
//         let max_va = *vnode_per_actor.iter().max().unwrap();
//         let min_va = *vnode_per_actor.iter().min().unwrap();
//         assert!(min_va >= 1, "some actor got zero vnodes");
//         assert!(max_va - min_va <= 1, "unexpected vnode-per-actor balance");
//
//         // All vnodes are assigned without any duplicates.
//         let assigned: Vec<_> = collect_assigned_vnodes(&result);
//         let unique: HashSet<_> = assigned.iter().collect();
//         assert_eq!(assigned.len(), vnodes.len());
//         assert_eq!(unique.len(), vnodes.len());
//
//         Ok(())
//     }
//
//     /// Helper to unwrap the hierarchy or panic in tests
//     fn build_hierarchy(
//         workers: &BTreeMap<i32, NonZeroUsize>,
//         actors: &[i32],
//         vnodes: &[i32],
//         salt: i32,
//     ) -> BTreeMap<i32, BTreeMap<i32, Vec<i32>>> {
//         assign_hierarchical_actor_oriented(workers, actors, vnodes, salt)
//             .unwrap_or_else(|e| panic!("Failed to build hierarchy: {}", e))
//     }
//
//     #[test]
//     fn actor_distribution_balanced() {
//         let workers = BTreeMap::from([
//             (1, NonZeroUsize::new(1).unwrap()),
//             (2, NonZeroUsize::new(1).unwrap()),
//             (3, NonZeroUsize::new(1).unwrap()),
//         ]);
//         let actors: Vec<i32> = (0..10).collect();
//         let vnodes: Vec<i32> = (0..20).collect();
//
//         let hierarchy = build_hierarchy(&workers, &actors, &vnodes, 0);
//
//         // Check the number of actors assigned to each worker, max - min <= 1.
//         let counts: Vec<usize> = hierarchy
//             .values()
//             .map(|actor_map| actor_map.len())
//             .collect();
//         let min = *counts.iter().min().unwrap();
//         let max = *counts.iter().max().unwrap();
//         assert!(
//             max - min <= 1,
//             "Actor distribution across workers is not balanced: min={}, max={}",
//             min,
//             max
//         );
//     }
//
//     #[test]
//     fn vnode_assignment_and_balance() {
//         let workers = BTreeMap::from([
//             (1, NonZeroUsize::new(1).unwrap()),
//             (2, NonZeroUsize::new(1).unwrap()),
//         ]);
//         let actors = vec![0, 1, 2, 3];
//         let vnodes: Vec<i32> = (0..8).collect();
//
//         let hierarchy = build_hierarchy(&workers, &actors, &vnodes, 42);
//
//         // 1) All actors have been assigned.
//         let mut assigned_actors: Vec<i32> = hierarchy
//             .iter()
//             .flat_map(|(_, actor_map)| actor_map.keys().cloned())
//             .collect();
//         assigned_actors.sort_unstable();
//         assigned_actors.dedup();
//         assert_eq!(assigned_actors, actors, "Not all actors were assigned");
//
//         // 2) Each actor must have at least one vnode, and the number of vnodes among actors should be balanced (max - min <= 1).
//         let mut vnode_counts: Vec<usize> = Vec::new();
//         for actor_map in hierarchy.values() {
//             for vns in actor_map.values() {
//                 assert!(!vns.is_empty(), "Actor has no vnodes assigned");
//                 vnode_counts.push(vns.len());
//             }
//         }
//         let min_v = *vnode_counts.iter().min().unwrap();
//         let max_v = *vnode_counts.iter().max().unwrap();
//         assert!(
//             max_v - min_v <= 1,
//             "VNode distribution among actors not balanced: min={}, max={}",
//             min_v,
//             max_v
//         );
//
//         // 3) The number of vnodes on each worker with vnodes should also be balanced.
//         let worker_vnode_counts: Vec<usize> = hierarchy
//             .values()
//             .map(|actor_map| actor_map.values().map(Vec::len).sum())
//             .collect();
//         let min_wv = *worker_vnode_counts.iter().min().unwrap();
//         let max_wv = *worker_vnode_counts.iter().max().unwrap();
//         assert!(
//             max_wv - min_wv <= 1,
//             "VNode distribution among workers not balanced: min={}, max={}",
//             min_wv,
//             max_wv
//         );
//
//         // 4) No vnodes are missing.
//         let mut all_vnodes: Vec<i32> = hierarchy
//             .values()
//             .flat_map(|actor_map| actor_map.values().cloned())
//             .flatten()
//             .collect();
//         all_vnodes.sort_unstable();
//         all_vnodes.dedup();
//         let mut original = vnodes.clone();
//         original.sort_unstable();
//         assert_eq!(all_vnodes, original, "Some vnodes were not assigned");
//     }
//
//     fn check_hierarchy_balanced(worker_count: usize, actor_count: usize, vnode_count: usize) {
//         // 构造输入
//         let workers: BTreeMap<i32, NonZeroUsize> = (0..worker_count)
//             .map(|i| (i as i32, NonZeroUsize::new(1).unwrap()))
//             .collect();
//         let actors: Vec<i32> = (0..actor_count).map(|i| i as i32).collect();
//         let vnodes: Vec<i32> = (0..vnode_count).map(|i| i as i32).collect();
//         let salt = 0_i32;
//
//         let hierarchy = assign_hierarchical_worker_oriented(&workers, &actors, &vnodes, salt)
//             .expect("assign_hierarchical should succeed");
//
//         println!("h {:#?}", hierarchy);
//
//         // 1. Verify that actors are distributed evenly among workers.
//         let actor_counts: Vec<usize> = hierarchy.values().map(|m| m.len()).collect();
//         let min_actors = *actor_counts.iter().min().unwrap_or(&0);
//         let max_actors = *actor_counts.iter().max().unwrap_or(&0);
//         assert!(
//             max_actors - min_actors <= 1,
//             "Actors per worker not balanced: min={}, max={}",
//             min_actors,
//             max_actors
//         );
//
//         // 2. Verify that vnodes are distributed evenly among workers.
//         let vnode_counts_per_worker: Vec<usize> = hierarchy
//             .values()
//             .map(|m| m.values().map(Vec::len).sum())
//             .collect();
//         let min_vpw = *vnode_counts_per_worker.iter().min().unwrap_or(&0);
//         let max_vpw = *vnode_counts_per_worker.iter().max().unwrap_or(&0);
//         assert!(
//             max_vpw - min_vpw <= 1,
//             "Vnodes per worker not balanced: min={}, max={}",
//             min_vpw,
//             max_vpw
//         );
//
//         // 3. Verify that all vnodes are assigned.
//         let assigned: HashSet<i32> = hierarchy
//             .values()
//             .flat_map(|m| m.values().flatten().cloned())
//             .collect();
//         assert_eq!(
//             assigned.len(),
//             vnode_count,
//             "Some vnodes were not assigned: assigned={}, expected={}",
//             assigned.len(),
//             vnode_count
//         );
//     }
//
//     #[test]
//     fn test_small_balanced() {
//         check_hierarchy_balanced(3, 5, 8); // 3 workers, 5 actors, 8 vnodes
//     }
//
//     #[test]
//     fn test_medium_balanced() {
//         check_hierarchy_balanced(4, 10, 16); // 4 workers, 10 actors, 16 vnodes
//     }
//
//     #[test]
//     fn test_equal_counts() {
//         check_hierarchy_balanced(5, 5, 5); // 5 workers, 5 actors, 5 vnodes
//     }
//
//     fn check_balanced(worker_count: usize, actor_count: usize, vnode_count: usize) {
//         let workers: BTreeMap<i32, NonZeroUsize> = (0..worker_count)
//             .map(|i| (i as i32, NonZeroUsize::new(1).unwrap()))
//             .collect();
//         let actors: Vec<i32> = (0..actor_count).map(|i| i as i32).collect();
//         let vnodes: Vec<i32> = (0..vnode_count).map(|i| i as i32).collect();
//
//         let hierarchy = assign_hierarchical_actor_oriented(&workers, &actors, &vnodes, 42_i32)
//             .expect("should succeed");
//
//         let hierarchy: HashMap<_, _> = hierarchy
//             .into_iter()
//             .filter(|(_, v)| !v.is_empty())
//             .collect();
//
//         println!("result {:?}", hierarchy);
//
//         // 1. Actors are distributed evenly among workers.
//         let actor_per_worker: Vec<usize> = hierarchy.values().map(|m| m.len()).collect();
//         let min_awo = *actor_per_worker.iter().min().unwrap_or(&0);
//         let max_awo = *actor_per_worker.iter().max().unwrap_or(&0);
//         assert!(
//             max_awo - min_awo <= 1,
//             "Actors per worker not balanced (workers={}, actors={}): min={}, max={}",
//             worker_count,
//             actor_count,
//             min_awo,
//             max_awo
//         );
//
//         // 2. Vnodes are distributed evenly among workers.
//         // let vnode_per_worker: Vec<usize> = hierarchy
//         //     .values()
//         //     .map(|m| m.values().map(Vec::len).sum())
//         //     .collect();
//         // let min_vwo = *vnode_per_worker.iter().min().unwrap_or(&0);
//         // let max_vwo = *vnode_per_worker.iter().max().unwrap_or(&0);
//         // assert!(
//         //     max_vwo - min_vwo <= 1,
//         //     "Vnodes per worker not balanced (workers={}, vnodes={}): min={}, max={}",
//         //     worker_count,
//         //     vnode_count,
//         //     min_vwo,
//         //     max_vwo
//         // );
//
//         // 3. Vnodes are distributed evenly among actors.
//         let mut vnode_per_actor = Vec::new();
//         for actor_map in hierarchy.values() {
//             for vns in actor_map.values() {
//                 vnode_per_actor.push(vns.len());
//             }
//         }
//         let min_vao = *vnode_per_actor.iter().min().unwrap_or(&0);
//         let max_vao = *vnode_per_actor.iter().max().unwrap_or(&0);
//         assert!(
//             max_vao - min_vao <= 1,
//             "Vnodes per actor not balanced (actors={}, vnodes={}): min={}, max={}",
//             actor_count,
//             vnode_count,
//             min_vao,
//             max_vao
//         );
//
//         // 4. All actors are assigned.
//         let assigned_actors: HashSet<i32> =
//             hierarchy.values().flat_map(|m| m.keys().cloned()).collect();
//         assert_eq!(
//             assigned_actors.len(),
//             actor_count,
//             "Some actors were not assigned (workers={}, actors={})",
//             worker_count,
//             actor_count
//         );
//
//         // 5. All vnodes are assigned.
//         let assigned_vnodes: HashSet<i32> = hierarchy
//             .values()
//             .flat_map(|m| m.values().flatten().cloned())
//             .collect();
//         assert_eq!(
//             assigned_vnodes.len(),
//             vnode_count,
//             "Some vnodes were not assigned (workers={}, vnodes={})",
//             worker_count,
//             vnode_count
//         );
//     }
//
//     #[test]
//     fn test_various_scenarios() {
//         // (worker_count, actor_count, vnode_count)
//         let scenarios = vec![
//             (1, 1, 1),
//             (1, 1, 5),
//             (5, 1, 5),
//             (5, 1, 10),
//             (5, 5, 5),
//             (5, 5, 10),
//             (5, 10, 10),
//             (5, 10, 20),
//             (10, 1, 10),
//             (10, 5, 10),
//             (10, 5, 20),
//             (10, 10, 20),
//             (10, 20, 20),
//             (3, 2, 3),
//             (3, 3, 5),
//             (4, 3, 4),
//             (4, 10, 40),
//             (20, 10, 20),
//         ];
//
//         for &(w, a, v) in &scenarios {
//             println!(
//                 "worker count: {}, actor count: {}, vnode count: {}",
//                 w, a, v
//             );
//             check_balanced(w, a, v);
//         }
//
//         // Test invalid scenario: actor_count > vnode_count.
//         let bad = (5, 10, 5);
//         assert!(
//             assign_hierarchical_worker_oriented(
//                 &BTreeMap::from_iter((0..bad.0).map(|i| (i, NonZeroUsize::new(1).unwrap()))),
//                 &(0..bad.1).collect::<Vec<_>>(),
//                 &(0..bad.2).collect::<Vec<_>>(),
//                 0_i32
//             )
//             .is_err(),
//             "Expected error when actors > vnodes"
//         );
//     }
// }
//
// #[cfg(test)]
// mod test2 {
//     use std::collections::BTreeMap;
//     use std::num::NonZeroUsize;
//
//     use itertools::Itertools;
//
//     use crate::stream::assign_hierarchical_worker_oriented;
//
//     #[test]
//     fn test_wtf() {
//         for weight in 1..1000 {
//             let mut workers = BTreeMap::new();
//             workers.insert(1, NonZeroUsize::new(weight).unwrap());
//             workers.insert(2, NonZeroUsize::new(1).unwrap());
//
//             let actors = (0..10).collect_vec();
//             let vnodes = (0..10).collect_vec();
//             let result =
//                 assign_hierarchical_worker_oriented(&workers, &actors, &vnodes, 0_i32).unwrap();
//
//             let x = result
//                 .values()
//                 .flat_map(|actor_map| actor_map.values().flat_map(|vns| vns.iter().cloned()))
//                 .sorted()
//                 .collect_vec();
//
//             assert_eq!(x, vnodes);
//         }
//     }
// }
