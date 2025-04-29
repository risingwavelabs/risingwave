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

use std::cmp::min;
use std::marker::PhantomData;

use faiss::index::hnsw::Hnsw;
use rand::Rng;
use rand::distr::uniform::{UniformFloat, UniformSampler};

use crate::vector::utils::{BoundedNearest, MinDistanceHeap};
use crate::vector::{
    MeasureDistance, MeasureDistanceBuilder, OnNearestItem, VectorInner, VectorItem, VectorRef,
};

pub struct HnswBuilderOptions {
    m: usize,
    ef_construction: usize,
    max_level: usize,
}

impl HnswBuilderOptions {
    fn level_m(&self, level: usize) -> usize {
        // borrowed from pg_vector
        // double the number of connections in ground level
        if level == 0 { 2 * self.m } else { self.m }
    }

    fn m_l(&self) -> f32 {
        1.0 / (self.m as f32).ln()
    }
}

struct VectorHnswNode {
    level_neighbours: Vec<BoundedNearest<usize>>,
}

impl VectorHnswNode {
    fn level(&self) -> usize {
        self.level_neighbours.len()
    }
}

struct VectorStore {
    vector_len: usize,
    vector_payload: Vec<VectorItem>,
    info_payload: Vec<u8>,
    info_offsets: Vec<usize>,
}

impl VectorStore {
    fn new(vector_len: usize) -> Self {
        Self {
            vector_len,
            vector_payload: vec![],
            info_payload: Default::default(),
            info_offsets: vec![],
        }
    }

    fn len(&self) -> usize {
        self.info_offsets.len()
    }

    fn vec_ref(&self, idx: usize) -> VectorRef<'_> {
        assert!(idx < self.info_offsets.len());
        let start = idx * self.vector_len;
        let end = start + self.vector_len;
        VectorInner(&self.vector_payload[start..end])
    }

    fn info(&self, idx: usize) -> &[u8] {
        let start = self.info_offsets[idx];
        let end = if idx < self.info_offsets.len() - 1 {
            self.info_offsets[idx + 1]
        } else {
            self.info_payload.len()
        };
        &self.info_payload[start..end]
    }

    fn add(&mut self, vec: VectorRef<'_>, info: &[u8]) {
        assert_eq!(vec.0.len(), self.vector_len);

        self.vector_payload.extend_from_slice(vec.0);
        let offset = self.info_payload.len();
        self.info_payload.extend_from_slice(info);
        self.info_offsets.push(offset);
    }
}

pub struct HnswBuilder<M: MeasureDistanceBuilder, R: Rng> {
    options: HnswBuilderOptions,
    m_l: f32,

    // metadata
    /// entrypoint of the graph: Some(`entrypoint_vector_idx`)
    entrypoint: Option<usize>,

    // payload
    vector_store: VectorStore,
    nodes: Vec<VectorHnswNode>,

    // utils
    rng: R,
    _measure: PhantomData<M>,
}

#[derive(Default, Debug)]
pub struct HnswStats {
    distances_computed: usize,
    nhops: usize,
}

struct VecSet {
    // TODO: optimize with bitmap
    payload: Vec<bool>,
}

impl VecSet {
    fn new(size: usize) -> Self {
        Self {
            payload: vec![false; size],
        }
    }

    fn set(&mut self, idx: usize) {
        self.payload[idx] = true;
    }

    fn is_set(&self, idx: usize) -> bool {
        self.payload[idx]
    }

    fn reset(&mut self) {
        self.payload.fill(false);
    }
}

impl<M: MeasureDistanceBuilder, R: Rng> HnswBuilder<M, R> {
    pub fn new(vector_len: usize, rng: R, options: HnswBuilderOptions) -> Self {
        let m_l = options.m_l();
        Self {
            options,
            m_l,
            entrypoint: None,
            vector_store: VectorStore::new(vector_len),
            nodes: vec![],
            rng,
            _measure: Default::default(),
        }
    }

    pub fn with_faiss_hnsw(self, faiss_hnsw: Hnsw<'_>) -> Self {
        assert_eq!(self.vector_store.len(), faiss_hnsw.levels_raw().len());
        let (entry_point, _max_level) = faiss_hnsw.entry_point().unwrap();
        let levels = faiss_hnsw.levels_raw();
        assert_eq!(levels.len(), self.nodes.len());
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for (node, level_count) in levels.iter().enumerate() {
            let level_count = *level_count as usize;
            let mut level_neighbors = Vec::with_capacity(level_count);
            for level in 0..level_count {
                let neighbors = faiss_hnsw.neighbors_raw(node, level);
                let mut nearest_neighbors = BoundedNearest::new(neighbors.len());
                for &neighbor in neighbors {
                    nearest_neighbors.insert(
                        M::distance(
                            self.vector_store.vec_ref(node),
                            self.vector_store.vec_ref(neighbor as _),
                        ),
                        || neighbor as _,
                    );
                }
                level_neighbors.push(nearest_neighbors);
            }
            nodes.push(VectorHnswNode {
                level_neighbours: level_neighbors,
            });
        }
        Self {
            options: self.options,
            m_l: self.m_l,
            entrypoint: Some(entry_point),
            vector_store: self.vector_store,
            nodes,
            rng: self.rng,
            _measure: Default::default(),
        }
    }

    pub fn print_graph(&self) {
        let Some(entrypoint) = self.entrypoint else {
            println!("empty graph");
            return;
        };
        println!(
            "entrypoint {} in level {}",
            entrypoint,
            self.nodes[entrypoint].level()
        );
        for (i, node) in self.nodes.iter().enumerate() {
            println!("node {} has {} levels", i, node.level());
            for level in 0..node.level() {
                print!("level {}: ", level);
                for (_, &neighbor) in &node.level_neighbours[level] {
                    print!("{} ", neighbor);
                }
                println!()
            }
        }
    }

    pub fn insert(&mut self, vec: VectorRef<'_>, info: &[u8]) -> HnswStats {
        let mut node = self.new_node();
        let Some(entrypoint_index_mut) = &mut self.entrypoint else {
            // first vector, no neighbour, just return
            self.entrypoint = Some(self.nodes.len());
            self.nodes.push(node);
            self.vector_store.add(vec, info);
            return HnswStats::default();
        };
        let mut stats = HnswStats::default();
        let entrypoint_index = *entrypoint_index_mut;
        let measure = M::new(vec);
        let mut entrypoints = BoundedNearest::new(1);
        entrypoints.insert(
            measure.measure(self.vector_store.vec_ref(entrypoint_index)),
            || entrypoint_index,
        );
        let mut visited = VecSet::new(self.nodes.len());
        let entrypoint_level = self.nodes[entrypoint_index].level();
        {
            let mut curr_level = entrypoint_level;
            while curr_level > node.level() + 1 {
                curr_level -= 1;
                entrypoints = Self::search_layer(
                    &self.vector_store,
                    &self.nodes,
                    &measure,
                    entrypoints,
                    curr_level,
                    1,
                    &mut stats,
                    &mut visited,
                );
            }
        }
        {
            let mut curr_level = min(entrypoint_level, node.level());
            while curr_level > 0 {
                curr_level -= 1;
                entrypoints = Self::search_layer(
                    &self.vector_store,
                    &self.nodes,
                    &measure,
                    entrypoints,
                    curr_level,
                    self.options.ef_construction,
                    &mut stats,
                    &mut visited,
                );
                let level_neighbour = &mut node.level_neighbours[curr_level];
                for (neighbour_distance, &neighbour_index) in &entrypoints {
                    level_neighbour.insert(neighbour_distance, || neighbour_index);
                }
            }
        }
        let vector_index = self.nodes.len();
        for (level_index, level) in node.level_neighbours.iter().enumerate() {
            for (neighbour_distance, &neighbour_index) in level {
                self.nodes[neighbour_index].level_neighbours[level_index]
                    .insert(neighbour_distance, || vector_index);
            }
        }
        if self.nodes[entrypoint_index].level() < node.level() {
            *entrypoint_index_mut = vector_index;
        }
        self.nodes.push(node);
        self.vector_store.add(vec, info);
        stats
    }

    pub fn nearest<O>(
        &self,
        vec: VectorRef<'_>,
        on_nearest_fn: impl OnNearestItem<O>,
        ef_search: usize,
        top_n: usize,
    ) -> (Vec<O>, HnswStats) {
        let Some(entrypoint_index) = self.entrypoint else {
            assert!(self.nodes.is_empty());
            return (vec![], HnswStats::default());
        };
        let measure = M::new(vec);
        let mut entrypoints = BoundedNearest::new(1);
        let mut stats = HnswStats::default();
        entrypoints.insert(
            measure.measure(self.vector_store.vec_ref(entrypoint_index)),
            || entrypoint_index,
        );
        stats.distances_computed += 1;
        let entrypoint_level = self.nodes[entrypoint_index].level();
        let mut visited = VecSet::new(self.nodes.len());
        {
            let mut curr_level = entrypoint_level;
            while curr_level > 1 {
                curr_level -= 1;
                entrypoints = Self::search_layer(
                    &self.vector_store,
                    &self.nodes,
                    &measure,
                    entrypoints,
                    curr_level,
                    1,
                    &mut stats,
                    &mut visited,
                );
            }
        }
        entrypoints = Self::search_layer(
            &self.vector_store,
            &self.nodes,
            &measure,
            entrypoints,
            0,
            ef_search,
            &mut stats,
            &mut visited,
        );
        let mut ret = BoundedNearest::new(top_n);
        for (neighbour_distance, &neighbour_index) in &entrypoints {
            ret.insert(neighbour_distance, || {
                on_nearest_fn(
                    self.vector_store.vec_ref(neighbour_index),
                    neighbour_distance,
                    self.vector_store.info(neighbour_index),
                )
            });
        }
        (ret.collect(), stats)
    }

    fn search_layer(
        vector_store: &VectorStore,
        nodes: &Vec<VectorHnswNode>,
        measure: &M::Measure<'_>,
        entrypoints: BoundedNearest<usize>,
        level_index: usize,
        ef: usize,
        stats: &mut HnswStats,
        visited: &mut VecSet,
    ) -> BoundedNearest<usize> {
        visited.reset();
        let mut candidates = MinDistanceHeap::with_capacity(ef);
        for (distance, &idx) in &entrypoints {
            visited.set(idx);
            candidates.push(distance, idx);
        }
        let mut nearest = entrypoints;
        nearest.resize(ef);

        while let Some((c_distance, c_index)) = candidates.pop() {
            let (f_distance, _) = nearest.furthest().expect("non-empty");
            if c_distance > f_distance {
                // early break here when even the nearest node in `candidates` is further than the
                // furthest node in the `nearest` set, because no node in `candidates` can be added to `nearest`
                break;
            }
            stats.nhops += 1;
            for (_, &neighbour_index) in &nodes[c_index].level_neighbours[level_index] {
                if visited.is_set(neighbour_index) {
                    continue;
                }
                visited.set(neighbour_index);
                let distance = measure.measure(vector_store.vec_ref(neighbour_index));
                stats.distances_computed += 1;
                let mut added = false;
                let added = &mut added;
                nearest.insert(distance, || {
                    *added = true;
                    neighbour_index
                });
                if *added {
                    candidates.push(distance, neighbour_index);
                }
            }
        }

        nearest
    }

    fn gen_level(&mut self) -> usize {
        let level = (-UniformFloat::<f32>::sample_single(0.0, 1.0, &mut self.rng)
            .unwrap()
            .ln()
            * self.m_l)
            .floor() as usize;
        min(level, self.options.max_level)
    }

    fn new_node(&mut self) -> VectorHnswNode {
        let level = self.gen_level();
        let mut level_neighbours = Vec::with_capacity(level);
        level_neighbours
            .extend((0..=level).map(|level| BoundedNearest::new(self.options.level_m(level))));
        VectorHnswNode { level_neighbours }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::iter::repeat_with;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use faiss::{ConcurrentIndex, Index, MetricType};
    use itertools::Itertools;
    use rand::SeedableRng;
    use rand::prelude::StdRng;

    use crate::vector::NearestBuilder;
    use crate::vector::distance::InnerProductDistance;
    use crate::vector::hnsw::{HnswBuilder, HnswBuilderOptions};
    use crate::vector::test_utils::{gen_info, gen_vector};

    fn recall(actual: &Vec<Bytes>, expected: &Vec<Bytes>) -> f32 {
        let expected: HashSet<_> = expected.iter().map(|b| b.as_ref()).collect();
        (actual
            .iter()
            .filter(|info| expected.contains(info.as_ref()))
            .count() as f32)
            / (expected.len() as f32)
    }

    const VERBOSE: bool = false;
    const VECTOR_LEN: usize = 128;
    const INPUT_COUNT: usize = 20000;
    const QUERY_COUNT: usize = 5000;
    const TOP_N: usize = 10;
    const EF_SEARCH_LIST: &[usize] = &[16];
    // const EF_SEARCH_LIST: &'static [usize] = &[16, 30, 100];

    #[test]
    fn test_hnsw_basic() {
        let input = (0..INPUT_COUNT)
            .map(|i| (gen_vector(VECTOR_LEN), gen_info(i)))
            .collect_vec();
        let m = 40;
        let hnsw_start_time = Instant::now();
        let mut hnsw_builder = HnswBuilder::<InnerProductDistance, _>::new(
            VECTOR_LEN,
            StdRng::seed_from_u64(233),
            // StdRng::try_from_os_rng().unwrap(),
            HnswBuilderOptions {
                m,
                ef_construction: 40,
                max_level: 10,
            },
        );
        for (vec, info) in &input {
            hnsw_builder.insert(vec.to_ref(), info);
        }
        println!("hnsw build time: {:?}", hnsw_start_time.elapsed());
        if VERBOSE {
            hnsw_builder.print_graph();
        }

        let faiss_hnsw_start_time = Instant::now();
        let mut faiss_hnsw = faiss::index::hnsw::HnswFlatIndex::new(
            VECTOR_LEN as _,
            m as _,
            MetricType::InnerProduct,
        )
        .unwrap();

        faiss_hnsw
            .add(&hnsw_builder.vector_store.vector_payload)
            .unwrap();
        // for (vec, info) in &input {
        //     faiss_hnsw.add(&vec.0).unwrap();
        // }

        if VERBOSE {
            let faiss_hnsw = faiss_hnsw.hnsw();
            let (entry_point, max_level) = faiss_hnsw.entry_point().unwrap();
            println!("faiss hnsw entry_point: {} {}", entry_point, max_level);
            let levels = faiss_hnsw.levels_raw();
            println!("entry point level: {}", levels[entry_point]);
            for level in 0..=max_level {
                let neighbors = faiss_hnsw.neighbors_raw(entry_point, level);
                println!("entry point level {} neighbors {:?}", level, neighbors);
            }
        }
        println!(
            "faiss hnsw build time: {:?}",
            faiss_hnsw_start_time.elapsed()
        );

        // let hnsw_builder = hnsw_builder.with_faiss_hnsw(faiss_hnsw.hnsw());

        let queries = (0..QUERY_COUNT)
            .map(|_| gen_vector(VECTOR_LEN))
            .collect_vec();
        let expected = queries
            .iter()
            .map(|query| {
                let mut nearest_builder =
                    NearestBuilder::<'_, _, InnerProductDistance>::new(query.to_ref(), TOP_N);
                nearest_builder.add(
                    input
                        .iter()
                        .map(|(vec, info)| (vec.to_ref(), info.as_ref())),
                    |_, _, info| Bytes::copy_from_slice(info),
                );
                nearest_builder.finish()
            })
            .collect_vec();
        let faiss_start_time = Instant::now();
        let repeat_query = if cfg!(debug_assertions) { 1 } else { 60 };
        println!("start faiss query");
        let faiss_actual = repeat_with(|| queries.iter().enumerate())
            .take(repeat_query)
            .flatten()
            .map(|(i, query)| {
                let start_time = Instant::now();
                let actual = faiss_hnsw
                    .assign(&query.0, TOP_N)
                    .unwrap()
                    .labels
                    .into_iter()
                    .filter_map(|i| i.get().map(|i| gen_info(i as _)))
                    .collect_vec();
                let recall = recall(&actual, &expected[i]);
                (start_time.elapsed(), recall)
            })
            .collect_vec();
        let faiss_query_time = faiss_start_time.elapsed();
        println!("start query");
        let actuals = EF_SEARCH_LIST
            .iter()
            .map(|&ef_search| {
                let start_time = Instant::now();
                let actuals = repeat_with(|| queries.iter().enumerate())
                    .take(repeat_query)
                    .flatten()
                    .map(|(i, query)| {
                        let start_time = Instant::now();
                        let (actual, stats) = hnsw_builder.nearest(
                            query.to_ref(),
                            |_, _, info| Bytes::copy_from_slice(info),
                            ef_search,
                            TOP_N,
                        );
                        if VERBOSE {
                            println!("stats: {:?}", stats);
                        }
                        let recall = recall(&actual, &expected[i]);
                        (start_time.elapsed(), recall)
                    })
                    .collect_vec();
                (actuals, start_time.elapsed())
            })
            .collect_vec();
        if VERBOSE {
            for i in 0..20 {
                for elapsed in [&faiss_actual]
                    .into_iter()
                    .chain(actuals.iter().map(|(actual, _)| actual))
                    .map(|actual| actual[i].0)
                {
                    print!("{:?}\t", elapsed);
                }
                println!();
                for recall in [&faiss_actual]
                    .into_iter()
                    .chain(actuals.iter().map(|(actual, _)| actual))
                    .map(|actual| actual[i].1)
                {
                    print!("{}\t", recall);
                }
                println!();
            }
        }
        fn avg_recall(actual: &Vec<(Duration, f32)>) -> f32 {
            actual.iter().map(|(_, elapsed)| *elapsed).sum::<f32>() / (actual.len() as f32)
        }
        println!("faiss {:?} {}", faiss_query_time, avg_recall(&faiss_actual));
        for i in 0..EF_SEARCH_LIST.len() {
            println!(
                "ef_search[{}] {:?} {}",
                EF_SEARCH_LIST[i],
                actuals[i].1,
                avg_recall(&actuals[i].0)
            );
        }
    }
}
