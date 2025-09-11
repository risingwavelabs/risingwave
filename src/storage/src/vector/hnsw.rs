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

use std::cmp::{max, min};
use std::marker::PhantomData;

use faiss::index::hnsw::Hnsw;
use rand::Rng;
use rand::distr::uniform::{UniformFloat, UniformSampler};
use risingwave_pb::hummock::PbHnswGraph;
use risingwave_pb::hummock::hnsw_graph::{PbHnswLevel, PbHnswNeighbor, PbHnswNode};

use crate::hummock::HummockResult;
use crate::vector::utils::{BoundedNearest, MinDistanceHeap};
use crate::vector::{
    MeasureDistance, MeasureDistanceBuilder, OnNearestItem, VectorDistance, VectorItem, VectorRef,
};

#[derive(Copy, Clone)]
pub struct HnswBuilderOptions {
    pub m: usize,
    pub ef_construction: usize,
    pub max_level: usize,
}

pub fn level_m(m: usize, level: usize) -> usize {
    // borrowed from pg_vector
    // double the number of connections in ground level
    if level == 0 { 2 * m } else { m }
}

impl HnswBuilderOptions {
    fn m_l(&self) -> f32 {
        1.0 / (self.m as f32).ln()
    }
}

fn gen_level(options: &HnswBuilderOptions, rng: &mut impl Rng) -> usize {
    let level = (-UniformFloat::<f32>::sample_single(0.0, 1.0, rng)
        .unwrap()
        .ln()
        * options.m_l())
    .floor() as usize;
    min(level, options.max_level)
}

pub(crate) fn new_node(options: &HnswBuilderOptions, rng: &mut impl Rng) -> VectorHnswNode {
    let level = gen_level(options, rng);
    let mut level_neighbours = Vec::with_capacity(level + 1);
    level_neighbours
        .extend((0..=level).map(|level| BoundedNearest::new(level_m(options.m, level))));
    VectorHnswNode { level_neighbours }
}

pub(crate) struct VectorHnswNode {
    level_neighbours: Vec<BoundedNearest<usize>>,
}

impl VectorHnswNode {
    /// Returns the number of levels this node has (levels are indexed 0..num_levels-1).
    fn num_levels(&self) -> usize {
        self.level_neighbours.len()
    }
}

struct InMemoryVectorStore {
    dimension: usize,
    vector_payload: Vec<VectorItem>,
    info_payload: Vec<u8>,
    info_offsets: Vec<usize>,
}

impl InMemoryVectorStore {
    fn new(dimension: usize) -> Self {
        Self {
            dimension,
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
        let start = idx * self.dimension;
        let end = start + self.dimension;
        VectorRef::from_slice_unchecked(&self.vector_payload[start..end])
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
        assert_eq!(vec.dimension(), self.dimension);

        self.vector_payload.extend_from_slice(vec.as_slice());
        let offset = self.info_payload.len();
        self.info_payload.extend_from_slice(info);
        self.info_offsets.push(offset);
    }
}

pub trait VectorAccessor {
    fn vec_ref(&self) -> VectorRef<'_>;

    fn info(&self) -> &[u8];
}

pub trait VectorStore: 'static {
    type Accessor<'a>: VectorAccessor + 'a
    where
        Self: 'a;
    async fn get_vector(&self, idx: usize) -> HummockResult<Self::Accessor<'_>>;
}

pub struct InMemoryVectorStoreAccessor<'a> {
    vector_store_impl: &'a InMemoryVectorStore,
    idx: usize,
}

impl VectorAccessor for InMemoryVectorStoreAccessor<'_> {
    fn vec_ref(&self) -> VectorRef<'_> {
        self.vector_store_impl.vec_ref(self.idx)
    }

    fn info(&self) -> &[u8] {
        self.vector_store_impl.info(self.idx)
    }
}

impl VectorStore for InMemoryVectorStore {
    type Accessor<'a> = InMemoryVectorStoreAccessor<'a>;

    async fn get_vector(&self, idx: usize) -> HummockResult<Self::Accessor<'_>> {
        Ok(InMemoryVectorStoreAccessor {
            vector_store_impl: self,
            idx,
        })
    }
}

#[expect(clippy::len_without_is_empty)]
pub trait HnswGraph {
    fn entrypoint(&self) -> usize;
    fn len(&self) -> usize;
    /// Returns the number of levels for `idx` (levels are indexed 0..num_levels-1).
    fn node_num_levels(&self, idx: usize) -> usize;
    fn node_neighbours(
        &self,
        idx: usize,
        level: usize,
    ) -> impl Iterator<Item = (usize, VectorDistance)> + '_;
}

impl HnswGraph for PbHnswGraph {
    fn entrypoint(&self) -> usize {
        self.entrypoint_id as _
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn node_num_levels(&self, idx: usize) -> usize {
        self.nodes[idx].levels.len()
    }

    fn node_neighbours(
        &self,
        idx: usize,
        level: usize,
    ) -> impl Iterator<Item = (usize, VectorDistance)> + '_ {
        self.nodes[idx]
            .levels
            .get(level)
            .into_iter()
            .flat_map(|level| {
                level
                    .neighbors
                    .iter()
                    .map(|neighbor| (neighbor.vector_id as usize, neighbor.distance))
            })
    }
}

pub struct HnswGraphBuilder {
    /// entrypoint of the graph: Some(`entrypoint_vector_idx`)
    entrypoint: usize,
    nodes: Vec<VectorHnswNode>,
}

impl HnswGraphBuilder {
    pub(crate) fn first(node: VectorHnswNode) -> Self {
        Self {
            entrypoint: 0,
            nodes: vec![node],
        }
    }

    pub fn to_protobuf(&self) -> PbHnswGraph {
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let mut levels = Vec::with_capacity(node.num_levels());
            for level in &node.level_neighbours {
                let mut neighbors = Vec::with_capacity(level.len());
                for (distance, &neighbor_index) in level {
                    neighbors.push(PbHnswNeighbor {
                        vector_id: neighbor_index as u64,
                        distance,
                    });
                }
                levels.push(PbHnswLevel { neighbors });
            }
            nodes.push(PbHnswNode { levels });
        }
        PbHnswGraph {
            entrypoint_id: self.entrypoint as u64,
            nodes,
        }
    }

    pub fn from_protobuf(pb: &PbHnswGraph, m: usize) -> Self {
        let entrypoint = pb.entrypoint_id as usize;
        let nodes = pb
            .nodes
            .iter()
            .map(|node| {
                let level_neighbours = node
                    .levels
                    .iter()
                    .enumerate()
                    .map(|(level_idx, level)| {
                        let level_m = level_m(m, level_idx);
                        let mut nearest = BoundedNearest::new(level_m);
                        for neighbor in &level.neighbors {
                            nearest.insert(neighbor.distance, || neighbor.vector_id as _);
                        }
                        nearest
                    })
                    .collect();
                VectorHnswNode { level_neighbours }
            })
            .collect();
        Self { entrypoint, nodes }
    }
}

impl HnswGraph for HnswGraphBuilder {
    fn entrypoint(&self) -> usize {
        self.entrypoint
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn node_num_levels(&self, idx: usize) -> usize {
        self.nodes[idx].num_levels()
    }

    fn node_neighbours(
        &self,
        idx: usize,
        level: usize,
    ) -> impl Iterator<Item = (usize, VectorDistance)> + '_ {
        (&self.nodes[idx].level_neighbours[level])
            .into_iter()
            .map(|(distance, &neighbour_index)| (neighbour_index, distance))
    }
}

pub struct HnswBuilder<V: VectorStore, G: HnswGraph, M: MeasureDistanceBuilder, R: Rng> {
    options: HnswBuilderOptions,

    // payload
    vector_store: V,
    graph: Option<G>,

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

impl<M: MeasureDistanceBuilder, R: Rng> HnswBuilder<InMemoryVectorStore, HnswGraphBuilder, M, R> {
    pub fn new(dimension: usize, rng: R, options: HnswBuilderOptions) -> Self {
        Self {
            options,
            graph: None,
            vector_store: InMemoryVectorStore::new(dimension),
            rng,
            _measure: Default::default(),
        }
    }

    pub fn with_faiss_hnsw(self, faiss_hnsw: Hnsw<'_>) -> Self {
        assert_eq!(self.vector_store.len(), faiss_hnsw.levels_raw().len());
        let (entry_point, _max_level) = faiss_hnsw.entry_point().unwrap();
        let levels = faiss_hnsw.levels_raw();
        let Some(graph) = &self.graph else {
            assert_eq!(levels.len(), 0);
            return Self::new(self.vector_store.dimension, self.rng, self.options);
        };
        assert_eq!(levels.len(), graph.nodes.len());
        let mut nodes = Vec::with_capacity(graph.nodes.len());
        for (node, level_count) in levels.iter().enumerate() {
            let level_count = *level_count as usize;
            let mut level_neighbors = Vec::with_capacity(level_count);
            for level_idx in 0..level_count {
                let neighbors = faiss_hnsw.neighbors_raw(node, level_idx);
                let mut nearest_neighbors = BoundedNearest::new(max(neighbors.len(), 1));
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
            graph: Some(HnswGraphBuilder {
                entrypoint: entry_point,
                nodes,
            }),
            vector_store: self.vector_store,
            rng: self.rng,
            _measure: Default::default(),
        }
    }

    pub fn print_graph(&self) {
        let Some(graph) = &self.graph else {
            println!("empty graph");
            return;
        };
        println!(
            "entrypoint {} has {} levels",
            graph.entrypoint,
            graph.nodes[graph.entrypoint].num_levels()
        );
        for (i, node) in graph.nodes.iter().enumerate() {
            println!("node {} has {} levels", i, node.num_levels());
            for level in 0..node.num_levels() {
                print!("level {}: ", level);
                for (_, &neighbor) in &node.level_neighbours[level] {
                    print!("{} ", neighbor);
                }
                println!()
            }
        }
    }

    pub async fn insert(&mut self, vec: VectorRef<'_>, info: &[u8]) -> HummockResult<HnswStats> {
        let node = new_node(&self.options, &mut self.rng);
        let stat = if let Some(graph) = &mut self.graph {
            insert_graph::<M>(
                &self.vector_store,
                graph,
                node,
                vec,
                self.options.ef_construction,
            )
            .await?
        } else {
            self.graph = Some(HnswGraphBuilder::first(node));
            HnswStats::default()
        };
        self.vector_store.add(vec, info);
        Ok(stat)
    }
}

pub(crate) async fn insert_graph<M: MeasureDistanceBuilder>(
    vector_store: &impl VectorStore,
    graph: &mut HnswGraphBuilder,
    mut node: VectorHnswNode,
    vec: VectorRef<'_>,
    ef_construction: usize,
) -> HummockResult<HnswStats> {
    {
        let mut stats = HnswStats::default();
        let mut visited = VecSet::new(graph.nodes.len());
        let entrypoint_index = graph.entrypoint();
        let measure = M::new(vec);
        let mut entrypoints = BoundedNearest::new(1);

        entrypoints.insert(
            measure.measure(vector_store.get_vector(entrypoint_index).await?.vec_ref()),
            || (entrypoint_index, ()),
        );
        stats.distances_computed += 1;

        // Walk from entrypoint's top level down to (node_top + 1), inclusive.
        let entry_top_level_idx = graph.nodes[entrypoint_index].num_levels() - 1;
        let node_top_level_idx = node.num_levels() - 1;

        for level_idx in ((node_top_level_idx + 1)..=entry_top_level_idx).rev() {
            entrypoints = search_layer(
                vector_store,
                &*graph,
                &measure,
                |_, _, _| (),
                entrypoints,
                level_idx,
                1,
                &mut stats,
                &mut visited,
            )
            .await?;
        }

        // Connect from min(entry_top, node_top) down to ground (0).
        let start_level_idx = min(entry_top_level_idx, node_top_level_idx);
        for level_idx in (0..=start_level_idx).rev() {
            entrypoints = search_layer(
                vector_store,
                &*graph,
                &measure,
                |_, _, _| (),
                entrypoints,
                level_idx,
                ef_construction,
                &mut stats,
                &mut visited,
            )
            .await?;
            let level_neighbour = &mut node.level_neighbours[level_idx];
            for (neighbour_distance, &(neighbour_index, _)) in &entrypoints {
                level_neighbour.insert(neighbour_distance, || neighbour_index);
            }
        }

        let vector_index = graph.nodes.len();
        for (level_index, level) in node.level_neighbours.iter().enumerate() {
            for (neighbour_distance, &neighbour_index) in level {
                graph.nodes[neighbour_index].level_neighbours[level_index]
                    .insert(neighbour_distance, || vector_index);
            }
        }
        if graph.nodes[entrypoint_index].num_levels() < node.num_levels() {
            graph.entrypoint = vector_index;
        }
        graph.nodes.push(node);
        Ok(stats)
    }
}

pub async fn nearest<O: Send, M: MeasureDistanceBuilder>(
    vector_store: &impl VectorStore,
    graph: &impl HnswGraph,
    vec: VectorRef<'_>,
    on_nearest_fn: impl OnNearestItem<O>,
    ef_search: usize,
    top_n: usize,
) -> HummockResult<(Vec<O>, HnswStats)> {
    {
        // Fast path: if no exploration breadth or no results requested, do nothing.
        // Returns empty results and zeroed stats.
        let mut stats = HnswStats::default();
        if ef_search == 0 || top_n == 0 {
            return Ok((Vec::new(), stats));
        }

        let entrypoint_index = graph.entrypoint();
        let measure = M::new(vec);
        let mut entrypoints = BoundedNearest::new(1);
        let entrypoint_vector = vector_store.get_vector(entrypoint_index).await?;
        let entrypoint_distance = measure.measure(entrypoint_vector.vec_ref());
        entrypoints.insert(entrypoint_distance, || {
            (
                entrypoint_index,
                on_nearest_fn(
                    entrypoint_vector.vec_ref(),
                    entrypoint_distance,
                    entrypoint_vector.info(),
                ),
            )
        });
        stats.distances_computed += 1;
        let entry_top_level_idx = graph.node_num_levels(entrypoint_index) - 1;
        let mut visited = VecSet::new(graph.len());
        for level_idx in (1..=entry_top_level_idx).rev() {
            entrypoints = search_layer(
                vector_store,
                graph,
                &measure,
                &on_nearest_fn,
                entrypoints,
                level_idx, // level index
                1,
                &mut stats,
                &mut visited,
            )
            .await?;
        }
        entrypoints = search_layer(
            vector_store,
            graph,
            &measure,
            &on_nearest_fn,
            entrypoints,
            0,
            ef_search,
            &mut stats,
            &mut visited,
        )
        .await?;
        Ok((
            entrypoints.collect_with(|_, (_, output)| output, Some(top_n)),
            stats,
        ))
    }
}

async fn search_layer<O: Send>(
    vector_store: &impl VectorStore,
    graph: &impl HnswGraph,
    measure: &impl MeasureDistance,
    on_nearest_fn: impl OnNearestItem<O>,
    entrypoints: BoundedNearest<(usize, O)>,
    level_index: usize,
    ef: usize,
    stats: &mut HnswStats,
    visited: &mut VecSet,
) -> HummockResult<BoundedNearest<(usize, O)>> {
    #[cfg(test)]
    {
        __hnsw_test_hooks::record_level(level_index);
    }
    {
        visited.reset();

        let mut candidates = MinDistanceHeap::with_capacity(ef);
        for (distance, &(idx, _)) in &entrypoints {
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
            for (neighbour_index, _) in graph.node_neighbours(c_index, level_index) {
                if visited.is_set(neighbour_index) {
                    continue;
                }
                visited.set(neighbour_index);
                let vector = vector_store.get_vector(neighbour_index).await?;
                let info = vector.info();

                let distance = measure.measure(vector.vec_ref());

                stats.distances_computed += 1;

                let mut added = false;
                let added = &mut added;
                nearest.insert(distance, || {
                    *added = true;
                    (
                        neighbour_index,
                        on_nearest_fn(vector.vec_ref(), distance, info),
                    )
                });
                if *added {
                    candidates.push(distance, neighbour_index);
                }
            }
        }

        Ok(nearest)
    }
}

#[cfg(test)]
mod __hnsw_test_hooks {
    use std::cell::RefCell;

    thread_local! {
        #[allow(clippy::missing_const_for_thread_local)]
        static LEVELS: RefCell<Vec<usize>> = RefCell::new(Vec::new());
    }

    pub fn record_level(level: usize) {
        LEVELS.with(|v| v.borrow_mut().push(level));
    }

    pub fn take_levels() -> Vec<usize> {
        LEVELS.with(|v| std::mem::take(&mut *v.borrow_mut()))
    }

    pub fn clear_levels() {
        LEVELS.with(|v| v.borrow_mut().clear());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::iter::repeat_with;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use faiss::{ConcurrentIndex, Index, MetricType};
    use futures::executor::block_on;
    use itertools::Itertools;
    use rand::SeedableRng;
    use rand::prelude::StdRng;
    use risingwave_common::types::F32;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_common::vector::distance::InnerProductDistance;

    use super::*;
    use crate::vector::NearestBuilder;
    use crate::vector::hnsw::{HnswBuilder, HnswBuilderOptions, nearest};
    use crate::vector::test_utils::{gen_info, gen_vector};

    fn recall(actual: &Vec<Bytes>, expected: &Vec<Bytes>) -> f32 {
        let expected: HashSet<_> = expected.iter().map(|b| b.as_ref()).collect();
        (actual
            .iter()
            .filter(|info| expected.contains(info.as_ref()))
            .count() as f32)
            / (expected.len() as f32)
    }

    /// Minimal L2 distance for tests using only public traits.
    struct TestL2;

    struct TestL2Measure<'a> {
        q: VectorRef<'a>,
    }

    impl MeasureDistanceBuilder for TestL2 {
        type Measure<'a> = TestL2Measure<'a>;

        fn new<'a>(q: VectorRef<'a>) -> Self::Measure<'a> {
            TestL2Measure { q }
        }

        fn distance(a: VectorRef<'_>, b: VectorRef<'_>) -> VectorDistance {
            // Sum of squared diffs over the public slice API.
            a.as_slice()
                .iter()
                .zip_eq_debug(b.as_slice().iter())
                .map(|(&x, &y)| {
                    // VectorItem <-> f32 conversion (mirrors test_utils usage).
                    let xf: f32 = x.into();
                    let yf: f32 = y.into();
                    let d = (xf as f64) - (yf as f64);
                    d * d
                })
                .sum::<f64>()
        }
    }

    impl<'a> MeasureDistance for TestL2Measure<'a> {
        fn measure(&self, v: VectorRef<'_>) -> VectorDistance {
            TestL2::distance(self.q, v)
        }
    }

    fn opts(m: usize, efc: usize, max_level: usize) -> HnswBuilderOptions {
        HnswBuilderOptions {
            m,
            ef_construction: efc,
            max_level,
        }
    }

    const VERBOSE: bool = false;
    const VECTOR_LEN: usize = 128;
    const INPUT_COUNT: usize = 20000;
    const QUERY_COUNT: usize = 5000;
    const TOP_N: usize = 10;
    const EF_SEARCH_LIST: &[usize] = &[16];
    // const EF_SEARCH_LIST: &'static [usize] = &[16, 30, 100];

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_hnsw_basic() {
        let input = (0..INPUT_COUNT)
            .map(|i| (gen_vector(VECTOR_LEN), gen_info(i)))
            .collect_vec();
        let m = 40;
        let hnsw_start_time = Instant::now();
        let mut hnsw_builder = HnswBuilder::<_, _, InnerProductDistance, _>::new(
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
            hnsw_builder.insert(vec.to_ref(), info).await.unwrap();
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
            .add(F32::inner_slice(&hnsw_builder.vector_store.vector_payload))
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
                    .assign(query.as_raw_slice(), TOP_N)
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
                        let (actual, stats) = block_on(nearest::<_, InnerProductDistance>(
                            &hnsw_builder.vector_store,
                            hnsw_builder.graph.as_ref().unwrap(),
                            query.to_ref(),
                            |_, _, info| Bytes::copy_from_slice(info),
                            ef_search,
                            TOP_N,
                        ))
                        .unwrap();
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

    // Visits in insert_graph upper-layer descent should be: entry_top, entry_top-1, ..., node_top+1 (inclusive).
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn hnsw_insert_graph_visits_expected_upper_layers() -> HummockResult<()> {
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        use super::__hnsw_test_hooks as hooks;

        // Use the same options helper from this test module.
        let dim = 8;
        let options = opts(8, 16, 8); // m, ef_construction, max_level

        // Try a handful of seeds so we reliably get an entry node with >= 3 levels.
        // This remains deterministic across runs.
        for seed in 1u64..=200 {
            // Fresh builder per attempt so the RNG state matches expectations.
            let mut hnsw: HnswBuilder<InMemoryVectorStore, HnswGraphBuilder, TestL2, StdRng> =
                HnswBuilder::new(dim, StdRng::seed_from_u64(seed), options);

            // Insert first vector: becomes the entrypoint.
            let v0 = gen_vector(dim);
            let _ = hnsw
                .insert(VectorRef::from_slice_unchecked(v0.as_slice()), &gen_info(0))
                .await?;

            // Peek current entrypoint's top level index.
            let g = hnsw.graph.as_ref().unwrap();
            let entry_idx = g.entrypoint();
            let entry_top_level_idx = g.node_num_levels(entry_idx) - 1;

            // We need at least 2 upper layers to make the assertion interesting.
            if entry_top_level_idx < 2 {
                continue; // try next seed
            }

            // Insert second vector and record which levels search_layer visits.
            hooks::clear_levels();
            let v1 = gen_vector(dim);
            let _ = hnsw
                .insert(VectorRef::from_slice_unchecked(v1.as_slice()), &gen_info(1))
                .await?;

            // After insertion, read the new node's level count (it’s at the tail).
            let g = hnsw.graph.as_ref().unwrap();
            let new_idx = g.len() - 1;
            let node_top_level_idx = g.node_num_levels(new_idx) - 1;

            // If the new node's top level > entry_top, HNSW would promote it to entrypoint.
            // That changes the descent semantics; skip such seeds to keep the assertion crisp.
            if node_top_level_idx > entry_top_level_idx {
                continue;
            }

            // What the algorithm *should* have visited on the first descent:
            let expected: Vec<usize> = ((node_top_level_idx + 1)..=entry_top_level_idx)
                .rev()
                .collect();

            // Extract the actual visited levels (recorded at the start of search_layer).
            let visited = hooks::take_levels();
            // Keep only *upper-layer* calls (>= 1); the final ground pass is level 0.
            let upper: Vec<usize> = visited.into_iter().filter(|&l| l >= 1).collect();

            assert_eq!(
                upper, expected,
                "seed={seed}, entry_top_level_idx={entry_top_level_idx}, node_top_level_idx={node_top_level_idx}"
            );
            return Ok(()); // success on this seed
        }

        panic!(
            "could not find a suitable seed (entry_top_level_idx>=2 and node_top<=entry_top_level_idx) within the search window"
        );
    }

    // Visits in nearest upper-layer descent should be: entry_top, entry_top-1, ..., 1 (then the ground pass at 0 separately).
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn hnsw_nearest_visits_expected_upper_layers() -> HummockResult<()> {
        use super::__hnsw_test_hooks as hooks;

        // Build minimal graph with one node of 3 levels (indices 0,1,2).
        let dim = 1;
        let mut store = InMemoryVectorStore::new(dim);
        let v0 = gen_vector(dim);
        store.add(VectorRef::from_slice_unchecked(v0.as_slice()), &gen_info(0));

        let graph = HnswGraphBuilder {
            entrypoint: 0,
            nodes: vec![VectorHnswNode {
                level_neighbours: (0..3).map(|_| BoundedNearest::new(0)).collect(),
            }],
        };

        // Query doesn't matter; we just want to observe level calls.
        let q = gen_vector(dim);

        hooks::clear_levels();

        // ef_search >= 1 to ensure we do the usual traversal.
        let (_out, _stats) = nearest::<usize, TestL2>(
            &store,
            &graph,
            VectorRef::from_slice_unchecked(q.as_slice()),
            |_v, _d, _info| 0usize,
            4,
            1,
        )
        .await?;

        let visited = hooks::take_levels();

        // Extract only upper-layer visits (>= 1). Ground layer (0) is handled later and isn't part of this loop.
        let upper: Vec<usize> = visited.into_iter().filter(|&l| l >= 1).collect();

        // With entry_top_level_idx = 2, we expect visits at levels [2, 1] in that order.
        assert_eq!(
            upper,
            vec![2, 1],
            "nearest should visit levels [2, 1] top-down before level 0"
        );
        Ok(())
    }

    #[cfg(not(madsim))]
    #[test]
    fn hnsw_vector_hnsw_node_level_returns_count() {
        // Construct a node with 3 level_neighbours (indices 0, 1, 2).
        let node = VectorHnswNode {
            level_neighbours: (0..3).map(|_| BoundedNearest::new(0)).collect(),
        };

        // By contract, `num_level()` should return the COUNT (3), not the max index (2).
        assert_eq!(
            node.num_levels(),
            node.level_neighbours.len(),
            "VectorHnswNode::num_levels() must return the count of levels, \
             not the maximum level index"
        );
    }

    #[cfg(not(madsim))]
    #[test]
    fn hnsw_graph_node_level_returns_count() {
        // Graph with a single node with 4 levels (indices 0,1,2,3).
        let graph = HnswGraphBuilder {
            entrypoint: 0,
            nodes: vec![VectorHnswNode {
                level_neighbours: (0..4).map(|_| BoundedNearest::new(0)).collect(),
            }],
        };

        assert_eq!(
            graph.node_num_levels(0),
            4,
            "HnswGraph::node_level() must return the COUNT of levels, not the max index"
        );
    }
}
