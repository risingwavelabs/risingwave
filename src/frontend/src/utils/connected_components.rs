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

use std::collections::{HashMap, HashSet};

// TODO: could try to optimize using union-find algorithm
#[derive(Debug)]
pub(crate) struct ConnectedComponentLabeller {
    vertex_to_label: HashMap<usize, usize>,
    labels_to_vertices: HashMap<usize, HashSet<usize>>,
    labels_to_edges: HashMap<usize, HashSet<(usize, usize)>>,
}

impl ConnectedComponentLabeller {
    pub(crate) fn new(vertices: usize) -> Self {
        let mut vertex_to_label = HashMap::with_capacity(vertices);
        let mut labels_to_vertices = HashMap::with_capacity(vertices);
        let labels_to_edges = HashMap::new();
        for i in 0..vertices {
            vertex_to_label.insert(i, i);
            labels_to_vertices.insert(i, vec![i].into_iter().collect());
        }
        Self {
            vertex_to_label,
            labels_to_vertices,
            labels_to_edges,
        }
    }

    pub(crate) fn add_edge(&mut self, v1: usize, v2: usize) {
        let v1_label = *self.vertex_to_label.get(&v1).unwrap();
        let v2_label = *self.vertex_to_label.get(&v2).unwrap();

        let (new_label, old_label) = if v1_label < v2_label {
            (v1_label, v2_label)
        } else {
            // v1_label > v2_label
            (v2_label, v1_label)
        };

        {
            let edges = self
                .labels_to_edges
                .entry(new_label)
                .or_insert_with(HashSet::new);

            let new_edge = if v1 < v2 { (v1, v2) } else { (v2, v1) };
            edges.insert(new_edge);
        }

        if v1_label == v2_label {
            return;
        }

        // Reassign to the smaller label
        let old_vertices = self.labels_to_vertices.remove(&old_label).unwrap();
        self.labels_to_vertices
            .get_mut(&new_label)
            .unwrap()
            .extend(old_vertices.iter());
        for v in old_vertices {
            self.vertex_to_label.insert(v, new_label);
        }
        if let Some(old_edges) = self.labels_to_edges.remove(&old_label) {
            let edges = self
                .labels_to_edges
                .entry(new_label)
                .or_insert_with(HashSet::new);
            edges.extend(old_edges);
        }
    }

    pub(crate) fn into_edge_sets(self) -> Vec<HashSet<(usize, usize)>> {
        self.labels_to_edges.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connected_component_labeller() {
        // Graph:
        // 0-1-2  3-4-5  6
        // => 0-1-2-3-4-5  6
        // => 0-1-2-3-4-5-6
        let mut labeller = ConnectedComponentLabeller::new(7);
        labeller.add_edge(0, 1);
        labeller.add_edge(1, 2);

        labeller.add_edge(3, 4);
        labeller.add_edge(4, 5);

        assert_eq!(labeller.labels_to_vertices.len(), 3);

        labeller.add_edge(2, 3);

        assert_eq!(labeller.labels_to_vertices.len(), 2);

        labeller.add_edge(5, 6);

        assert_eq!(labeller.labels_to_vertices.len(), 1);
        assert_eq!(
            *labeller.labels_to_vertices.iter().next().unwrap().1,
            (0..7).collect::<HashSet<_>>()
        );
        assert_eq!(
            *labeller.labels_to_edges.iter().next().unwrap().1,
            vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]
                .into_iter()
                .collect::<HashSet<_>>()
        );
    }
}
