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

//! This module contains the logic for matching the internal state tables of two streaming jobs,
//! used for replacing a streaming job (typically `ALTER MV`) while preserving the existing state.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash as _, Hasher as _};

use itertools::Itertools;
use risingwave_common::catalog::TableDesc;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_tables_inner;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::stream_plan::StreamNode;
use strum::IntoDiscriminant;

use crate::model::StreamJobFragments;
use crate::stream::StreamFragmentGraph;

/// Helper type for describing a [`StreamNode`] in error messages.
pub(crate) struct StreamNodeDesc(Box<str>);

impl From<&StreamNode> for StreamNodeDesc {
    fn from(node: &StreamNode) -> Self {
        let id = node.operator_id;
        let identity = &node.identity;
        let body = node.node_body.as_ref().unwrap();

        Self(format!("{}({}, {})", body, id, identity).into_boxed_str())
    }
}

impl std::fmt::Display for StreamNodeDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error type for failed state table matching.
#[derive(thiserror::Error, thiserror_ext::Macro, thiserror_ext::ReportDebug)]
pub(crate) enum Error {
    #[error("failed to match graph: {message}")]
    Graph { message: String },

    #[error("failed to match fragment {id}: {message}")]
    Fragment {
        source: Option<Box<Error>>,
        id: Id,
        message: String,
    },

    #[error("failed to match operator {from} to {to}: {message}")]
    Operator {
        from: StreamNodeDesc,
        to: StreamNodeDesc,
        message: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Fragment id.
type Id = u32;

/// Node for a fragment in the [`Graph`].
struct Fragment {
    /// The fragment id.
    id: Id,
    /// The root node of the fragment.
    root: StreamNode,
}

/// A streaming job graph that's used for state table matching.
pub struct Graph {
    /// All fragments in the graph.
    nodes: HashMap<Id, Fragment>,
    /// Downstreams of each fragment.
    downstreams: HashMap<Id, Vec<Id>>,
    /// Upstreams of each fragment.
    upstreams: HashMap<Id, Vec<Id>>,
}

impl Graph {
    /// Returns the number of fragments in the graph.
    fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the downstreams of a fragment.
    fn downstreams(&self, id: Id) -> &[Id] {
        self.downstreams.get(&id).map_or(&[], |v| v.as_slice())
    }

    /// Returns the upstreams of a fragment.
    fn upstreams(&self, id: Id) -> &[Id] {
        self.upstreams.get(&id).map_or(&[], |v| v.as_slice())
    }

    /// Returns the topological order of the graph.
    fn topo_order(&self) -> Result<Vec<Id>> {
        let mut topo = Vec::new();
        let mut downstream_cnts = HashMap::new();

        // Iterate all nodes to find the root and initialize the downstream counts.
        for node_id in self.nodes.keys() {
            let downstream_cnt = self.downstreams(*node_id).len();
            if downstream_cnt == 0 {
                topo.push(*node_id);
            } else {
                downstream_cnts.insert(*node_id, downstream_cnt);
            }
        }

        let mut i = 0;
        while let Some(&node_id) = topo.get(i) {
            i += 1;
            // Find if we can process more nodes.
            for &upstream_id in self.upstreams(node_id) {
                let downstream_cnt = downstream_cnts.get_mut(&upstream_id).unwrap();
                *downstream_cnt -= 1;
                if *downstream_cnt == 0 {
                    downstream_cnts.remove(&upstream_id);
                    topo.push(upstream_id);
                }
            }
        }

        if !downstream_cnts.is_empty() {
            // There are nodes that are not processed yet.
            bail_graph!("fragment graph is not a DAG");
        }
        assert_eq!(topo.len(), self.len());

        Ok(topo)
    }

    /// Calculates the fingerprints of all fragments based on their position in the graph.
    ///
    /// This is used to locate the candidates when matching a fragment against another graph.
    fn fingerprints(&self) -> Result<HashMap<Id, u64>> {
        let mut fps = HashMap::new();

        let order = self.topo_order()?;
        for u in order.into_iter().rev() {
            let upstream_fps = self
                .upstreams(u)
                .iter()
                .map(|id| *fps.get(id).unwrap())
                .sorted() // allow input order to be arbitrary
                .collect_vec();

            // Hash the downstream count, upstream count, and upstream fingerprints to
            // generate the fingerprint of this node.
            let mut hasher = DefaultHasher::new();
            (
                self.upstreams(u).len(),
                self.downstreams(u).len(),
                upstream_fps,
            )
                .hash(&mut hasher);
            let fingerprint = hasher.finish();

            fps.insert(u, fingerprint);
        }

        Ok(fps)
    }
}

/// The match result of a fragment in the source graph to a fragment in the target graph.
struct Match {
    /// The target fragment id.
    target: Id,
    /// The mapping from source table id to target table id within the fragment.
    table_matches: HashMap<u32, u32>,
}

/// The successful matching result of two [`Graph`]s.
struct Matches {
    /// The mapping from source fragment id to target fragment id.
    inner: HashMap<Id, Match>,
    /// The set of target fragment ids that are already matched.
    matched_targets: HashSet<Id>,
}

impl Matches {
    /// Creates a new empty match result.
    fn new() -> Self {
        Self {
            inner: HashMap::new(),
            matched_targets: HashSet::new(),
        }
    }

    /// Returns the target fragment id of a source fragment id.
    fn target(&self, u: Id) -> Option<Id> {
        self.inner.get(&u).map(|m| m.target)
    }

    /// Returns the number of matched fragments.
    fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the source fragment is already matched.
    fn matched(&self, u: Id) -> bool {
        self.inner.contains_key(&u)
    }

    /// Returns true if the target fragment is already matched.
    fn target_matched(&self, v: Id) -> bool {
        self.matched_targets.contains(&v)
    }

    /// Tries to match a source fragment to a target fragment. If successful, they will be recorded
    /// in the match result.
    ///
    /// This will check the operators and internal tables of the fragment.
    fn try_match(&mut self, u: &Fragment, v: &Fragment) -> Result<()> {
        if self.matched(u.id) {
            panic!("fragment {} was already matched", u.id);
        }

        // Collect the internal tables of a node (not visiting children).
        let collect_tables = |x: &StreamNode| {
            let mut tables = Vec::new();
            visit_stream_node_tables_inner(&mut x.clone(), true, false, |table, name| {
                tables.push((name.to_owned(), table.clone()));
            });
            tables
        };

        let mut table_matches = HashMap::new();

        // Use BFS to match the operator nodes.
        let mut uq = VecDeque::from([&u.root]);
        let mut vq = VecDeque::from([&v.root]);

        while let Some(mut un) = uq.pop_front() {
            // Since we ensure the number of inputs of an operator is the same before extending
            // the BFS queue, we can safely unwrap here.
            let mut vn = vq.pop_front().unwrap();

            // Skip while the node is stateless and has only one input.
            let mut u_tables = collect_tables(un);
            while u_tables.is_empty() && un.input.len() == 1 {
                un = &un.input[0];
                u_tables = collect_tables(un);
            }
            let mut v_tables = collect_tables(vn);
            while v_tables.is_empty() && vn.input.len() == 1 {
                vn = &vn.input[0];
                v_tables = collect_tables(vn);
            }

            // If we reach the leaf node, we are done of this fragment.
            if un.input.is_empty() && vn.input.is_empty() {
                continue;
            }

            // Perform checks.
            if un.node_body.as_ref().unwrap().discriminant()
                != vn.node_body.as_ref().unwrap().discriminant()
            {
                bail_operator!(from = un, to = vn, "operator has different type");
            }
            if un.input.len() != vn.input.len() {
                bail_operator!(
                    from = un,
                    to = vn,
                    "operator has different number of inputs ({} vs {})",
                    un.input.len(),
                    vn.input.len()
                );
            }

            // Extend the BFS queue.
            uq.extend(un.input.iter());
            vq.extend(vn.input.iter());

            for (ut_name, ut) in u_tables {
                let vt_cands = v_tables
                    .extract_if(.., |(vt_name, _)| *vt_name == ut_name)
                    .collect_vec();

                if vt_cands.is_empty() {
                    bail_operator!(
                        from = un,
                        to = vn,
                        "cannot find a match for table `{ut_name}`",
                    );
                } else if vt_cands.len() > 1 {
                    bail_operator!(
                        from = un,
                        to = vn,
                        "found multiple matches for table `{ut_name}`",
                    );
                }

                let (_, vt) = vt_cands.into_iter().next().unwrap();

                // Since the requirement is to ensure the state compatibility, we focus solely on
                // the "physical" part of the table, best illustrated by `TableDesc`.
                let table_desc_for_compare = |table: &PbTable| {
                    let mut table = table.clone();
                    table.id = 0; // ignore id
                    table.maybe_vnode_count = Some(42); // vnode count is unfilled for new fragment graph, fill it with a dummy value before proceeding

                    TableDesc::from_pb_table(&table)
                };

                let ut_compare = table_desc_for_compare(&ut);
                let vt_compare = table_desc_for_compare(&vt);

                if ut_compare != vt_compare {
                    bail_operator!(
                        from = un,
                        to = vn,
                        "found a match for table `{ut_name}`, but they are incompatible, diff:\n{}",
                        pretty_assertions::Comparison::new(&ut_compare, &vt_compare)
                    );
                }

                table_matches.try_insert(ut.id, vt.id).unwrap_or_else(|_| {
                    panic!("duplicated table id {} in fragment {}", ut.id, u.id)
                });
            }
        }

        let m = Match {
            target: v.id,
            table_matches,
        };
        self.inner.insert(u.id, m);
        self.matched_targets.insert(v.id);

        Ok(())
    }

    /// Undoes the match of a source fragment.
    fn undo_match(&mut self, u: Id) {
        let target = self
            .inner
            .remove(&u)
            .unwrap_or_else(|| panic!("fragment {} was not previously matched", u))
            .target;

        let target_removed = self.matched_targets.remove(&target);
        assert!(target_removed);
    }

    /// Converts the match result into a table mapping.
    fn into_table_mapping(self) -> HashMap<u32, u32> {
        self.inner
            .into_iter()
            .flat_map(|(_, m)| m.table_matches.into_iter())
            .collect()
    }
}

/// Matches two [`Graph`]s, and returns the match result from each fragment in `g1` to `g2`.
fn match_graph(g1: &Graph, g2: &Graph) -> Result<Matches> {
    if g1.len() != g2.len() {
        bail_graph!(
            "graphs have different number of fragments ({} vs {})",
            g1.len(),
            g2.len()
        );
    }

    let fps1 = g1.fingerprints()?;
    let fps2 = g2.fingerprints()?;

    // Collect the candidates for each fragment.
    let mut fp_cand = HashMap::with_capacity(g1.len());
    for (&u, &f1) in &fps1 {
        for (&v, &f2) in &fps2 {
            if f1 == f2 {
                fp_cand.entry(u).or_insert_with(HashSet::new).insert(v);
            }
        }
    }

    fn dfs(
        g1: &Graph,
        g2: &Graph,
        fp_cand: &mut HashMap<Id, HashSet<Id>>,
        matches: &mut Matches,
    ) -> Result<()> {
        // If all fragments are matched, return.
        if matches.len() == g1.len() {
            return Ok(());
        }

        // Choose fragment with fewest remaining candidates that's not matched.
        let (&u, u_cands) = fp_cand
            .iter()
            .filter(|(u, _)| !matches.matched(**u))
            .min_by_key(|(_, cands)| cands.len())
            .unwrap();
        let u_cands = u_cands.clone();

        let mut last_error = None;

        for &v in &u_cands {
            // Skip if v is already used.
            if matches.target_matched(v) {
                continue;
            }

            // For each upstream of u, if it's already matched, then it must be matched to the corresponding v's upstream.
            let upstreams = g1.upstreams(u).to_vec();
            for u_upstream in upstreams {
                if let Some(v_upstream) = matches.target(u_upstream) {
                    if !g2.upstreams(v).contains(&v_upstream) {
                        // Not a valid match.
                        continue;
                    }
                }
            }
            // Same for downstream of u.
            let downstreams = g1.downstreams(u).to_vec();
            for u_downstream in downstreams {
                if let Some(v_downstream) = matches.target(u_downstream) {
                    if !g2.downstreams(v).contains(&v_downstream) {
                        // Not a valid match.
                        continue;
                    }
                }
            }

            // Now that `u` and `v` are in the same position of the graph, try to match them by visiting the operators.
            match matches.try_match(&g1.nodes[&u], &g2.nodes[&v]) {
                Ok(()) => {
                    let fp_cand_clone = fp_cand.clone();

                    // v cannot be a candidate for any other u. Remove it before proceeding.
                    for (_, u_cands) in fp_cand.iter_mut() {
                        u_cands.remove(&v);
                    }

                    // Try to match the rest.
                    match dfs(g1, g2, fp_cand, matches) {
                        Ok(()) => return Ok(()), // success, return
                        Err(err) => {
                            last_error = Some(err);

                            // Backtrack.
                            *fp_cand = fp_cand_clone;
                            matches.undo_match(u);
                        }
                    }
                }

                Err(err) => last_error = Some(err),
            }
        }

        if let Some(error) = last_error {
            bail_fragment!(
                source = Box::new(error),
                id = u,
                "tried against all {} candidates, but failed",
                u_cands.len()
            );
        } else {
            bail_fragment!(
                id = u,
                "cannot find a candidate with same topological position"
            )
        }
    }

    let mut matches = Matches::new();
    dfs(g1, g2, &mut fp_cand, &mut matches)?;
    Ok(matches)
}

/// Matches two [`Graph`]s, and returns the internal table mapping from `g1` to `g2`.
pub(crate) fn match_graph_internal_tables(g1: &Graph, g2: &Graph) -> Result<HashMap<u32, u32>> {
    match_graph(g1, g2).map(|matches| matches.into_table_mapping())
}

impl Graph {
    /// Creates a [`Graph`] from a [`StreamFragmentGraph`] that's being built.
    pub(crate) fn from_building(graph: &StreamFragmentGraph) -> Self {
        let nodes = graph
            .fragments
            .iter()
            .map(|(&id, f)| {
                (
                    id.as_global_id(),
                    Fragment {
                        id: id.as_global_id(),
                        root: f.node.clone().unwrap(),
                    },
                )
            })
            .collect();

        let downstreams = graph
            .downstreams
            .iter()
            .map(|(&id, downstreams)| {
                (
                    id.as_global_id(),
                    downstreams
                        .iter()
                        .map(|(&id, _)| id.as_global_id())
                        .collect(),
                )
            })
            .collect();

        let upstreams = graph
            .upstreams
            .iter()
            .map(|(&id, upstreams)| {
                (
                    id.as_global_id(),
                    upstreams.iter().map(|(&id, _)| id.as_global_id()).collect(),
                )
            })
            .collect();

        Self {
            nodes,
            downstreams,
            upstreams,
        }
    }

    /// Creates a [`Graph`] from a [`StreamJobFragments`] that's existing.
    pub(crate) fn from_existing(
        fragments: &StreamJobFragments,
        fragment_upstreams: &HashMap<u32, HashSet<u32>>,
    ) -> Self {
        let nodes: HashMap<_, _> = fragments
            .fragments
            .iter()
            .map(|(&id, f)| {
                (
                    id,
                    Fragment {
                        id,
                        root: f.nodes.clone(),
                    },
                )
            })
            .collect();

        let mut downstreams = HashMap::new();
        let mut upstreams = HashMap::new();

        for (&id, fragment_upstreams) in fragment_upstreams {
            assert!(nodes.contains_key(&id));

            for &upstream in fragment_upstreams {
                if !nodes.contains_key(&upstream) {
                    // Upstream fragment can be from a different job, ignore it.
                    continue;
                }
                downstreams
                    .entry(upstream)
                    .or_insert_with(Vec::new)
                    .push(id);
                upstreams.entry(id).or_insert_with(Vec::new).push(upstream);
            }
        }

        Self {
            nodes,
            downstreams,
            upstreams,
        }
    }
}
