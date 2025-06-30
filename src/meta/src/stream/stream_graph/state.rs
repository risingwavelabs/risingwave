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

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash as _, Hasher as _};

use itertools::Itertools;
use risingwave_common::catalog::{self, TableDesc};
use risingwave_common::util::stream_graph_visitor::visit_stream_node_tables;
use risingwave_pb::catalog::PbTable;
use risingwave_pb::stream_plan::PbStreamNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use strum::IntoDiscriminant;

/// Helper type for describing a [`StreamNode`] in error messages.
#[derive(thiserror::Error, thiserror_ext::Macro, Debug)]
pub enum Error {
    #[error("invalid graph: {0}")]
    InvalidGraph(#[message] String),
    #[error("failed to match: {0}")]
    FailedToMatch(#[message] String),
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Operator id.
type Id = u64;

struct Node {
    id: Id,
    body: NodeBody,
}

pub struct Graph {
    nodes: HashMap<Id, Node>,
    downstreams: HashMap<Id, Vec<Id>>,
    upstreams: HashMap<Id, Vec<Id>>,
}

impl Graph {
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn topo_order(&self) -> Result<Vec<Id>> {
        let mut topo = Vec::new();
        let mut downstream_cnts = HashMap::new();

        // Iterate all nodes to find the root and initialize the downstream counts.
        for node_id in self.nodes.keys() {
            let downstream_cnt = self.downstreams.get(node_id).unwrap().len();
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
            for &upstream_id in self.upstreams.get(&node_id).unwrap() {
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
            bail_invalid_graph!("graph is not a DAG");
        }

        Ok(topo)
    }

    fn fingerprints(&self) -> Result<HashMap<Id, u64>> {
        let mut fps = HashMap::new();

        let order = self.topo_order()?;
        for node_id in order.into_iter().rev() {
            let node = &self.nodes[&node_id];
            let upstream_fps = self.upstreams[&node_id]
                .iter()
                .map(|id| *fps.get(id).unwrap())
                .sorted() // ignore order
                .collect_vec();

            let mut hasher = DefaultHasher::new();
            (
                node.body.discriminant(),
                self.upstreams[&node_id].len(),
                self.downstreams[&node_id].len(),
                upstream_fps,
            )
                .hash(&mut hasher);
            let fingerprint = hasher.finish();

            fps.insert(node_id, fingerprint);
        }

        Ok(fps)
    }
}

struct Match {
    target: Id,
    table_matches: HashMap<u32, u32>,
}

struct Matches {
    inner: HashMap<Id, Match>,
    matched_targets: HashSet<Id>,
}

impl Matches {
    fn new() -> Self {
        Self {
            inner: HashMap::new(),
            matched_targets: HashSet::new(),
        }
    }

    fn target(&self, u: Id) -> Option<Id> {
        self.inner.get(&u).map(|m| m.target)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn mapped(&self, u: Id) -> bool {
        self.inner.contains_key(&u)
    }

    fn target_used(&self, v: Id) -> bool {
        self.matched_targets.contains(&v)
    }

    fn try_match(&mut self, u: &Node, v: &Node) -> Result<()> {
        if self.mapped(u.id) {
            panic!("node {} was already mapped", u.id);
        }

        let collect_tables = |x: &Node| {
            let mut dummy = PbStreamNode {
                node_body: Some(x.body.clone()),
                ..Default::default()
            };

            let mut tables = Vec::new();
            visit_stream_node_tables(&mut dummy, |table, name| {
                tables.push((name.to_owned(), table.clone()));
            });
            tables
        };

        let u_tables = collect_tables(u);
        let mut v_tables = collect_tables(v);

        let mut table_matches = HashMap::new();

        for (ut_name, ut) in u_tables {
            let vt_cands = v_tables
                .extract_if(.., |(vt_name, _)| &*vt_name == &ut_name)
                .collect_vec();

            if vt_cands.len() == 0 {
                bail_failed_to_match!("cannot find a match for table {} in node {}", ut_name, u.id);
            } else if vt_cands.len() > 1 {
                bail_failed_to_match!(
                    "found multiple matches for table {} in node {}",
                    ut_name,
                    u.id
                );
            }

            let (_, vt) = vt_cands.into_iter().next().unwrap();

            let table_desc_for_compare = |table: &PbTable| {
                let mut desc = TableDesc::from_pb_table(table);
                desc.table_id = catalog::TableId::placeholder();
                desc
            };

            let ut_compare = table_desc_for_compare(&ut);
            let vt_compare = table_desc_for_compare(&vt);

            if ut_compare != vt_compare {
                bail_failed_to_match!(
                    "table {} in node {} cannot be matched, diff:\n{}",
                    ut_name,
                    u.id,
                    pretty_assertions::Comparison::new(&ut_compare, &vt_compare)
                );
            }

            table_matches
                .try_insert(ut.id, vt.id)
                .unwrap_or_else(|_| panic!("duplicated table id {} in node {}", ut.id, u.id));
        }

        let m = Match {
            target: v.id,
            table_matches,
        };
        self.inner.insert(u.id, m);
        self.matched_targets.insert(v.id);

        Ok(())
    }

    fn undo_match(&mut self, u: Id) {
        let target = self
            .inner
            .remove(&u)
            .unwrap_or_else(|| panic!("node {} was not mapped", u))
            .target;

        let target_removed = self.matched_targets.remove(&target);
        assert!(target_removed);
    }

    fn into_table_mapping(self) -> HashMap<u32, u32> {
        self.inner
            .into_iter()
            .flat_map(|(_, m)| m.table_matches.into_iter())
            .collect()
    }
}

fn match_graph(g1: &Graph, g2: &Graph) -> Result<Matches> {
    if g1.len() != g2.len() {
        bail_failed_to_match!("graphs have different number of nodes");
    }

    let fps1 = g1.fingerprints()?;
    let fps2 = g2.fingerprints()?;

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
        if matches.len() == g1.len() {
            // We are done.
            return Ok(());
        }

        // Choose node with fewest remaining candidates that's unmapped.
        let (&u, u_cands) = fp_cand
            .iter()
            .filter(|(u, _)| !matches.mapped(**u))
            .min_by_key(|(_, cands)| cands.len())
            .unwrap();
        let u_cands = u_cands.clone();

        for v in u_cands {
            // Skip if v is already used.
            if matches.target_used(v) {
                continue;
            }

            // For each upstream of u, if it's already matched, then it must be matched to the corresponding v's upstream.
            let upstreams = g1.upstreams[&u].clone();
            for u_upstream in upstreams {
                if let Some(v_upstream) = matches.target(u_upstream) {
                    if !g2.upstreams[&v].contains(&v_upstream) {
                        // Not a valid match.
                        continue;
                    }
                }
            }
            // Same for downstream of u.
            let downstreams = g1.downstreams[&u].clone();
            for u_downstream in downstreams {
                if let Some(v_downstream) = matches.target(u_downstream) {
                    if !g2.downstreams[&v].contains(&v_downstream) {
                        // Not a valid match.
                        continue;
                    }
                }
            }

            match matches.try_match(&g1.nodes[&u], &g2.nodes[&v]) {
                Ok(_) => {
                    let fp_cand_clone = fp_cand.clone();

                    // v cannot be a candidate for any other u. Remove it.
                    for (_, u_cands) in fp_cand.iter_mut() {
                        u_cands.remove(&v);
                    }

                    // Try to match the rest.
                    match dfs(g1, g2, fp_cand, matches) {
                        Ok(_) => return Ok(()),
                        Err(_err) => {} // TODO: record error
                    }

                    // Backtrack.
                    *fp_cand = fp_cand_clone;
                    matches.undo_match(u);
                }

                Err(_err) => {} // TODO: record error
            }
        }

        bail_failed_to_match!("no valid match found for node {u} ({})", g1.nodes[&u].body);
    }

    let mut matches = Matches::new();
    dfs(g1, g2, &mut fp_cand, &mut matches)?;
    Ok(matches)
}

// TODO: make it `pub(super)`
pub fn match_graph_internal_tables(g1: &Graph, g2: &Graph) -> Result<HashMap<u32, u32>> {
    match_graph(g1, g2).map(|matches| matches.into_table_mapping())
}
