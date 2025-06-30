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

use std::collections::HashMap;
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
enum Error {
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

struct Graph {
    nodes: HashMap<Id, Node>,
    downstreams: HashMap<Id, Vec<Id>>,
    upstreams: HashMap<Id, Vec<Id>>,
}

impl Graph {
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

struct Mapping {
    inner: HashMap<Id, Match>,
}

impl Mapping {
    fn target(&self, u: Id) -> Option<Id> {
        self.inner.get(&u).map(|m| m.target)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn mapped(&self, u: Id) -> bool {
        self.inner.contains_key(&u)
    }

    fn try_add(&mut self, u: &Node, v: &Node) -> Result<()> {
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

        Ok(())
    }

    fn remove(&mut self, u: Id) {
        self.inner
            .remove(&u)
            .unwrap_or_else(|| panic!("node {} was not mapped", u));
    }
}
