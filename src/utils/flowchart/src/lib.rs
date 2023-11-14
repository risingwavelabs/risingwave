use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write;

use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{Dispatcher, StreamNode};

#[derive(Default)]
struct IdGen {
    next: usize,
}

impl Iterator for IdGen {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.next;
        self.next += 1;
        Some(id)
    }
}

struct Builder {
    _frag_id: u32,

    nodes: BTreeMap<usize, String>,
    edges: Vec<(usize, usize)>,

    root_id: Option<usize>,
    upstream_frags: Vec<u32>,
    merges: BTreeMap<u32, usize>,
}

impl Builder {
    fn new(frag_id: u32) -> Self {
        Self {
            _frag_id: frag_id,
            nodes: Default::default(),
            edges: Default::default(),
            root_id: None,
            upstream_frags: Default::default(),
            merges: Default::default(),
        }
    }

    fn add_node(&mut self, id: usize, body: &NodeBody) -> usize {
        if let NodeBody::Merge(m) = body {
            self.upstream_frags.push(m.upstream_fragment_id);
            self.merges.insert(m.upstream_fragment_id, id);
        }

        let name = body.to_string();
        if self.root_id.is_none() {
            self.root_id = Some(id);
        }
        self.nodes.insert(id, name);
        id
    }

    fn add_edge(&mut self, from: usize, to: usize) {
        self.edges.push((from, to));
    }
}

pub fn generate(fragments: HashMap<u32, Fragment>) -> String {
    let mut id_gen = IdGen::default();
    let mut builders = BTreeMap::new();

    for (&frag_id, fragment) in &fragments {
        let actor = &fragment.actors[0];

        let mut builder = Builder::new(frag_id);

        fn visit(
            node: &StreamNode,
            id_gen: &mut IdGen,
            builder: &mut Builder,
            parent: Option<usize>,
        ) {
            let this = builder.add_node(id_gen.next().unwrap(), &node.node_body.as_ref().unwrap());
            if let Some(parent) = parent {
                builder.add_edge(this, parent);
            }
            for input in &node.input {
                visit(input, id_gen, builder, Some(this));
            }
        }

        visit(
            actor.nodes.as_ref().unwrap(),
            &mut id_gen,
            &mut builder,
            None,
        );

        builders.insert(frag_id, builder);
    }

    let mut inter_edges = BTreeSet::new();

    for (_, builder) in &builders {
        for &upstream_frag_id in &builder.upstream_frags {
            if let Some(upstream_builder) = builders.get(&upstream_frag_id) {
                let root_id = upstream_builder.root_id.unwrap();
                let merge_id = builder.merges[&upstream_frag_id];

                inter_edges.insert((root_id, merge_id));
            }
        }
    }

    let mut f = String::new();
    writeln!(f, "flowchart LR").unwrap();

    for (frag_id, builder) in builders {
        writeln!(f, "subgraph Fragment {frag_id}").unwrap();
        writeln!(f, "direction LR").unwrap();
        for (id, name) in builder.nodes {
            writeln!(f, "o_{id}[{name}]").unwrap();
        }
        for (from, to) in builder.edges {
            writeln!(f, "o_{from} --> o_{to}").unwrap();
        }
        writeln!(f, "end\n").unwrap();
    }

    for (from, to) in inter_edges {
        writeln!(f, "o_{from} --> o_{to}").unwrap();
    }

    f
}

// flowchart LR
//     o3 --> o7
//     o6 --> o8
//     o6 --> o12
//     o11 --> o15
//     o14 --> o16
//     o6 --> o19

//     subgraph Fragment 1
//     direction LR
//     o1[Source]
//     o2[Project]
//     o3[Dispatch]
//     o1 --> o2 --> o3
//     end

//     subgraph Fragment 2
//     direction LR
//     o4[Source]
//     o5[ProjectSet]
//     o6[Dispatch]
//     o4 --> o5 --> o6
//     end

//     subgraph Fragment 3
//     direction LR
//     o7[Merge]
//     o8[Merge]
//     o9[Join]
//     o10[Filter]
//     o11[Dispatch]
//     o7 & o8 --> o9 --> o10 --> o11
//     end

//     subgraph Fragment 4
//     direction LR
//     o12[Merge]
//     o13[Noop]
//     o14[Dispatch]
//     o12 --> o13 --> o14
//     end

//     subgraph Fragment 5
//     direction LR
//     o15[Merge]
//     o16[Merge]
//     o17[Join]
//     o18[Join]
//     o19[Merge]
//     o20[Sink]
//     o15 & o16 --> o17
//     o17 & o19 --> o18
//     o18 --> o20
//     end
