// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{Error, ErrorKind, Result};

use prometheus::core::{Collector, Desc};
use prometheus::{proto, IntCounter, Opts, Registry};

/// Report the existence of a node.
pub fn report_node_process(registry: &Registry) -> Result<()> {
    let pc = NodeCollector::new();
    registry
        .register(Box::new(pc))
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

struct NodeCollector {
    descs: Vec<Desc>,
    // Always set to `1`.
    node_num: IntCounter,
}

impl NodeCollector {
    pub fn new() -> Self {
        let node_num = IntCounter::with_opts(Opts::new("node_num", "The number of node.")).unwrap();
        node_num.inc();
        Self {
            descs: node_num.desc().into_iter().cloned().collect(),
            node_num,
        }
    }
}

impl Collector for NodeCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.node_num.collect()
    }
}
