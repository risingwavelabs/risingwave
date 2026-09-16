// Copyright 2026 RisingWave Labs
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
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_pb::id::PartialGraphId;

/// Recovery terms of the partial graphs whose sink writers may currently be admitted by the sink
/// coordinator.
///
/// A term identifies one incarnation of a partial graph. Recovery drops the terms of the graphs it
/// resets before it stops the sink coordinators and reconciles `pending_sink_state`, and issues new
/// terms only afterwards. A writer of a previous incarnation can therefore never be admitted again,
/// so it cannot persist sink state on top of the epochs that recovery has aborted, and a sink
/// coordinator always loads its state after recovery has reconciled it.
#[derive(Clone, Default)]
pub struct SinkWriterTerms {
    inner: Arc<RwLock<HashMap<PartialGraphId, HashSet<String>>>>,
}

impl SinkWriterTerms {
    /// Admit the writers of a newly created partial graph incarnation.
    pub fn register(&self, partial_graph_id: PartialGraphId, term_id: &str) {
        self.inner
            .write()
            .entry(partial_graph_id)
            .or_default()
            .insert(term_id.to_owned());
    }

    /// Stop admitting the writers of partial graphs that are being reset. Their actors are dropped
    /// by the compute nodes and must not come back.
    pub fn unregister(&self, partial_graph_ids: impl IntoIterator<Item = PartialGraphId>) {
        let mut inner = self.inner.write();
        for partial_graph_id in partial_graph_ids {
            inner.remove(&partial_graph_id);
        }
    }

    /// Keep the terms of a finished partial graph alive under `into`. Actors that were built
    /// under the finished graph keep presenting its term until `into` is reset.
    pub fn merge(&self, from: PartialGraphId, into: PartialGraphId) {
        if from == into {
            return;
        }
        let mut inner = self.inner.write();
        if let Some(terms) = inner.remove(&from) {
            inner.entry(into).or_default().extend(terms);
        }
    }

    /// Stop admitting the writers of every partial graph matching `predicate`.
    pub fn unregister_if(&self, predicate: impl Fn(PartialGraphId) -> bool) {
        self.inner
            .write()
            .retain(|partial_graph_id, _| !predicate(*partial_graph_id));
    }

    pub fn clear(&self) {
        self.inner.write().clear();
    }

    pub fn is_active(&self, term_id: &str) -> bool {
        self.inner
            .read()
            .values()
            .any(|terms| terms.contains(term_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_writer_terms() {
        let terms = SinkWriterTerms::default();
        let database = PartialGraphId::new(1 << 32 | u32::MAX as u64);
        let creating_job = PartialGraphId::new(1 << 32 | 7);
        assert!(!terms.is_active("db-1"));

        terms.register(database, "db-1");
        terms.register(creating_job, "job-1");
        assert!(terms.is_active("db-1"));
        assert!(terms.is_active("job-1"));

        // The finished creating job hands its term over to the database graph.
        terms.merge(creating_job, database);
        assert!(terms.is_active("job-1"));

        // Resetting the database graph fences every previous incarnation.
        terms.unregister([database]);
        assert!(!terms.is_active("db-1"));
        assert!(!terms.is_active("job-1"));

        terms.register(database, "db-2");
        terms.register(creating_job, "job-2");
        assert!(terms.is_active("db-2"));
        terms.unregister_if(|partial_graph_id| partial_graph_id == creating_job);
        assert!(terms.is_active("db-2"));
        assert!(!terms.is_active("job-2"));
        terms.clear();
        assert!(!terms.is_active("db-2"));
    }
}
