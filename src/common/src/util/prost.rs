// Copyright 2022 RisingWave Labs
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

use std::collections::btree_map::Entry;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

use risingwave_pb::batch_plan;
use risingwave_pb::monitor_service::StackTraceResponse;
use tracing::warn;

/// Reserved string-map entries carrying the temporary Compactor diagnosis report.
/// This avoids changing the `StackTrace` protobuf for the diagnostic patch.
pub const COMPACTION_DIAGNOSTICS_PREFIX: &str = "__compactor_diagnostics_v1__/";

pub trait TypeUrl {
    fn type_url() -> &'static str;
}

impl TypeUrl for batch_plan::ExchangeNode {
    fn type_url() -> &'static str {
        "type.googleapis.com/plan.ExchangeNode"
    }
}

pub struct StackTraceResponseOutput<'a>(&'a StackTraceResponse);

impl Deref for StackTraceResponseOutput<'_> {
    type Target = StackTraceResponse;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl Display for StackTraceResponseOutput<'_> {
    fn fmt(&self, s: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.node_errors.is_empty() {
            writeln!(s, "--- Stack Trace Errors ---")?;
            for (worker_id, err) in &self.node_errors {
                writeln!(s, ">> Worker {worker_id}")?;
                writeln!(s, "{err}\n")?;
            }
        }
        if !self.actor_traces.is_empty() {
            writeln!(s, "--- Actor Traces ---")?;
            for (actor_id, trace) in &self.actor_traces {
                writeln!(s, ">> Actor {}", *actor_id)?;
                writeln!(s, "{trace}")?;
            }
        }
        if !self.rpc_traces.is_empty() {
            let _ = writeln!(s, "--- RPC Traces ---");
            for (name, trace) in &self.rpc_traces {
                writeln!(s, ">> RPC {name}")?;
                writeln!(s, "{trace}")?;
            }
        }
        let (mut diagnostics, traces): (Vec<_>, Vec<_>) = self
            .compaction_task_traces
            .iter()
            .partition(|(name, _)| name.starts_with(COMPACTION_DIAGNOSTICS_PREFIX));
        if !diagnostics.is_empty() {
            diagnostics.sort_unstable_by_key(|(name, _)| *name);
            writeln!(s, "--- Compactor Diagnostics ---")?;
            for (name, report) in diagnostics {
                writeln!(
                    s,
                    ">> {}",
                    name.strip_prefix(COMPACTION_DIAGNOSTICS_PREFIX)
                        .unwrap_or(name)
                )?;
                writeln!(s, "{report}")?;
            }
        }
        if !traces.is_empty() {
            writeln!(s, "--- Compactor Traces ---")?;
            for (name, trace) in traces {
                writeln!(s, ">> Compaction Task {name}")?;
                writeln!(s, "{trace}")?;
            }
        }

        if !self.inflight_barrier_traces.is_empty() {
            writeln!(s, "--- Inflight Barrier Traces ---")?;
            for (name, trace) in &self.inflight_barrier_traces {
                writeln!(s, ">> Barrier {name}")?;
                writeln!(s, "{trace}")?;
            }
        }

        writeln!(s, "\n\n--- Barrier Worker States ---")?;
        for (worker_id, state) in &self.barrier_worker_state {
            writeln!(s, ">> Worker {worker_id}")?;
            writeln!(s, "{state}\n")?;
        }

        if !self.jvm_stack_traces.is_empty() {
            writeln!(s, "\n\n--- JVM Stack Traces ---")?;
            for (worker_id, state) in &self.jvm_stack_traces {
                writeln!(s, ">> Worker {worker_id}")?;
                writeln!(s, "{state}\n")?;
            }
        }

        if !self.meta_traces.is_empty() {
            writeln!(s, "\n\n--- Meta Traces ---")?;
            for (key, value) in &self.meta_traces {
                writeln!(s, ">> {key}")?;
                writeln!(s, "{value}\n")?;
            }
        }

        Ok(())
    }
}

#[easy_ext::ext(StackTraceResponseExt)]
impl StackTraceResponse {
    pub fn merge_other(&mut self, b: StackTraceResponse) {
        self.actor_traces.extend(b.actor_traces);
        self.rpc_traces.extend(b.rpc_traces);
        self.compaction_task_traces.extend(b.compaction_task_traces);
        self.inflight_barrier_traces
            .extend(b.inflight_barrier_traces);
        for (worker_id, err) in b.node_errors {
            if self.node_errors.contains_key(&worker_id) {
                warn!(
                    worker_id = %worker_id,
                    error = %err,
                    "duplicate node error. skipped"
                );
                continue;
            }
            self.node_errors.insert(worker_id, err);
        }
        for (worker_id, worker_state) in b.barrier_worker_state {
            match self.barrier_worker_state.entry(worker_id) {
                Entry::Occupied(_entry) => {
                    warn!(
                        %worker_id,
                        worker_state, "duplicate barrier worker state. skipped"
                    );
                }
                Entry::Vacant(entry) => {
                    entry.insert(worker_state);
                }
            }
        }
        for (worker_id, worker_state) in b.jvm_stack_traces {
            match self.jvm_stack_traces.entry(worker_id) {
                Entry::Occupied(_entry) => {
                    warn!(
                        %worker_id,
                        worker_state, "duplicate jvm stack trace. skipped"
                    );
                }
                Entry::Vacant(entry) => {
                    entry.insert(worker_state);
                }
            }
        }
        self.meta_traces.extend(b.meta_traces);
    }

    pub fn output(&self) -> StackTraceResponseOutput<'_> {
        StackTraceResponseOutput(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compactor_reports_merge_without_collision_and_render_separately() {
        let mut all = StackTraceResponse::default();
        for id in [1, 2] {
            let mut worker = StackTraceResponse::default();
            worker.compaction_task_traces.insert(
                format!("{COMPACTION_DIAGNOSTICS_PREFIX}worker-{id}"),
                format!("report for worker {id}"),
            );
            worker.compaction_task_traces.insert(
                format!("CompactRunner task={id}"),
                format!("tree for task {id}"),
            );
            all.merge_other(worker);
        }
        let output = all.output().to_string();
        let sections = output.split("--- Compactor Traces ---").collect::<Vec<_>>();
        assert_eq!(sections.len(), 2);
        assert!(sections[0].contains("--- Compactor Diagnostics ---"));
        for id in [1, 2] {
            assert!(sections[0].contains(&format!("report for worker {id}")));
            assert!(!sections[1].contains(&format!("report for worker {id}")));
            assert!(sections[1].contains(&format!("tree for task {id}")));
        }
        assert!(!output.contains(COMPACTION_DIAGNOSTICS_PREFIX));
    }

    #[test]
    fn test_report_without_await_tree_and_legacy_tree_without_report() {
        let mut response = StackTraceResponse::default();
        response.compaction_task_traces.insert(
            format!("{COMPACTION_DIAGNOSTICS_PREFIX}worker-1"),
            "Running tasks: 0".to_owned(),
        );
        let output = response.output().to_string();
        assert!(output.contains("--- Compactor Diagnostics ---"));
        assert!(!output.contains("--- Compactor Traces ---"));
        response.compaction_task_traces.clear();
        response
            .compaction_task_traces
            .insert("CompactRunner".to_owned(), "legacy tree".to_owned());
        let output = response.output().to_string();
        assert!(!output.contains("--- Compactor Diagnostics ---"));
        assert!(output.contains("legacy tree"));
    }
}
