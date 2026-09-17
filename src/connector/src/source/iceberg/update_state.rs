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

//! Durable List/Fetch handoff for Insert/Delete updates. Completion rows must be read from a
//! globally committed checkpoint. Local flushes, queue emptiness and actor reports are not a fence.

use anyhow::{Result, ensure};
use iceberg::table::Table;
use risingwave_common::catalog::ColumnCatalog;
use serde::{Deserialize, Serialize};

use super::update_planner::{
    IcebergUpdateBinding, IcebergUpdateCursor, IcebergUpdateLimits, IcebergUpdatePage,
    IcebergUpdatePhase, IcebergUpdatePlanner, IcebergUpdateTask,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdateAssignment {
    /// The dedicated Fetch state table ID. Distinct ingestion jobs never share this table.
    pub job_id: u32,
    pub generation: u64,
    pub task: IcebergUpdateTask,
}

impl IcebergUpdateAssignment {
    pub fn key(&self) -> Result<String> {
        Ok(serde_json::to_string(&(
            self.job_id,
            self.generation,
            &self.task.id,
        ))?)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdateFetchState {
    pub assignment: IcebergUpdateAssignment,
    pub next_position: u64,
    pub finished: bool,
}

impl IcebergUpdateFetchState {
    pub fn new(assignment: IcebergUpdateAssignment) -> Self {
        Self {
            assignment,
            next_position: 0,
            finished: false,
        }
    }

    pub fn advance(&mut self, next_position: u64, finished: bool) -> Result<()> {
        ensure!(
            !self.finished
                && next_position >= self.next_position
                && next_position <= self.assignment.task.record_count(),
            "invalid Iceberg fetch cursor advance"
        );
        ensure!(
            !finished || next_position == self.assignment.task.record_count(),
            "Iceberg task completion before EOF"
        );
        self.next_position = next_position;
        self.finished = finished;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdateListState {
    pub job_id: u32,
    binding: IcebergUpdateBinding,
    bootstrap_complete: bool,
    applied_snapshot: Option<i64>,
    cursor: Option<IcebergUpdateCursor>,
    generation: u64,
    pending: Option<IcebergUpdatePage>,
}

impl IcebergUpdateListState {
    pub fn new(job_id: u32, binding: IcebergUpdateBinding) -> Self {
        Self {
            job_id,
            binding,
            bootstrap_complete: false,
            applied_snapshot: None,
            cursor: None,
            generation: 0,
            pending: None,
        }
    }

    pub fn validate_columns(&self, job_id: u32, columns: &[ColumnCatalog]) -> Result<()> {
        ensure!(
            self.job_id == job_id,
            "Iceberg List job changed on recovery"
        );
        self.binding.validate_columns(columns)
    }

    pub fn assignments(&self) -> Vec<IcebergUpdateAssignment> {
        self.pending
            .as_ref()
            .map(|pending| {
                pending
                    .tasks
                    .iter()
                    .map(|task| IcebergUpdateAssignment {
                        job_id: self.job_id,
                        generation: self.generation,
                        task: task.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Build one bounded page. Pending work is never re-enumerated against latest metadata.
    /// Returned assignments and this state must be checkpointed in the same graph epoch.
    pub async fn enumerate(
        &mut self,
        table: &Table,
        limits: IcebergUpdateLimits,
    ) -> Result<Vec<IcebergUpdateAssignment>> {
        ensure!(
            self.pending.is_none(),
            "cannot enumerate while Iceberg tasks are outstanding"
        );
        let planner = IcebergUpdatePlanner::new(self.binding.clone()).with_limits(limits)?;
        if self.cursor.is_none() {
            self.cursor =
                planner.start_cursor(table, self.bootstrap_complete, self.applied_snapshot)?;
            if self.cursor.is_none() {
                // Binding an empty table is itself a completed empty bootstrap, not permission
                // to choose a different bootstrap on the next poll.
                self.bootstrap_complete = true;
                return Ok(vec![]);
            }
        }
        let page = planner
            .plan_page(table, self.cursor.as_ref().expect("cursor set"))
            .await?;
        if page.tasks.is_empty() {
            ensure!(page.phase_finished, "empty non-final Iceberg page");
            self.finish_page(page);
            return Ok(vec![]);
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Iceberg generation overflow"))?;
        self.pending = Some(page);
        Ok(self.assignments())
    }

    /// All states must come from one globally committed epoch. Missing or stale rows do not
    /// complete a page. Duplicate notifications cannot advance a second generation.
    pub fn acknowledge_committed(&mut self, states: &[IcebergUpdateFetchState]) -> Result<bool> {
        let Some(page) = &self.pending else {
            return Ok(false);
        };
        for task in &page.tasks {
            let mut matching = states.iter().filter(|state| {
                state.assignment.job_id == self.job_id
                    && state.assignment.generation == self.generation
                    && state.assignment.task == *task
            });
            let Some(state) = matching.next() else {
                return Ok(false);
            };
            if matching.next().is_some() || !state.finished {
                return Ok(false);
            }
            ensure!(
                state.next_position == task.record_count(),
                "invalid committed task EOF"
            );
        }
        let pending = self.pending.take().expect("outstanding assignments");
        self.finish_page(pending);
        Ok(true)
    }

    fn finish_page(&mut self, page: IcebergUpdatePage) {
        let mut cursor = self.cursor.take().expect("enumerated page has a cursor");
        if !page.phase_finished {
            cursor.after_path = Some(
                page.tasks
                    .last()
                    .expect("non-final page is nonempty")
                    .id
                    .data_file_path
                    .clone(),
            );
            self.cursor = Some(cursor);
        } else if cursor.phase == IcebergUpdatePhase::Delete {
            cursor.phase = IcebergUpdatePhase::Insert;
            cursor.after_path = None;
            self.cursor = Some(cursor);
        } else {
            self.applied_snapshot = Some(cursor.snapshot_id);
            self.bootstrap_complete = true;
            self.cursor = None;
        }
    }
}
