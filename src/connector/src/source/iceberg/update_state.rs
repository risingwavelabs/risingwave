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

//! Durable List/Fetch handoff for Insert/Delete updates. List dispatches a whole phase,
//! then confirms that its Fetch queue is empty at a committed epoch covering the dispatch.

use anyhow::{Result, ensure};
use iceberg::table::Table;
use risingwave_common::catalog::ColumnCatalog;
use serde::{Deserialize, Serialize};

use super::update_planner::{
    IcebergUpdateBinding, IcebergUpdateCursor, IcebergUpdatePhase, IcebergUpdatePlanner,
    IcebergUpdateTask,
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
    /// The graph epoch containing the last page of this phase. An older empty Fetch
    /// snapshot says nothing about completion, since assignments may not yet be visible.
    phase_end_epoch: Option<u64>,
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
            phase_end_epoch: None,
        }
    }

    pub fn validate_columns(&self, job_id: u32, columns: &[ColumnCatalog]) -> Result<()> {
        ensure!(
            self.job_id == job_id,
            "Iceberg List job changed on recovery"
        );
        self.binding.validate_columns(columns)
    }

    pub fn phase_end_epoch(&self) -> Option<u64> {
        self.phase_end_epoch
    }

    /// Build one bounded page and advance the enumeration cursor, without waiting for Fetch.
    /// The boolean marks the last nonempty page. The caller must record its dispatch epoch
    /// before persisting this state. Assignments and the cursor share a graph epoch.
    pub async fn enumerate(
        &mut self,
        table: &Table,
        batch_size: usize,
        planner: &mut Option<IcebergUpdatePlanner>,
    ) -> Result<(Vec<IcebergUpdateAssignment>, bool)> {
        ensure!(batch_size > 0, "Iceberg fetch batch size must be positive");
        ensure!(
            self.phase_end_epoch.is_none(),
            "cannot enumerate until the Iceberg phase is committed"
        );
        let planner =
            planner.get_or_insert_with(|| IcebergUpdatePlanner::new(self.binding.clone()));
        if self.cursor.is_none() {
            self.cursor =
                planner.start_cursor(table, self.bootstrap_complete, self.applied_snapshot)?;
            if self.cursor.is_none() {
                // Binding an empty table is itself a completed empty bootstrap, not permission
                // to choose a different bootstrap on the next poll.
                self.bootstrap_complete = true;
                return Ok((vec![], false));
            }
        }
        let page = planner
            .plan_page(table, self.cursor.as_ref().expect("cursor set"), batch_size)
            .await?;
        if page.tasks.is_empty() {
            ensure!(page.phase_finished, "empty non-final Iceberg page");
            self.advance_phase();
            return Ok((vec![], false));
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Iceberg generation overflow"))?;
        self.cursor.as_mut().expect("cursor set").after_path = Some(
            page.tasks
                .last()
                .expect("nonempty page")
                .id
                .data_file_path
                .clone(),
        );
        let assignments = page
            .tasks
            .into_iter()
            .map(|task| IcebergUpdateAssignment {
                job_id: self.job_id,
                generation: self.generation,
                task,
            })
            .collect();
        Ok((assignments, page.phase_finished))
    }

    /// Called when the final page is emitted, not when its asynchronous planning started.
    pub fn record_phase_end(&mut self, epoch: u64) -> Result<()> {
        ensure!(
            self.cursor.is_some() && self.phase_end_epoch.is_none(),
            "invalid Iceberg phase dispatch"
        );
        self.phase_end_epoch = Some(epoch);
        Ok(())
    }

    /// The caller has observed an empty Fetch table at this committed epoch.
    pub fn complete_phase(&mut self, committed_epoch: u64) -> Result<()> {
        ensure!(
            self.phase_end_epoch
                .is_some_and(|epoch| epoch <= committed_epoch),
            "Iceberg completion snapshot predates phase dispatch"
        );
        self.phase_end_epoch = None;
        self.advance_phase();
        Ok(())
    }

    fn advance_phase(&mut self) {
        let mut cursor = self.cursor.take().expect("enumerated page has a cursor");
        if cursor.phase == IcebergUpdatePhase::Delete {
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
