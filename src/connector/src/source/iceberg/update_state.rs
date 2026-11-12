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

//! Durable List/Fetch handoff for Insert/Delete updates.
//!
//! Every task has a Fetch route and a dispatch sequence number. The Fetch state table is
//! distributed by route and ordered by sequence number within a route, so one Fetch actor applies
//! the tasks of a route in dispatch order, also across rescaling. A phase that fits in one small
//! page goes to the shared ordered route and needs no completion fence: later ordered phases queue
//! behind it. Other phases fan out across routes. Before a fan-out page, and after the last page
//! of a fan-out phase, List confirms that the Fetch queue is empty at a committed epoch covering
//! the earlier dispatch.

use anyhow::Context;
use iceberg::table::Table;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::ensure;
use serde::{Deserialize, Serialize};

use super::update_planner::{
    IcebergUpdateBinding, IcebergUpdateCursor, IcebergUpdatePhase, IcebergUpdatePlanner,
    IcebergUpdateTask,
};
use crate::error::ConnectorResult;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdateAssignment {
    /// The dedicated Fetch state table ID. Distinct ingestion jobs never share this table.
    pub job_id: u32,
    /// The Fetch state distribution key.
    pub route: i64,
    /// Unique within the job and increasing in dispatch order. Orders tasks within a route.
    pub seq: i64,
    pub task: IcebergUpdateTask,
}

impl IcebergUpdateAssignment {
    /// Shared by all ordered phases of a job. Fan-out tasks use their nonnegative `seq`.
    pub const ORDERED_ROUTE: i64 = -1;
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

    pub fn advance(&mut self, next_position: u64, finished: bool) -> ConnectorResult<()> {
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

/// Which outstanding tasks a dispatch must wait for, recorded at the epoch that emits a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergUpdateFenceKind {
    /// A complete phase on the ordered route. Only fan-out dispatch waits for it.
    Ordered,
    /// The last page of a fan-out phase. All later dispatch waits for it.
    FanOut,
}

/// Outstanding tasks, confirmed by an empty committed Fetch queue at `epoch` or later. The
/// queue never holds ordered and fan-out tasks at once, so emptiness covers both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergUpdateFence {
    /// The graph epoch of the last covered dispatch. An older empty Fetch snapshot says
    /// nothing about completion, since assignments may not yet be visible.
    epoch: u64,
    /// Only ordered tasks are outstanding and no fan-out page waits for them, so ordered
    /// dispatch may continue behind them.
    ordered: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergUpdateListState {
    pub job_id: u32,
    binding: IcebergUpdateBinding,
    bootstrap_complete: bool,
    /// The target of the last range whose tasks are all dispatched, and the base of the next.
    dispatched_snapshot: Option<i64>,
    cursor: Option<IcebergUpdateCursor>,
    next_seq: i64,
    fence: Option<IcebergUpdateFence>,
}

impl IcebergUpdateListState {
    pub fn new(job_id: u32, binding: IcebergUpdateBinding) -> Self {
        Self {
            job_id,
            binding,
            bootstrap_complete: false,
            dispatched_snapshot: None,
            cursor: None,
            next_seq: 0,
            fence: None,
        }
    }

    pub fn validate_columns(&self, job_id: u32, columns: &[ColumnCatalog]) -> ConnectorResult<()> {
        ensure!(
            self.job_id == job_id,
            "Iceberg List job changed on recovery"
        );
        self.binding.validate_columns(columns)
    }

    /// The epoch whose committed Fetch queue must be empty before List dispatches again.
    pub fn wait_epoch(&self) -> Option<u64> {
        self.fence
            .filter(|fence| !fence.ordered)
            .map(|fence| fence.epoch)
    }

    /// Build one bounded page and advance the enumeration cursor, without waiting for Fetch.
    /// A phase that fits in its first page with at most `ordered_max_tasks` tasks goes to the
    /// ordered route. The caller must record the returned fence at the epoch emitting the page
    /// before persisting this state. Assignments and the cursor share a graph epoch.
    pub async fn enumerate(
        &mut self,
        table: &Table,
        batch_size: usize,
        ordered_max_tasks: usize,
        planner: &mut Option<IcebergUpdatePlanner>,
    ) -> ConnectorResult<(Vec<IcebergUpdateAssignment>, Option<IcebergUpdateFenceKind>)> {
        ensure!(batch_size > 0, "Iceberg fetch batch size must be positive");
        ensure!(
            self.wait_epoch().is_none(),
            "cannot enumerate until dispatched Iceberg tasks are applied"
        );
        let planner =
            planner.get_or_insert_with(|| IcebergUpdatePlanner::new(self.binding.clone()));
        if self.cursor.is_none() {
            self.cursor =
                planner.start_cursor(table, self.bootstrap_complete, self.dispatched_snapshot)?;
            if self.cursor.is_none() {
                // Binding an empty table is itself a completed empty bootstrap, not permission
                // to choose a different bootstrap on the next poll.
                self.bootstrap_complete = true;
                return Ok((vec![], None));
            }
        }
        let cursor = self.cursor.clone().expect("cursor set");
        let page = planner.plan_page(table, &cursor, batch_size).await?;
        if page.tasks.is_empty() {
            ensure!(page.phase_finished, "empty non-final Iceberg page");
            self.advance_phase();
            return Ok((vec![], None));
        }
        // The route of a phase is chosen before its first dispatch, so only a phase that fits
        // in its first page can be ordered.
        let ordered = cursor.after_path.is_none()
            && page.phase_finished
            && page.tasks.len() <= ordered_max_tasks;
        if !ordered && let Some(fence) = &mut self.fence {
            // Fan-out tasks on other routes could overtake the outstanding ordered tasks. Keep
            // the page for the next poll and wait until the ordered route drains.
            planner.unread(cursor, page.tasks);
            fence.ordered = false;
            return Ok((vec![], None));
        }
        let first_seq = self.next_seq;
        self.next_seq = i64::try_from(page.tasks.len())
            .ok()
            .and_then(|len| first_seq.checked_add(len))
            .context("Iceberg task sequence overflow")?;
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
            .enumerate()
            .map(|(offset, task)| (task, first_seq + offset as i64))
            .map(|(task, seq)| IcebergUpdateAssignment {
                job_id: self.job_id,
                route: if ordered {
                    IcebergUpdateAssignment::ORDERED_ROUTE
                } else {
                    seq
                },
                seq,
                task,
            })
            .collect();
        let fence = if ordered {
            Some(IcebergUpdateFenceKind::Ordered)
        } else {
            page.phase_finished
                .then_some(IcebergUpdateFenceKind::FanOut)
        };
        if page.phase_finished {
            self.advance_phase();
        }
        Ok((assignments, fence))
    }

    /// Called when the page is emitted, not when its asynchronous planning started.
    pub fn record_fence(
        &mut self,
        kind: IcebergUpdateFenceKind,
        epoch: u64,
    ) -> ConnectorResult<()> {
        let ordered = kind == IcebergUpdateFenceKind::Ordered;
        ensure!(
            match self.fence {
                None => true,
                Some(fence) => ordered && fence.ordered && fence.epoch <= epoch,
            },
            "invalid Iceberg dispatch fence"
        );
        self.fence = Some(IcebergUpdateFence { epoch, ordered });
        Ok(())
    }

    /// The caller has observed an empty Fetch table at this committed epoch.
    pub fn complete_fence(&mut self, committed_epoch: u64) -> ConnectorResult<()> {
        ensure!(
            self.wait_epoch()
                .is_some_and(|epoch| epoch <= committed_epoch),
            "Iceberg completion snapshot predates dispatch"
        );
        self.fence = None;
        Ok(())
    }

    fn advance_phase(&mut self) {
        let mut cursor = self.cursor.take().expect("enumerated page has a cursor");
        if cursor.phase == IcebergUpdatePhase::Delete {
            cursor.phase = IcebergUpdatePhase::Insert;
            cursor.after_path = None;
            self.cursor = Some(cursor);
        } else {
            self.dispatched_snapshot = Some(cursor.snapshot_id);
            self.bootstrap_complete = true;
            self.cursor = None;
        }
    }
}
