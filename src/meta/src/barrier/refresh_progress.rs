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

//! Progress tracking for REFRESH MATERIALIZED VIEW operations.
//!
//! This module provides coordination for refresh operations across parallel actors.
//! It ensures that state transitions (ListFinish, LoadFinish) only happen when ALL
//! parallel actors have completed their respective phases.

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::TableId;
use tracing::{debug, info};

use crate::model::ActorId;

/// State of an actor during refresh operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshActorState {
    /// Actor is in list phase
    Listing,
    /// Actor has finished listing
    ListDone,
    /// Actor is in load phase (only for fetch actors)
    Loading,
    /// Actor has finished loading (only for fetch actors)
    LoadDone,
}

/// Progress tracking for a single refresh operation
#[derive(Debug)]
struct RefreshProgress {
    /// Table being refreshed
    table_id: TableId,
    /// Associated source ID
    associated_source_id: TableId,
    /// List actor states
    list_actors: HashMap<ActorId, RefreshActorState>,
    /// Fetch actor states
    fetch_actors: HashMap<ActorId, RefreshActorState>,
    /// Number of list actors that have finished
    list_done_count: usize,
    /// Number of fetch actors that have finished
    load_done_count: usize,
}

impl RefreshProgress {
    fn new(
        table_id: TableId,
        associated_source_id: TableId,
        list_actors: impl IntoIterator<Item = ActorId>,
        fetch_actors: impl IntoIterator<Item = ActorId>,
    ) -> Self {
        let list_actors: HashMap<_, _> = list_actors
            .into_iter()
            .map(|actor| (actor, RefreshActorState::Listing))
            .collect();

        let fetch_actors: HashMap<_, _> = fetch_actors
            .into_iter()
            .map(|actor| (actor, RefreshActorState::Listing))
            .collect();

        info!(
            ?table_id,
            ?associated_source_id,
            list_actor_count = list_actors.len(),
            fetch_actor_count = fetch_actors.len(),
            "Created refresh progress tracker"
        );

        Self {
            table_id,
            associated_source_id,
            list_actors,
            fetch_actors,
            list_done_count: 0,
            load_done_count: 0,
        }
    }

    /// Update list progress for an actor. Returns true if ALL list actors are now done.
    fn update_list_progress(&mut self, actor: ActorId, done: bool) -> bool {
        if let Some(state) = self.list_actors.get_mut(&actor) {
            match state {
                RefreshActorState::Listing if done => {
                    *state = RefreshActorState::ListDone;
                    self.list_done_count += 1;
                    debug!(
                        ?actor,
                        list_done_count = self.list_done_count,
                        total_list_actors = self.list_actors.len(),
                        "List actor finished"
                    );
                }
                RefreshActorState::ListDone => {
                    // Already done, this is a duplicate report (can happen across barriers)
                    debug!(?actor, "Duplicate list done report, ignoring");
                }
                _ => {}
            }
        } else {
            debug!(?actor, "Unknown list actor, ignoring report");
        }

        self.is_list_phase_complete()
    }

    /// Update load progress for an actor. Returns true if ALL fetch actors are now done.
    fn update_load_progress(&mut self, actor: ActorId, done: bool) -> bool {
        if let Some(state) = self.fetch_actors.get_mut(&actor) {
            match state {
                RefreshActorState::Listing | RefreshActorState::Loading if done => {
                    *state = RefreshActorState::LoadDone;
                    self.load_done_count += 1;
                    debug!(
                        ?actor,
                        load_done_count = self.load_done_count,
                        total_fetch_actors = self.fetch_actors.len(),
                        "Load actor finished"
                    );
                }
                RefreshActorState::Listing if !done => {
                    *state = RefreshActorState::Loading;
                }
                RefreshActorState::LoadDone => {
                    // Already done, this is a duplicate report
                    debug!(?actor, "Duplicate load done report, ignoring");
                }
                _ => {}
            }
        } else {
            debug!(?actor, "Unknown fetch actor, ignoring report");
        }

        self.is_load_phase_complete()
    }

    /// Check if all list actors have completed
    fn is_list_phase_complete(&self) -> bool {
        !self.list_actors.is_empty() && self.list_done_count == self.list_actors.len()
    }

    /// Check if all fetch actors have completed
    fn is_load_phase_complete(&self) -> bool {
        !self.fetch_actors.is_empty() && self.load_done_count == self.fetch_actors.len()
    }

    /// Get all actor IDs for cleanup
    fn all_actors(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.list_actors
            .keys()
            .chain(self.fetch_actors.keys())
            .copied()
    }
}

/// Result of updating refresh progress
#[derive(Debug)]
pub enum RefreshProgressUpdateResult {
    /// No action needed
    None,
    /// All list actors finished, should inject ListFinish mutation
    AllListFinished {
        table_id: TableId,
        associated_source_id: TableId,
    },
    /// All load actors finished, should inject LoadFinish mutation
    AllLoadFinished {
        table_id: TableId,
        associated_source_id: TableId,
    },
}

/// Tracker for all ongoing refresh operations
///
/// This tracker ensures that mutations (ListFinish, LoadFinish) are only injected
/// when ALL parallel actors for a given phase have completed their work.
#[derive(Default, Debug)]
pub struct RefreshProgressTracker {
    /// Map from table_id to refresh progress
    progress_map: HashMap<TableId, RefreshProgress>,
    /// Map from actor_id to table_id for quick lookup
    actor_to_table: HashMap<ActorId, TableId>,
}

impl RefreshProgressTracker {
    /// Start tracking a new refresh operation
    pub fn start_refresh(
        &mut self,
        table_id: TableId,
        associated_source_id: TableId,
        list_actors: Vec<ActorId>,
        fetch_actors: Vec<ActorId>,
    ) {
        info!(
            ?table_id,
            ?associated_source_id,
            list_actor_count = list_actors.len(),
            fetch_actor_count = fetch_actors.len(),
            "Starting refresh progress tracking"
        );

        // Register all actors in the actor_to_table map
        for &actor in &list_actors {
            if let Some(old_table) = self.actor_to_table.insert(actor, table_id) {
                tracing::warn!(
                    ?actor,
                    ?old_table,
                    ?table_id,
                    "Actor already tracked for a different table, replacing"
                );
            }
        }
        for &actor in &fetch_actors {
            if let Some(old_table) = self.actor_to_table.insert(actor, table_id) {
                tracing::warn!(
                    ?actor,
                    ?old_table,
                    ?table_id,
                    "Actor already tracked for a different table, replacing"
                );
            }
        }

        let progress =
            RefreshProgress::new(table_id, associated_source_id, list_actors, fetch_actors);

        if let Some(old_progress) = self.progress_map.insert(table_id, progress) {
            tracing::warn!(
                ?table_id,
                "Replacing existing refresh progress, cleaning up old actors"
            );
            // Clean up old actor mappings
            for actor in old_progress.all_actors() {
                self.actor_to_table.remove(&actor);
            }
        }
    }

    /// Update list progress for an actor
    pub fn update_list_progress(&mut self, actor: ActorId) -> RefreshProgressUpdateResult {
        let Some(&table_id) = self.actor_to_table.get(&actor) else {
            debug!(?actor, "List progress report for unknown actor, ignoring");
            return RefreshProgressUpdateResult::None;
        };

        let Some(progress) = self.progress_map.get_mut(&table_id) else {
            debug!(
                ?actor,
                ?table_id,
                "List progress report for table with no progress tracking, ignoring"
            );
            return RefreshProgressUpdateResult::None;
        };

        if progress.update_list_progress(actor, true) {
            info!(
                ?table_id,
                associated_source_id = ?progress.associated_source_id,
                "All list actors finished"
            );
            RefreshProgressUpdateResult::AllListFinished {
                table_id: progress.table_id,
                associated_source_id: progress.associated_source_id,
            }
        } else {
            RefreshProgressUpdateResult::None
        }
    }

    /// Update load progress for an actor
    pub fn update_load_progress(&mut self, actor: ActorId) -> RefreshProgressUpdateResult {
        let Some(&table_id) = self.actor_to_table.get(&actor) else {
            debug!(?actor, "Load progress report for unknown actor, ignoring");
            return RefreshProgressUpdateResult::None;
        };

        let Some(progress) = self.progress_map.get_mut(&table_id) else {
            debug!(
                ?actor,
                ?table_id,
                "Load progress report for table with no progress tracking, ignoring"
            );
            return RefreshProgressUpdateResult::None;
        };

        if progress.update_load_progress(actor, true) {
            info!(
                ?table_id,
                associated_source_id = ?progress.associated_source_id,
                "All load actors finished"
            );
            RefreshProgressUpdateResult::AllLoadFinished {
                table_id: progress.table_id,
                associated_source_id: progress.associated_source_id,
            }
        } else {
            RefreshProgressUpdateResult::None
        }
    }

    /// Finish tracking a refresh operation and clean up
    pub fn finish_refresh(&mut self, table_id: TableId) {
        if let Some(progress) = self.progress_map.remove(&table_id) {
            info!(?table_id, "Finishing refresh progress tracking");
            // Clean up all actor mappings
            for actor in progress.all_actors() {
                self.actor_to_table.remove(&actor);
            }
        } else {
            debug!(?table_id, "Attempted to finish tracking for unknown table");
        }
    }

    /// Get all tables currently being tracked
    pub fn tracked_tables(&self) -> HashSet<TableId> {
        self.progress_map.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_progress_basic() {
        let mut tracker = RefreshProgressTracker::default();
        let table_id = TableId::new(1);
        let source_id = TableId::new(2);

        // Start tracking with 2 list actors and 2 fetch actors
        tracker.start_refresh(table_id, source_id, vec![1, 2], vec![3, 4]);

        // Report first list actor
        let result = tracker.update_list_progress(1);
        assert!(matches!(result, RefreshProgressUpdateResult::None));

        // Report second list actor - should trigger AllListFinished
        let result = tracker.update_list_progress(2);
        assert!(matches!(
            result,
            RefreshProgressUpdateResult::AllListFinished { .. }
        ));

        // Report first fetch actor
        let result = tracker.update_load_progress(3);
        assert!(matches!(result, RefreshProgressUpdateResult::None));

        // Report second fetch actor - should trigger AllLoadFinished
        let result = tracker.update_load_progress(4);
        assert!(matches!(
            result,
            RefreshProgressUpdateResult::AllLoadFinished { .. }
        ));

        // Finish tracking
        tracker.finish_refresh(table_id);
        assert!(tracker.tracked_tables().is_empty());
    }

    #[test]
    fn test_duplicate_reports() {
        let mut tracker = RefreshProgressTracker::default();
        let table_id = TableId::new(1);
        let source_id = TableId::new(2);

        tracker.start_refresh(table_id, source_id, vec![1], vec![2]);

        // Report same actor multiple times
        let result = tracker.update_list_progress(1);
        assert!(matches!(
            result,
            RefreshProgressUpdateResult::AllListFinished { .. }
        ));

        // Duplicate report should be ignored
        let result = tracker.update_list_progress(1);
        assert!(matches!(result, RefreshProgressUpdateResult::None));
    }

    #[test]
    fn test_unknown_actor() {
        let mut tracker = RefreshProgressTracker::default();

        // Report for unknown actor should be ignored
        let result = tracker.update_list_progress(999);
        assert!(matches!(result, RefreshProgressUpdateResult::None));
    }
}
