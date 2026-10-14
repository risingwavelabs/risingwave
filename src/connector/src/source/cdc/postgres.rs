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

use std::collections::BTreeMap;

use anyhow::Context;
use risingwave_common::bail;
use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::cdc::external::DebeziumOffset;
use crate::source::cdc::external::postgres::PostgresOffset;
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait, DebeziumCdcSplit};
use crate::source::cdc::{CdcSerializedFieldNames, CdcSourceFrontendValidator, Postgres};

// ------- Persisted names -------

impl CdcSerializedFieldNames for Postgres {
    const DEBEZIUM_CDC_SPLIT_SPLIT_FIELD: &'static str = "pg_split";
}

// ------- Frontend validation -------

impl CdcSourceFrontendValidator for Postgres {
    const ALLOW_ALTER_ON_FLY_FIELDS: &'static [&'static str] = &[
        "cdc.source.wait.streaming.start.timeout",
        "debezium.max.queue.size",
        "debezium.queue.memory.ratio",
        "debezium.heartbeat.interval.ms",
        "hostname",
        "port",
        "password",
    ];
    const COMPATIBLE_FORMATS: &'static [(PbFormatType, &'static [PbEncodeType])] = &[
        (PbFormatType::Debezium, &[PbEncodeType::Json]),
        // support source stream job
        (PbFormatType::Plain, &[PbEncodeType::Json]),
    ];

    fn fill_and_validate_props(props: &mut BTreeMap<String, String>) -> ConnectorResult<()> {
        fill_and_validate_postgres_props(props)
    }
}

/// Shared by Postgres and Citus.
pub(crate) fn fill_and_validate_postgres_props(
    props: &mut BTreeMap<String, String>,
) -> ConnectorResult<()> {
    match props.get("slot.name") {
        None => {
            // Build a random slot name with UUID
            // e.g. "rw_cdc_f9a3567e6dd54bf5900444c8b1c03815"
            let uuid = uuid::Uuid::new_v4();
            props.insert("slot.name".into(), format!("rw_cdc_{}", uuid.simple()));
        }
        Some(slot_name) => {
            // please refer to
            // - https://github.com/debezium/debezium/blob/97956ce25b7612e3413d363658661896b7d2e0a2/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresConnectorConfig.java#L1179
            // - https://doxygen.postgresql.org/slot_8c.html#afac399f07320b9adfd2c599cf822aaa3
            if !slot_name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                || slot_name.len() > 63
            {
                bail!(
                    "Invalid replication slot name: {:?}. Valid replication slot name must contain only digits, lowercase characters and underscores with length <= 63",
                    slot_name
                );
            }
        }
    }

    if !props.contains_key("schema.name") {
        // Default schema name is "public"
        props.insert("schema.name".into(), "public".into());
    }
    if !props.contains_key("publication.name") {
        // Build a random publication name with UUID to avoid conflicts between sources
        // e.g. "rw_publication_f9a3567e6dd54bf5900444c8b1c03815"
        let uuid = uuid::Uuid::new_v4();
        props.insert(
            "publication.name".into(),
            format!("rw_publication_{}", uuid.simple()),
        );
    }
    if !props.contains_key("publication.create.enable") {
        // Default auto create publication if doesn't exist
        props.insert("publication.create.enable".into(), "true".into());
    }
    Ok(())
}

// ------- Split -------

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PostgresCdcSplit {
    pub inner: CdcSplitBase,
}

impl CdcSplitTrait for PostgresCdcSplit {
    fn inner(&self) -> &CdcSplitBase {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut CdcSplitBase {
        &mut self.inner
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        let new_snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;

        // Monotonicity guard for streaming-phase Postgres CDC offsets.
        //
        // Debezium's task-level auto-restart (BaseSourceTask) reloads
        // effectiveOffset from ConfigurableOffsetBackingStore, which lags
        // dbz's in-memory position by one barrier + async commit cycle.
        // After a broken pipe, dbz re-emits older WAL events whose LSNs
        // are smaller than the freshest entry we already persisted.
        // Without this guard those stale LSNs would overwrite the split
        // state, the store would then be pushed backwards via the next
        // barrier, and a subsequent restart would load the stale value.
        //
        // We guard both `lsn_commit` and `lsn_proc` because dbz consumes
        // them for two different things on resume:
        //   - `lsn_commit` is what `validateLogPosition` checks against
        //     PG slot.restart_lsn; regression here causes a fatal
        //     `DebeziumException` ("change stream ... no longer available").
        //   - `lsn_proc` is the start position for `startStreaming` and
        //     the key `WalPositionLocator` uses to deduplicate replayed
        //     events on restart; regression here can lead to silent
        //     data loss / a hung resume if PG has already advanced past
        //     the rewound value.
        //
        // The per-message `lsn` field is intentionally NOT used as a
        // monotonicity key: under concurrent overlapping transactions
        // PG streams events in commit order, so the per-event physical
        // lsn can legitimately move backwards when a long-running
        // earlier transaction commits later than a shorter newer one.
        // Only `lsn_commit` and `lsn_proc` are monotonic under correct
        // dbz behavior.
        //
        // If both offsets are parseable as streaming Postgres offsets, use
        // the same `(lsn_commit, lsn_proc)` ordering as snapshot backfill
        // deduplication. Snapshot / heartbeat / incomplete offsets are not
        // comparable and pass through.
        if let (Some(old_offset), Some(new_offset)) = (
            self.inner
                .start_offset
                .as_deref()
                .and_then(parse_postgres_offset_from_offset_str),
            parse_postgres_offset_from_offset_str(&last_seen_offset),
        ) && new_offset < old_offset
        {
            tracing::warn!(
                split_id = self.inner.split_id,
                ?old_offset,
                ?new_offset,
                "Rejecting backward Postgres CDC offset update; \
                 keeping current state to prevent state-table regression."
            );
            return Ok(());
        }

        self.inner.snapshot_done = new_snapshot_done;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }

    fn extract_snapshot_flag(&self, start_offset: &str) -> ConnectorResult<bool> {
        // if snapshot_done is already true, it won't be changed
        let mut snapshot_done = self.is_snapshot_done();
        if snapshot_done {
            return Ok(snapshot_done);
        }

        let dbz_offset: DebeziumOffset = serde_json::from_str(start_offset).with_context(|| {
            format!(
                "invalid postgres offset: {}, split: {}",
                start_offset, self.inner.split_id
            )
        })?;

        // heartbeat event should not update the `snapshot_done` flag
        if !dbz_offset.is_heartbeat {
            snapshot_done = dbz_offset
                .source_offset
                .last_snapshot_record
                .unwrap_or(false);
        }
        Ok(snapshot_done)
    }
}

impl PostgresCdcSplit {
    /// Extract PostgreSQL LSN value from the offset JSON string.
    ///
    /// This function parses the offset JSON and extracts the LSN value from the sourceOffset.lsn field.
    /// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
    pub fn pg_lsn(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
        let source_offset = offset.get("sourceOffset")?;
        let lsn = source_offset.get("lsn")?;
        lsn.as_u64()
    }

    /// Extract PostgreSQL `lsn_commit` from the current offset (last commit position).
    pub fn pg_lsn_commit(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_postgres_lsn_commit_from_offset_str(offset_str)
    }

    /// Extract PostgreSQL `lsn_proc` from the current offset (last completely processed position).
    pub fn pg_lsn_proc(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_postgres_lsn_proc_from_offset_str(offset_str)
    }
}

impl DebeziumCdcSplit<Postgres> {
    /// Extract PostgreSQL LSN value from the current split offset.
    ///
    /// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
    pub fn pg_lsn(&self) -> Option<u64> {
        self.postgres_split.as_ref()?.pg_lsn()
    }
}

/// Extract PostgreSQL LSN value from a CDC offset JSON string.
///
/// This is a standalone helper function that can be used when you only have the offset string
/// (e.g., in callbacks) and don't have access to the Split object.
///
/// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
pub fn extract_postgres_lsn_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn")?;
    lsn.as_u64()
}

fn parse_postgres_offset_from_offset_str(offset_str: &str) -> Option<PostgresOffset> {
    PostgresOffset::parse_debezium_offset(offset_str).ok()
}

/// Extract PostgreSQL `lsn_commit` from a CDC offset JSON string.
///
/// `lsn_commit` is the WAL position of the last COMMIT message dbz has
/// processed. It is the value dbz flushes to PG (`flushLsn`) to advance
/// `confirmed_flush_lsn`, and the value `validateLogPosition` compares
/// against `slot.restart_lsn` on restart. Must be monotonic across
/// restarts -- a regression here can cause a fatal `DebeziumException`
/// ("change stream ... no longer available").
pub fn extract_postgres_lsn_commit_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn_commit")?;
    lsn.as_u64()
}

/// Extract PostgreSQL `lsn_proc` from a CDC offset JSON string.
///
/// `lsn_proc` is the WAL position of the last message dbz has fully
/// processed. On restart dbz uses it as the start position passed to
/// `startStreaming`, and as the dedup key for `WalPositionLocator` to
/// filter replayed events. A regression can cause silent data loss or
/// a hung resume if PG has already advanced past the rewound value.
pub fn extract_postgres_lsn_proc_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn_proc")?;
    lsn.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a streaming-phase PG offset JSON. Sets `lsn`, `lsn_proc`,
    /// and `lsn_commit` all to the same value -- this is the common
    /// shape for the guard's basic monotonic tests.
    fn pg_streaming_offset_json(lsn: u64) -> String {
        pg_streaming_offset_json_full(lsn, lsn, lsn)
    }

    /// Build a streaming-phase PG offset JSON with independent control
    /// over `lsn`, `lsn_proc`, and `lsn_commit`. Used to exercise
    /// transaction-interleave and asymmetric-regression scenarios.
    fn pg_streaming_offset_json_full(lsn: u64, lsn_proc: u64, lsn_commit: u64) -> String {
        format!(
            r#"{{
                "sourcePartition": {{"server": "RW_CDC_1001"}},
                "sourceOffset": {{
                    "last_snapshot_record": false,
                    "lsn": {lsn},
                    "lsn_proc": {lsn_proc},
                    "lsn_commit": {lsn_commit},
                    "txId": 12345,
                    "ts_usec": 1700000000000000
                }},
                "isHeartbeat": false
            }}"#
        )
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_rejects_backward_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        // Push a backward LSN -- must be rejected, current offset kept.
        split
            .update_offset(pg_streaming_offset_json(150))
            .expect("update_offset must not error on rejection");
        assert_eq!(split.pg_lsn(), Some(200));
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_allows_forward_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        split.update_offset(pg_streaming_offset_json(250)).unwrap();
        assert_eq!(split.pg_lsn(), Some(250));
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_allows_equal_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        // Equal is allowed (no regression).
        split.update_offset(pg_streaming_offset_json(200)).unwrap();
        assert_eq!(split.pg_lsn(), Some(200));
    }

    #[test]
    fn test_postgres_offset_snapshot_phase_bypasses_guard() {
        let snapshot_offset = r#"{
            "sourcePartition": {"server": "RW_CDC_1001"},
            "sourceOffset": {
                "last_snapshot_record": false,
                "lsn": 150,
                "txId": 12345,
                "ts_usec": 1700000000000000
            },
            "isHeartbeat": false
        }"#;
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        // Snapshot / incomplete offsets do not have a comparable streaming
        // `lsn_proc`, so they pass through even when the plain `lsn` is lower.
        split.update_offset(snapshot_offset.to_owned()).unwrap();
        assert_eq!(split.pg_lsn(), Some(150));
    }

    #[test]
    fn test_postgres_offset_lsn_commit_regression_rejected() {
        // `lsn_commit` regressing alone must trigger reject -- this is
        // the field validateLogPosition checks, regression here is the
        // catastrophic case.
        let initial = pg_streaming_offset_json_full(200, 200, 200);
        let mut split = PostgresCdcSplit::new(1, Some(initial));

        // lsn_commit 200 -> 150, but lsn / lsn_proc stay at 200.
        let new = pg_streaming_offset_json_full(200, 200, 150);
        split.update_offset(new).unwrap();

        assert_eq!(
            split.pg_lsn_commit(),
            Some(200),
            "lsn_commit must not regress"
        );
    }

    #[test]
    fn test_postgres_offset_lsn_proc_regression_rejected() {
        // `lsn_proc` regressing alone must trigger reject -- this is the
        // field dbz uses as the startStreaming resume position, and
        // WalPositionLocator's dedup key on restart.
        let initial = pg_streaming_offset_json_full(200, 200, 200);
        let mut split = PostgresCdcSplit::new(1, Some(initial));

        // lsn_proc 200 -> 150, but lsn / lsn_commit stay at 200.
        let new = pg_streaming_offset_json_full(200, 150, 200);
        split.update_offset(new).unwrap();

        assert_eq!(split.pg_lsn_proc(), Some(200), "lsn_proc must not regress");
    }

    #[test]
    fn test_postgres_offset_interleaved_tx_lsn_proc_drop_allowed_by_commit_lsn() {
        // Concurrent overlapping transactions under PG logical
        // replication produce a state-table chunk where the per-event
        // `lsn` and `lsn_proc` move backwards while `lsn_commit`
        // strictly advances (because PG streams events in commit
        // order, and the earlier-begin transaction's INSERT has a
        // smaller physical LSN even though it commits later).
        //
        // Use the same Postgres offset ordering as #22503: compare
        // `lsn_commit` first, then `lsn_proc`. Since the commit LSN
        // advances, this update is not a regression.
        let initial = pg_streaming_offset_json_full(33_967_592, 33_967_592, 33_893_392);
        let mut split = PostgresCdcSplit::new(1, Some(initial));

        let new = pg_streaming_offset_json_full(33_918_336, 33_918_336, 33_969_832);
        split.update_offset(new).unwrap();

        assert_eq!(split.pg_lsn_commit(), Some(33_969_832));
        assert_eq!(split.pg_lsn_proc(), Some(33_918_336));
    }
}
