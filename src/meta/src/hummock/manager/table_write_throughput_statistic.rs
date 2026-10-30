// Copyright 2024 RisingWave Labs
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
use std::time::Duration;

use risingwave_common::catalog::TableId;
use tokio::time::Instant;

// Coalesce bursts of commit completions instead of treating each completion as a sample.
const MIN_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const BUCKET_COUNT: usize = 6;

#[derive(Debug, Clone, Copy, Default)]
struct ThroughputBucket {
    bytes: f64,
    seconds: f64,
    peak: u64,
    last_sample: Option<Instant>,
}

#[derive(Debug, Clone)]
struct TableThroughput {
    buckets: [ThroughputBucket; BUCKET_COUNT],
    observed_since: Instant,
    sample_start: Instant,
    last_interval: Duration,
    latest: Option<u64>,
    pending_bytes: u64,
}

impl TableThroughput {
    fn record(&mut self, bytes: u64, now: Instant, bucket_width: Duration) {
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        let elapsed = now.duration_since(self.sample_start);
        let index = (now.duration_since(self.observed_since).as_nanos() / bucket_width.as_nanos()
            % BUCKET_COUNT as u128) as usize;
        let bucket = &mut self.buckets[index];
        // Only reset the slot being reused. Queries exclude stale slots by timestamp,
        // so even a long gap needs neither a rotation loop nor synthetic zero samples.
        if bucket
            .last_sample
            .is_some_and(|last| now.duration_since(last) >= bucket_width)
        {
            *bucket = ThroughputBucket::default();
        }
        let rate = self.pending_bytes as f64 / elapsed.max(MIN_SAMPLE_INTERVAL).as_secs_f64();
        // Retain pending bounds too: they can already trigger split, even if a later
        // completion averages the same bytes over a longer, colder interval.
        // Round up so a fractional rate above a threshold is never classified as cold.
        bucket.peak = bucket.peak.max(rate.ceil() as u64);
        bucket.last_sample = Some(now);
        if elapsed < MIN_SAMPLE_INTERVAL {
            return;
        }
        bucket.bytes += self.pending_bytes as f64;
        bucket.seconds += elapsed.as_secs_f64();
        self.latest = Some(rate as u64);
        self.pending_bytes = 0;
        self.sample_start = now;
        self.last_interval = elapsed;
    }

    fn window(&self, now: Instant, window: Duration) -> impl Iterator<Item = &ThroughputBucket> {
        self.buckets.iter().filter(move |bucket| {
            bucket
                .last_sample
                .is_some_and(|last| now.duration_since(last) <= window)
        })
    }
}

/// Successful Hummock ingress, independent of configured checkpoint periods.
/// One six-bucket history serves all consumers. A bucket is one fifth of the retention
/// window (at least one second). Whole boundary buckets extend history by at most one bucket.
/// Completed rates represent whole observed commit intervals; pending ingress uses a
/// conservative one-second bound. Long intervals cannot reveal bursts within them.
/// Timer reads neither add zero observations nor advance the history.
#[derive(Debug, Clone)]
pub struct TableWriteThroughputStatisticManager {
    tables: HashMap<TableId, TableThroughput>,
    retention: Duration,
    bucket_width: Duration,
}

impl TableWriteThroughputStatisticManager {
    pub fn new(retention_secs: usize) -> Self {
        let retention = Duration::from_secs(retention_secs.max(1) as u64);
        Self {
            tables: HashMap::new(),
            retention,
            bucket_width: (retention / (BUCKET_COUNT - 1) as u32).max(MIN_SAMPLE_INTERVAL),
        }
    }

    pub fn record_commit(&mut self, table_id: TableId, bytes: u64, now: Instant) {
        match self.tables.entry(table_id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                // The first commit has no known starting time. Establish a baseline rather
                // than attributing its bytes to an invented checkpoint interval.
                entry.insert(TableThroughput {
                    buckets: [ThroughputBucket::default(); BUCKET_COUNT],
                    observed_since: now,
                    sample_start: now,
                    last_interval: MIN_SAMPLE_INTERVAL,
                    latest: None,
                    pending_bytes: 0,
                });
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().record(bytes, now, self.bucket_width);
            }
        }
    }

    /// Historical peak after a full observed window, or None if history is missing/stale.
    /// Anchor history at the last successful sample so slow, successful empty commits remain
    /// useful between arrivals. Silence beyond the window or observed cadence is unknown.
    pub fn max_write_throughput(&self, table_id: TableId, now: Instant) -> Option<u64> {
        let table = self.tables.get(&table_id)?;
        if table.sample_start.duration_since(table.observed_since) < self.retention
            || now.duration_since(table.sample_start) > self.retention.max(table.last_interval)
        {
            return None;
        }
        let peak = table
            .window(table.sample_start, self.retention)
            .map(|bucket| bucket.peak)
            .max()?;
        // The one-second minimum sample interval makes pending bytes a rate bound.
        // Include the same bound as latest_table_throughput so pending hot ingress
        // cannot qualify as cold just because no later commit has closed its sample.
        Some(peak.max(table.pending_bytes))
    }

    /// Latest completed rate, with a provisional one-second bound for pending ingress.
    /// A burst of successful commits remains visible even without another completion to
    /// close its sample. Reads never add zero observations or dilute a rate with silence.
    pub fn latest_table_throughput(&self, table_id: TableId) -> Option<u64> {
        let table = self.tables.get(&table_id)?;
        if Instant::now().duration_since(table.sample_start)
            > self.retention.max(table.last_interval)
            || (table.latest.is_none() && table.pending_bytes == 0)
        {
            return None;
        }
        Some(table.latest.unwrap_or(0).max(table.pending_bytes))
    }

    /// Window-averaged ingress for metrics, using the same configured retention.
    /// Scheduling uses `latest_table_throughput` and `max_write_throughput` instead.
    /// Include whole boundary buckets and complete commit intervals; do not assume bytes
    /// arrived uniformly within a slow commit interval or count unobserved silence as zero.
    pub fn avg_write_throughput(&self, table_id: TableId) -> f64 {
        let Some(table) = self.tables.get(&table_id) else {
            return 0.0;
        };
        let now = Instant::now();
        let (mut bytes, mut seconds) = table
            .window(now, self.retention)
            .fold((0.0, 0.0), |(bytes, seconds), bucket| {
                (bytes + bucket.bytes, seconds + bucket.seconds)
            });
        let elapsed = now.duration_since(table.sample_start);
        if table.pending_bytes > 0 && elapsed <= self.retention {
            // Include bytes from completed commits even if their coalesced sample is still
            // open. Reads do not create observations; the minimum interval bounds the rate.
            bytes += table.pending_bytes as f64;
            seconds += elapsed.max(MIN_SAMPLE_INTERVAL).as_secs_f64();
        }
        if seconds == 0.0 { 0.0 } else { bytes / seconds }
    }

    pub fn remove_table(&mut self, table_id: TableId) {
        self.tables.remove(&table_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::test_utils::advance_time;

    #[test]
    fn test_peak_retention_at_bucket_boundaries() {
        let default_retention = risingwave_common::config::meta::default::meta::table_write_throughput_retention_seconds();
        assert_eq!(
            TableWriteThroughputStatisticManager::new(default_retention).bucket_width,
            Duration::from_secs(60)
        );
        let table = TableId::new(100);
        let start = Instant::now();
        for retention in [1_u64, 3, 10, 240, 300] {
            let bucket_width = retention.div_ceil((BUCKET_COUNT - 1) as u64).max(1);
            for offset in 1..=bucket_width {
                let mut stats = TableWriteThroughputStatisticManager::new(retention as usize);
                for second in 0..=retention + 2 * bucket_width + 1 {
                    let now = start + Duration::from_secs(second);
                    stats.record_commit(table, if second == offset { 32 } else { 0 }, now);
                    let peak = stats.max_write_throughput(table, now);
                    if second < retention {
                        assert_eq!(peak, None);
                    } else if second - offset < retention {
                        assert_eq!(
                            peak,
                            Some(32),
                            "retention={retention}, offset={offset}, second={second}"
                        );
                    } else if second - offset >= retention + bucket_width {
                        assert_eq!(
                            peak,
                            Some(0),
                            "retention={retention}, offset={offset}, second={second}"
                        );
                    }
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_window_rate_weights_elapsed_time() {
        let table = TableId::new(100);
        let mut stats = TableWriteThroughputStatisticManager::new(240);
        stats.record_commit(table, 0, Instant::now());
        advance_time(Duration::from_secs(1)).await;
        stats.record_commit(table, 100, Instant::now());
        advance_time(Duration::from_secs(99)).await;
        stats.record_commit(table, 0, Instant::now());
        // Two completions are not two equal votes: 100 bytes arrived over 100 seconds.
        assert_eq!(stats.avg_write_throughput(table), 1.0);
        // A burst completed within the minimum sample interval still contributes to
        // the metric, even if no subsequent commit arrives to close that sample.
        stats.record_commit(table, 900, Instant::now());
        assert_eq!(stats.avg_write_throughput(table), 1000.0 / 101.0);
        advance_time(Duration::from_secs(1)).await;
        stats.record_commit(table, 0, Instant::now());
        assert_eq!(stats.avg_write_throughput(table), 1000.0 / 101.0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_actual_intervals_and_bunched_commits() {
        let table = TableId::new(100);
        let mut stats = TableWriteThroughputStatisticManager::new(240);
        stats.record_commit(table, 999, Instant::now());
        assert_eq!(stats.latest_table_throughput(table), None);
        // Pending ingress is visible even before the first full sample has completed.
        stats.record_commit(table, 100, Instant::now());
        assert_eq!(stats.latest_table_throughput(table), Some(100));
        advance_time(Duration::from_secs(1)).await;
        stats.record_commit(table, 0, Instant::now());
        for seconds in [2, 10, 300, 1] {
            advance_time(Duration::from_secs(seconds)).await;
            stats.record_commit(table, 100 * seconds, Instant::now());
            assert_eq!(stats.latest_table_throughput(table), Some(100));
        }
        // Repeated completions at the same instant retain their bytes without gaining votes.
        for _ in 0..1000 {
            stats.record_commit(table, 1, Instant::now());
        }
        assert_eq!(stats.latest_table_throughput(table), Some(1000));
        assert_eq!(
            stats.max_write_throughput(table, Instant::now()),
            Some(1000)
        );
        advance_time(Duration::from_secs(1)).await;
        stats.record_commit(table, 0, Instant::now());
        assert_eq!(stats.latest_table_throughput(table), Some(1000));
        for _ in 0..500 {
            advance_time(Duration::from_secs(1)).await;
            stats.record_commit(table, 0, Instant::now());
        }

        assert_eq!(stats.max_write_throughput(table, Instant::now()), Some(0));
        stats.record_commit(table, 100, Instant::now());
        advance_time(Duration::from_secs(10)).await;
        stats.record_commit(table, 0, Instant::now());
        assert_eq!(stats.latest_table_throughput(table), Some(10));
        assert_eq!(
            stats.max_write_throughput(table, Instant::now()),
            Some(100),
            "closing a sample must retain the pending rate that could already trigger split"
        );
        stats.record_commit(table, 100, Instant::now());
        advance_time(Duration::from_secs(241)).await;
        assert_eq!(stats.latest_table_throughput(table), None);
        assert_eq!(stats.max_write_throughput(table, Instant::now()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_unknown_pause_resume_and_slow_idle() {
        let table = TableId::new(100);
        let mut stats = TableWriteThroughputStatisticManager::new(240);
        assert_eq!(stats.max_write_throughput(table, Instant::now()), None);
        stats.record_commit(table, 0, Instant::now());
        advance_time(Duration::from_secs(300)).await;
        assert_eq!(stats.max_write_throughput(table, Instant::now()), None);
        stats.record_commit(table, 0, Instant::now());
        assert_eq!(stats.max_write_throughput(table, Instant::now()), Some(0));
        advance_time(Duration::from_secs(300)).await;
        stats.record_commit(table, 0, Instant::now());
        // Slow successful empty commits provide evidence. A timer between them must not
        // require a shorter commit interval than the configured history window.
        advance_time(Duration::from_secs(299)).await;
        assert_eq!(stats.max_write_throughput(table, Instant::now()), Some(0));
        assert_eq!(stats.latest_table_throughput(table), Some(0));
        advance_time(Duration::from_secs(2)).await;
        assert_eq!(stats.max_write_throughput(table, Instant::now()), None);
        assert_eq!(stats.latest_table_throughput(table), None);
        // A successful empty commit accounts for the elapsed interval. Subsequent backlog
        // drain is real Hummock ingress and may legitimately turn the table hot again.
        stats.record_commit(table, 0, Instant::now());
        assert_eq!(stats.max_write_throughput(table, Instant::now()), Some(0));
        advance_time(Duration::from_secs(1)).await;
        stats.record_commit(table, 100, Instant::now());
        assert_eq!(stats.latest_table_throughput(table), Some(100));
        assert_eq!(stats.max_write_throughput(table, Instant::now()), Some(100));
        stats.remove_table(table);
        assert_eq!(stats.latest_table_throughput(table), None);
    }
}
