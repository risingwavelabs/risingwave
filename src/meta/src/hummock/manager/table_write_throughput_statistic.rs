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

use std::collections::{HashMap, VecDeque};

use risingwave_common::catalog::TableId;

#[derive(Debug, Clone)]
pub struct TableWriteThroughputStatistic {
    pub throughput: u64,
    pub timestamp_secs: i64,
    /// Expected checkpoint duration represented by this sample. Missing commits are not samples.
    pub interval_secs: i64,
}

impl AsRef<TableWriteThroughputStatistic> for TableWriteThroughputStatistic {
    fn as_ref(&self) -> &TableWriteThroughputStatistic {
        self
    }
}

impl TableWriteThroughputStatistic {
    pub fn is_expired(&self, max_statistic_expired_secs: i64, timestamp_secs: i64) -> bool {
        // max(0) is used to avoid overflow
        (timestamp_secs - self.timestamp_secs).max(0) > max_statistic_expired_secs
    }
}

#[derive(Debug, Clone)]
pub struct TableWriteThroughputStatisticManager {
    table_throughput: HashMap<TableId, VecDeque<TableWriteThroughputStatistic>>,
    max_statistic_expired_secs: i64,
}

impl TableWriteThroughputStatisticManager {
    pub fn new(max_statistic_expired_secs: i64) -> Self {
        Self {
            table_throughput: HashMap::new(),
            max_statistic_expired_secs,
        }
    }

    pub fn add_table_throughput_with_ts(
        &mut self,
        table_id: TableId,
        throughput: u64,
        timestamp_secs: i64,
        interval_secs: i64,
    ) {
        let table_throughput = self.table_throughput.entry(table_id).or_default();
        table_throughput.push_back(TableWriteThroughputStatistic {
            throughput,
            timestamp_secs,
            interval_secs,
        });

        // skip expired statistics
        while let Some(statistic) = table_throughput.front() {
            if statistic.is_expired(self.max_statistic_expired_secs, timestamp_secs) {
                table_throughput.pop_front();
            } else {
                break;
            }
        }

        if table_throughput.is_empty() {
            self.table_throughput.remove(&table_id);
        }
    }

    // `get_table_throughput` return the statistics of the table with the given `table_id` within the given `window_secs`.
    // The statistics are sorted by timestamp in descending order.
    pub fn get_table_throughput_descending(
        &self,
        table_id: TableId,
        window_secs: i64,
    ) -> impl Iterator<Item = &TableWriteThroughputStatistic> {
        self.get_table_throughput_descending_at(
            table_id,
            window_secs,
            chrono::Utc::now().timestamp(),
        )
    }

    pub fn get_table_throughput_descending_at(
        &self,
        table_id: TableId,
        window_secs: i64,
        timestamp_secs: i64,
    ) -> impl Iterator<Item = &TableWriteThroughputStatistic> {
        self.table_throughput
            .get(&table_id)
            .into_iter()
            .flatten()
            .rev()
            .take_while(move |statistic| !statistic.is_expired(window_secs, timestamp_secs))
    }

    /// Observed seconds in the window, excluding gaps and overlapping checkpoint intervals.
    /// A paused table has no fresh observation and must not be classified as idle.
    pub fn observed_window_secs(&self, table_id: TableId, window_secs: i64, now: i64) -> i64 {
        let mut samples = self
            .get_table_throughput_descending_at(table_id, window_secs, now)
            .peekable();
        let Some(latest) = samples.peek() else {
            return 0;
        };
        if now - latest.timestamp_secs > latest.interval_secs {
            return 0;
        }
        let window_start = now - window_secs;
        let mut end = now;
        let mut observed = 0;
        for sample in samples {
            let start = (sample.timestamp_secs - sample.interval_secs).max(window_start);
            observed += (sample.timestamp_secs.min(end) - start).max(0);
            end = end.min(start);
        }
        observed
    }

    pub fn remove_table(&mut self, table_id: TableId) {
        self.table_throughput.remove(&table_id);
    }

    // `avg_write_throughput` returns the average write throughput of the table with the given `table_id` within the given `window_secs`.
    pub fn avg_write_throughput(&self, table_id: TableId, window_secs: i64) -> f64 {
        let mut total_throughput = 0;
        let mut total_count = 0;
        let mut statistic_iter = self
            .get_table_throughput_descending(table_id, window_secs)
            .peekable();

        if statistic_iter.peek().is_none() {
            return 0.0;
        }

        for statistic in statistic_iter {
            total_throughput += statistic.throughput;
            total_count += 1;
        }

        total_throughput as f64 / total_count as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observed_window_pause_resume_and_idle() {
        let table = TableId::new(100);
        let mut stats = TableWriteThroughputStatisticManager::new(240);
        assert_eq!(stats.observed_window_secs(table, 240, 1000), 0);
        for now in 761..=1000 {
            stats.add_table_throughput_with_ts(table, 0, now, 1);
        }
        assert_eq!(stats.observed_window_secs(table, 240, 1000), 240);
        // Pausing commits does not turn missing time into observed zero throughput.
        assert_eq!(stats.observed_window_secs(table, 240, 1200), 0);
        stats.add_table_throughput_with_ts(table, 0, 1200, 1);
        assert_eq!(stats.observed_window_secs(table, 240, 1200), 41);
        for now in 1201..=1440 {
            stats.add_table_throughput_with_ts(table, 0, now, 1);
        }
        assert_eq!(stats.observed_window_secs(table, 240, 1440), 240);
    }

    #[test]
    fn test_observed_window_does_not_double_count_bunched_commits() {
        let table = TableId::new(100);
        let mut stats = TableWriteThroughputStatisticManager::new(240);
        for _ in 0..240 {
            stats.add_table_throughput_with_ts(table, 0, 1000, 10);
        }
        assert_eq!(stats.observed_window_secs(table, 240, 1000), 10);
        // The observation cadence is configured, not a hardcoded one-second requirement.
        stats.add_table_throughput_with_ts(table, 0, 1010, 10);
        assert_eq!(stats.observed_window_secs(table, 240, 1010), 20);
        assert_eq!(stats.observed_window_secs(table, 240, 1021), 0);
    }
}
