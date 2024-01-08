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
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::core::Collector;
use prometheus::{HistogramOpts, HistogramVec};

#[derive(Debug)]
pub struct SlowOpHistogramOpts {
    pub inner: HistogramOpts,
    pub threshold: Duration,
}

#[derive(Debug, Default)]
pub struct SlowOpHistogramInfo<const N: usize> {
    /// incremental sequence
    sequence: usize,
    /// ongoing op to watch
    ///
    /// NOTE: Use incremental sequence as key, because [`Instant`] is
    /// monotonically nondecreasing, instead of monotonically increasing.
    ongoing: HashMap<usize, ([String; N], Instant)>,
    /// count of duplicated label values
    labels_count: HashMap<[String; N], usize>,
    /// dropped label values to remove
    labels_to_remove: Vec<[String; N]>,
}

#[derive(Clone)]
pub struct SlowOpHistogramVec<const N: usize> {
    inner: HistogramVec,

    threshold: Duration,

    info: Arc<Mutex<SlowOpHistogramInfo<N>>>,

    labels: [&'static str; N],
}

impl<const N: usize> Debug for SlowOpHistogramVec<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlowOpHistogramVec")
            .field("vec", &self.inner)
            .field("threshold", &self.threshold)
            .field("labels", &self.labels)
            .finish()
    }
}

impl<const N: usize> SlowOpHistogramVec<N> {
    pub fn new(opts: SlowOpHistogramOpts, labels: &[&'static str; N]) -> Self {
        let inner = HistogramVec::new(opts.inner, labels).unwrap();
        Self {
            inner,
            threshold: opts.threshold,
            info: Arc::new(Mutex::new(SlowOpHistogramInfo::default())),
            labels: *labels,
        }
    }

    pub fn monitor(&self, label_values: &[&str; N]) -> LabeledSlowOpHistogramVecGuard<N> {
        let label_values = label_values.map(|str| str.to_string());

        let start = Instant::now();

        let mut guard = self.info.lock();
        let sequence = guard.sequence;
        guard.sequence += 1;
        *guard.labels_count.entry(label_values.clone()).or_default() += 1;
        guard.ongoing.insert(sequence, (label_values, start));

        LabeledSlowOpHistogramVecGuard {
            owner: self.clone(),
            sequence,
        }
    }
}

impl<const N: usize> Collector for SlowOpHistogramVec<N> {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.inner.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut guard = self.info.lock();

        // iterate through uncompleted tasks
        for (label_values, start) in guard.ongoing.values() {
            let label_values_str = label_values.iter().map(|s| s.as_str()).collect_vec();
            let duration = start.elapsed();
            if duration >= self.threshold {
                self.inner
                    .with_label_values(&label_values_str)
                    .observe(duration.as_secs_f64());
            }
        }

        let res = self.inner.collect();

        for label_values in guard.labels_to_remove.drain(..) {
            let label_values_str = label_values.iter().map(|s| s.as_str()).collect_vec();
            let _ = self.inner.remove_label_values(&label_values_str);
        }

        drop(guard);

        res
    }
}

pub struct LabeledSlowOpHistogramVecGuard<const N: usize> {
    owner: SlowOpHistogramVec<N>,
    sequence: usize,
}

impl<const N: usize> Drop for LabeledSlowOpHistogramVecGuard<N> {
    fn drop(&mut self) {
        let mut guard = self.owner.info.lock();
        if let Some((label_values, start)) = guard.ongoing.remove(&self.sequence) {
            let duration = start.elapsed();
            let label_values_str = label_values.iter().map(|s| s.as_str()).collect_vec();

            if duration >= self.owner.threshold {
                self.owner
                    .inner
                    .with_label_values(&label_values_str)
                    .observe(duration.as_secs_f64());
            }

            let count = guard.labels_count.get_mut(&label_values).unwrap();
            *count -= 1;
            if *count == 0 {
                guard
                    .labels_count
                    .remove(&label_values)
                    .expect("should exist");
                guard.labels_to_remove.push(label_values);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use futures::future::join_all;
    use prometheus::{exponential_buckets, histogram_opts};
    use tokio::sync::Barrier;
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_slow_op_histogram() {
        let opts = SlowOpHistogramOpts {
            inner: histogram_opts!("test", "test", exponential_buckets(0.01, 10.0, 7).unwrap()),
            threshold: Duration::from_millis(100),
        };
        let slow = SlowOpHistogramVec::new(opts, &["id"]);
        let b = Arc::new(Barrier::new(2));

        let s1 = slow.clone();
        let s2 = slow.clone();
        let s3 = slow.clone();
        let b3 = b.clone();

        let h1 = tokio::spawn(async move {
            let guard = s1.monitor(&["1"]);
            sleep(Duration::from_millis(50)).await;
            drop(guard);
        });
        let h2 = tokio::spawn(async move {
            let guard = s2.monitor(&["2"]);
            sleep(Duration::from_millis(150)).await;
            drop(guard);
        });
        let h3 = tokio::spawn(async move {
            let guard = s3.monitor(&["3"]);
            b3.wait().await;
            drop(guard);
        });

        join_all([h1, h2]).await;

        let mf = slow.collect().pop().unwrap();
        assert_eq!(mf.get_metric().len(), 2);
        let mut labels = mf
            .get_metric()
            .iter()
            .map(|m| m.get_label().iter().map(|p| p.get_value()).collect_vec())
            .collect_vec();
        labels.sort();
        assert_eq!(labels, vec![vec!["2"], vec!["3"]]);

        b.wait().await;
        h3.await.unwrap();

        let mf = slow.collect().pop().unwrap();
        assert_eq!(mf.get_metric().len(), 1);
        assert_eq!(
            mf.get_metric()
                .iter()
                .map(|m| m.get_label().iter().map(|p| p.get_value()).collect_vec())
                .collect_vec(),
            vec![vec!["3"]],
        );

        assert!(slow.info.lock().ongoing.is_empty());
        assert!(slow.info.lock().labels_count.is_empty());

        let s1 = slow.clone();
        let s2 = slow.clone();
        let b = Arc::new(Barrier::new(2));
        let b2 = b.clone();

        let h1 = tokio::spawn(async move {
            let guard = s1.monitor(&["1"]);
            sleep(Duration::from_millis(150)).await;
            drop(guard);
        });
        let h2 = tokio::spawn(async move {
            let guard = s2.monitor(&["1"]);
            b2.wait().await;
            drop(guard);
        });

        h1.await.unwrap();
        let mf = slow.collect().pop().unwrap();
        assert_eq!(mf.get_metric().len(), 1);
        assert_eq!(
            mf.get_metric()
                .iter()
                .map(|m| m.get_label().iter().map(|p| p.get_value()).collect_vec())
                .collect_vec(),
            vec![vec!["1"]],
        );
        assert_eq!(1, slow.info.lock().ongoing.len());
        assert_eq!(
            1,
            slow.info
                .lock()
                .labels_count
                .get(&["1".to_string()])
                .copied()
                .unwrap()
        );

        b.wait().await;

        h2.await.unwrap();
        let mf = slow.collect().pop().unwrap();
        assert_eq!(mf.get_metric().len(), 1);
        assert_eq!(
            mf.get_metric()
                .iter()
                .map(|m| m.get_label().iter().map(|p| p.get_value()).collect_vec())
                .collect_vec(),
            vec![vec!["1"]],
        );
        assert!(slow.info.lock().ongoing.is_empty());
        assert!(slow.info.lock().labels_count.is_empty());
    }
}
