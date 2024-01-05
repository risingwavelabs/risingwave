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
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::core::Collector;
use prometheus::{HistogramOpts, HistogramVec};

#[derive(Debug, Default)]
pub struct SlowOpHistogramInfo<const N: usize> {
    starts: HashMap<Instant, [String; N]>,
    to_collect: Vec<([String; N], Duration)>,
    labels_count: HashMap<[String; N], usize>,
}

#[derive(Debug, Clone)]
pub struct SlowOpHistogramVec<const N: usize> {
    vec: HistogramVec,

    threshold: Duration,

    info: Arc<Mutex<SlowOpHistogramInfo<N>>>,

    _labels: [&'static str; N],
}

impl<const N: usize> SlowOpHistogramVec<N> {
    pub fn new(opts: HistogramOpts, labels: &[&'static str; N], threshold: Duration) -> Self {
        let vec = HistogramVec::new(opts, labels).unwrap();
        Self {
            vec,
            threshold,
            info: Arc::new(Mutex::new(SlowOpHistogramInfo::default())),
            _labels: *labels,
        }
    }

    pub fn monitor(&self, label_values: &[&str; N]) -> LabeledSlowOpHistogramVecGuard<N> {
        let label_values = label_values.map(|str| str.to_string());

        let mut guard = self.info.lock();
        *guard.labels_count.entry(label_values).or_default() += 1;

        LabeledSlowOpHistogramVecGuard {
            owner: self.clone(),
            start: Instant::now(),
        }
    }
}

impl<const N: usize> Collector for SlowOpHistogramVec<N> {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        self.vec.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut guard = self.info.lock();
        let mut to_collect = vec![];
        std::mem::swap(&mut guard.to_collect, &mut to_collect);
        let dropped = to_collect.len();

        for (start, label_values) in &guard.starts {
            to_collect.push((label_values.clone(), start.elapsed()));
        }
        for (index, (label_values, duration)) in to_collect.into_iter().enumerate() {
            let label_values_str = label_values.iter().map(|s| s.as_str()).collect_vec();
            if duration >= self.threshold {
                self.vec
                    .with_label_values(&label_values_str)
                    .observe(duration.as_secs_f64());
            }
            if index < dropped {
                let count = guard.labels_count.get_mut(&label_values).unwrap();
                *count -= 1;
                if *count == 0 {
                    self.vec
                        .remove_label_values(&label_values_str)
                        .expect("should exist");
                }
                guard
                    .labels_count
                    .remove(&label_values)
                    .expect("should exist");
            }
        }
        drop(guard);

        self.vec.collect()
    }
}

pub struct LabeledSlowOpHistogramVecGuard<const N: usize> {
    owner: SlowOpHistogramVec<N>,
    start: Instant,
}

impl<const N: usize> Drop for LabeledSlowOpHistogramVecGuard<N> {
    fn drop(&mut self) {
        let mut guard = self.owner.info.lock();
        if let Some(label_values) = guard.starts.remove(&self.start) {
            let duration = self.start.elapsed();
            guard.to_collect.push((label_values, duration));
        }
    }
}
