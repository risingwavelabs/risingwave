// Copyright 2023 RisingWave Labs
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

use std::fmt::{Display, Formatter};

use itertools::Itertools;
use prometheus::proto::Histogram;

use crate::util::iter_util::ZipEqFast;

#[derive(Clone, Default, Debug)]
pub struct MyHistogram {
    pub upper_bound_list: Vec<f64>,
    pub count_list: Vec<u64>,
    pub total_count: u64,
    pub total_sum: f64,
}

impl MyHistogram {
    pub fn from_prom_hist(histogram: &Histogram) -> MyHistogram {
        let mut upper_bound_list = Vec::new();
        let mut count_list = Vec::new();

        let total_count = histogram.get_sample_count();
        let total_sum = histogram.get_sample_sum();

        let buckets = histogram.get_bucket();
        for bucket in buckets {
            let upper_bound = bucket.get_upper_bound();
            let count = bucket.get_cumulative_count();
            upper_bound_list.push(upper_bound);
            count_list.push(count);
        }

        MyHistogram {
            upper_bound_list,
            count_list,
            total_count,
            total_sum,
        }
    }

    pub fn from_diff(prev: &MyHistogram, cur: &MyHistogram) -> MyHistogram {
        MyHistogram {
            upper_bound_list: cur.upper_bound_list.clone(),
            count_list: match prev.count_list.is_empty() {
                true => cur.count_list.clone(),
                false => prev
                    .count_list
                    .iter()
                    .zip_eq_fast(cur.count_list.iter())
                    .map(|(&pb, &cb)| cb - pb)
                    .collect_vec(),
            },
            total_sum: cur.total_sum - prev.total_sum,
            total_count: cur.total_count - prev.total_count,
        }
    }

    pub fn get_percentile(&self, p: f64) -> f64 {
        let sample_count = self.total_count;

        // empty bucket may appear
        if sample_count == 0 {
            return 0.0;
        }
        let threshold = (sample_count as f64 * (p / 100.0_f64)).ceil() as u64;
        let mut last_upper_bound = 0.0;
        let mut last_count = 0;
        for (&upper_bound, &count) in self
            .upper_bound_list
            .iter()
            .zip_eq_fast(self.count_list.iter())
        {
            if count >= threshold {
                // assume scale linearly within this bucket,
                // return a value between last_upper_bound and upper_bound
                let right_left_diff = upper_bound - last_upper_bound;
                return last_upper_bound
                    + right_left_diff * (threshold - last_count) as f64
                        / (count - last_count) as f64;
            }
            last_upper_bound = upper_bound;
            last_count = count;
        }

        0.0
    }
}

impl Display for MyHistogram {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // calculate latencies statistics
        let mean = self.total_sum / self.total_count as f64;
        let p50 = self.get_percentile(50.0);
        let p90 = self.get_percentile(90.0);
        let p99 = self.get_percentile(99.0);
        let p100 = self.get_percentile(100.0);

        write!(
            f,
            "latency:
        mean: {},
        p50: {},
        p90: {},
        p99: {},
        p100: {};",
            mean, p50, p90, p99, p100
        )
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Metric;
    use prometheus::{histogram_opts, register_histogram_with_registry, Registry};

    use super::*;

    #[test]
    fn test_proc_histogram_basic() {
        fn new_simple_histogram(upper_bound: u64) -> MyHistogram {
            let registry = Registry::new();
            let buckets = (1..=upper_bound).map(|x| x as f64).collect::<Vec<f64>>();
            let opts = histogram_opts!("test_histogram", "test_histogram", buckets);

            let histogram = register_histogram_with_registry!(opts, registry).unwrap();

            for value in 1..=upper_bound {
                histogram.observe(value as f64);
            }

            MyHistogram::from_prom_hist(histogram.metric().get_histogram())
        }

        let histogram = new_simple_histogram(999);
        assert_eq!(histogram.get_percentile(50.0) as u64, 500);
        assert_eq!(histogram.get_percentile(90.0) as u64, 900);
        assert_eq!(histogram.get_percentile(99.0) as u64, 990);
        assert_eq!(histogram.get_percentile(99.9) as u64, 999);
        assert_eq!(histogram.get_percentile(100.0) as u64, 999);

        let histogram = new_simple_histogram(1000);
        assert_eq!(histogram.get_percentile(50.0) as u64, 500);
        assert_eq!(histogram.get_percentile(90.0) as u64, 900);
        assert_eq!(histogram.get_percentile(99.0) as u64, 990);
        assert_eq!(histogram.get_percentile(99.9) as u64, 1000);
        assert_eq!(histogram.get_percentile(100.0) as u64, 1000);

        let histogram = new_simple_histogram(9999);
        assert_eq!(histogram.get_percentile(50.0) as u64, 5000);
        assert_eq!(histogram.get_percentile(90.0) as u64, 9000);
        assert_eq!(histogram.get_percentile(99.0) as u64, 9900);
        assert_eq!(histogram.get_percentile(99.9) as u64, 9990);
        assert_eq!(histogram.get_percentile(100.0) as u64, 9999);
    }

    #[test]
    fn test_proc_histogram_uneven_distributed() {
        let registry = Registry::new();
        let buckets = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        let opts = histogram_opts!("test_histogram", "test_histogram", buckets);
        let histogram = register_histogram_with_registry!(opts, registry).unwrap();

        let mut i = 0.005;
        while i < 10.0 {
            histogram.observe(i);
            i += 0.005;
        }

        let histogram = MyHistogram::from_prom_hist(histogram.metric().get_histogram());
        assert_eq!(histogram.get_percentile(50.0), 5.0);
        assert_eq!(histogram.get_percentile(90.0), 9.004004004004004);
        assert_eq!(histogram.get_percentile(99.0), 9.904904904904905);
        assert_eq!(histogram.get_percentile(99.9), 9.994994994994995);
        assert_eq!(histogram.get_percentile(100.0), 10.0);
    }

    #[test]
    fn test_proc_histogram_realistic() {
        let registry = Registry::new();
        let buckets = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        let opts = histogram_opts!("test_histogram", "test_histogram", buckets);
        let histogram = register_histogram_with_registry!(opts, registry).unwrap();

        histogram.observe(0.0012);
        histogram.observe(0.0013);
        histogram.observe(0.003);

        histogram.observe(0.0132);
        histogram.observe(0.0143);
        histogram.observe(0.0146);
        histogram.observe(0.0249);

        histogram.observe(0.99);

        histogram.observe(6.11);
        histogram.observe(7.833);

        let histogram = MyHistogram::from_prom_hist(histogram.metric().get_histogram());
        assert_eq!(histogram.get_percentile(50.0), 0.0175);
        assert_eq!(histogram.get_percentile(90.0), 7.5);
        assert_eq!(histogram.get_percentile(99.0), 10.00);
        assert_eq!(histogram.get_percentile(99.9), 10.00);
        assert_eq!(histogram.get_percentile(100.0), 10.00);
    }
}
