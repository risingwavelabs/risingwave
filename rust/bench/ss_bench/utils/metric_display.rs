use std::fmt::{Display, Formatter};

use itertools::Itertools;

use super::diff_statistics::MyHistogram;

pub(crate) struct MetricDisplay {
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub p100: f64,
}

impl Display for MetricDisplay {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "latency:
        mean: {},
        p50: {},
        p90: {},
        p99: {},
        p100: {};",
            self.mean, self.p50, self.p90, self.p99, self.p100
        )
    }
}

impl MetricDisplay {
    pub(crate) fn get_percentile(hist: &MyHistogram, p: f64) -> f64 {
        let sample_count = hist.total_count;

        // empty bucket may appear
        if sample_count == 0 {
            return 0.0;
        }
        let threshold = (sample_count as f64 * (p / 100.0_f64)).ceil() as u64;
        let mut last_upper_bound = 0.0;
        let mut last_count = 0;
        for (&upper_bound, &count) in hist.upper_bound_list.iter().zip_eq(hist.count_list.iter()) {
            if count >= threshold {
                // assume scale linearly within this bucket,
                // return a value bwtween last_upper_bound and upper_bound
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

    /// calculate latencies statistics
    pub(crate) fn new(histogram: &MyHistogram) -> Self {
        let mean = histogram.total_sum / histogram.total_count as f64;

        Self {
            mean,
            p50: MetricDisplay::get_percentile(histogram, 50.0),
            p90: MetricDisplay::get_percentile(histogram, 90.0),
            p99: MetricDisplay::get_percentile(histogram, 99.0),
            p100: MetricDisplay::get_percentile(histogram, 100.0),
        }
    }
}
