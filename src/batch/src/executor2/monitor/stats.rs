// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_with_registry, Histogram, Registry,
};

pub struct BatchMetrics {
    pub row_seq_scan_next_duration: Histogram,
}

impl BatchMetrics {
    pub fn new(registry: Registry) -> Self {
        let opts = histogram_opts!(
            "batch_row_seq_scan_next_duration",
            "Time spent deserializing into a row in cell based table.",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let row_seq_scan_next_duration = register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            row_seq_scan_next_duration,
        }
    }

    /// Create a new `BatchMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(prometheus::Registry::new())
    }
}
