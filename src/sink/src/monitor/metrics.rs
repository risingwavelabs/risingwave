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

use prometheus::Registry;
#[derive(Debug)]
pub struct SinkMetrics {
    pub registry: Registry,
}

impl SinkMetrics {
    pub fn new(registry: Registry) -> Self {
        SinkMetrics { registry }
    }

    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}

impl Default for SinkMetrics {
    fn default() -> Self {
        SinkMetrics::new(Registry::new())
    }
}
