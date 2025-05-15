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

use std::collections::BTreeMap;

use rand::Rng;
use serde::Deserialize;
#[derive(Clone, Debug, Deserialize)]
pub struct Configuration {
    pub config: BTreeMap<String, Status>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Status {
    pub weight: u8,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

impl Configuration {
    pub fn new(path: &str) -> Configuration {
        let data = std::fs::read_to_string(path).unwrap();
        let config: Configuration = serde_yaml::from_str(&data).unwrap();
        config
    }

    /// Returns true if the feature is enabled and passes the random check.
    /// If the feature is not configured, defaults to 50% chance.
    pub fn should_generate<R: Rng>(&self, feature: &str, rng: &mut R) -> bool {
        match self.config.get(feature) {
            Some(status) if status.enabled => rng.random_range(0..100) < status.weight,
            Some(_) => false,
            None => rng.random_bool(0.5),
        }
    }

    pub fn set_weight(&mut self, feature: &str, weight: u8) {
        self.config
            .entry(feature.to_string())
            .or_insert_with(|| Status {
                weight: 0,
                enabled: true,
            })
            .weight = weight;
    }

    pub fn set_enabled(&mut self, feature: &str, enabled: bool) {
        self.config
            .entry(feature.to_string())
            .or_insert_with(|| Status {
                weight: 0,
                enabled: true,
            })
            .enabled = enabled;
    }
}
