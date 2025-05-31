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
use std::fmt;

use rand::Rng;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Ord, PartialOrd, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    Where,
    Agg,
    Join,
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Feature::Where => "where",
            Feature::Agg => "agg",
            Feature::Join => "join",
        };
        write!(f, "{}", s)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Configuration {
    pub config: BTreeMap<Feature, Status>,
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

impl Default for Configuration {
    fn default() -> Self {
        Self::new("config.yml")
    }
}

impl Configuration {
    pub fn new(path: &str) -> Configuration {
        let data = std::fs::read_to_string(path).unwrap();
        let config: Configuration = serde_yaml::from_str(&data).unwrap();

        for (feature, status) in &config.config {
            if status.weight > 100 {
                panic!(
                    "Invalid weight {} for feature '{}': weight must be in [0, 100]",
                    status.weight, feature
                );
            }
        }

        config
    }

    /// Returns true if the feature is enabled and passes the random check.
    /// If the feature is not configured, defaults to 50% chance.
    pub fn should_generate<R: Rng>(&self, feature: Feature, rng: &mut R) -> bool {
        match self.config.get(&feature) {
            Some(status) if status.enabled => rng.random_range(0..100) < status.weight,
            Some(_) => false,
            None => rng.random_bool(0.5),
        }
    }

    pub fn enable_generate(&self, feature: Feature) -> bool {
        self.config
            .get(&feature)
            .map(|status| status.enabled)
            .unwrap_or(true)
    }

    pub fn set_weight(&mut self, feature: Feature, weight: u8) {
        self.config
            .entry(feature)
            .or_insert_with(|| Status {
                weight: 0,
                enabled: true,
            })
            .weight = weight;
    }

    pub fn set_enabled(&mut self, feature: Feature, enabled: bool) {
        self.config
            .entry(feature)
            .or_insert_with(|| Status {
                weight: 0,
                enabled: true,
            })
            .enabled = enabled;
    }
}
