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
pub enum Syntax {
    Where,
    Agg,
    Join,
}

impl fmt::Display for Syntax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Syntax::Where => "where",
            Syntax::Agg => "agg",
            Syntax::Join => "join",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    Eowc,
    NaturalJoin,
    UsingJoin,
    Except,
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Feature::Eowc => "eowc",
            Feature::NaturalJoin => "natural join",
            Feature::UsingJoin => "using join",
            Feature::Except => "except",
        };
        write!(f, "{}", s)
    }
}

impl From<Syntax> for GenerateItem {
    fn from(s: Syntax) -> Self {
        GenerateItem::Syntax(s)
    }
}

impl From<Feature> for GenerateItem {
    fn from(f: Feature) -> Self {
        GenerateItem::Feature(f)
    }
}

/// Unified abstraction for syntax and feature
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GenerateItem {
    Syntax(Syntax),
    Feature(Feature),
}

#[derive(Clone, Debug, Deserialize)]
pub struct Configuration {
    pub weight: BTreeMap<Syntax, u8>,

    #[serde(default)]
    pub feature: BTreeMap<Feature, bool>,
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

        for (syntax, weight) in &config.weight {
            if *weight > 100 {
                panic!(
                    "Invalid weight {} for syntax '{}': must be in [0, 100]",
                    weight, syntax
                );
            }
        }

        config
    }

    /// Decide whether to generate a syntax or enable a feature.
    pub fn should_generate<R, T>(&self, item: T, rng: &mut R) -> bool
    where
        R: Rng,
        T: Into<GenerateItem>,
    {
        match item.into() {
            GenerateItem::Syntax(syntax) => {
                let weight = self.weight.get(&syntax).cloned().unwrap_or(50);
                rng.random_range(0..100) < weight
            }
            GenerateItem::Feature(feature) => *self.feature.get(&feature).unwrap_or(&false),
        }
    }

    /// Dynamically update syntax weight.
    pub fn set_weight(&mut self, syntax: Syntax, weight: u8) {
        if weight > 100 {
            panic!("Invalid weight {}: must be in [0, 100]", weight);
        }

        self.weight.insert(syntax, weight);
    }

    /// Dynamically enable/disable a feature.
    pub fn set_enabled(&mut self, feature: Feature, enabled: bool) {
        self.feature.insert(feature, enabled);
    }

    /// Enable features from command-line `--enable` arguments
    pub fn enable_features_from_args(&mut self, features: &[String]) {
        for feat in features {
            let parsed = match feat.as_str() {
                "eowc" => Feature::Eowc,
                "natural_join" => Feature::NaturalJoin,
                "using_join" => Feature::UsingJoin,
                "except" => Feature::Except,
                _ => panic!("Unknown feature: {}", feat),
            };
            self.set_enabled(parsed, true);
        }
    }
}
