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

use super::*;

/// Unrecognized fields in a config section. Generic over the config section type to provide better
/// error messages.
///
/// The current implementation will log warnings if there are unrecognized fields.
#[derive(Educe)]
#[educe(Clone, Default)]
pub struct Unrecognized<T: 'static> {
    inner: BTreeMap<String, Value>,
    _marker: std::marker::PhantomData<&'static T>,
}

impl<T> std::fmt::Debug for Unrecognized<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Unrecognized<T> {
    /// Returns all unrecognized fields as a map.
    pub fn into_inner(self) -> BTreeMap<String, Value> {
        self.inner
    }
}

impl<'de, T> Deserialize<'de> for Unrecognized<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = BTreeMap::deserialize(deserializer)?;
        if !inner.is_empty() {
            tracing::warn!(
                "unrecognized fields in `{}`: {:?}",
                std::any::type_name::<T>(),
                inner.keys()
            );
        }
        Ok(Unrecognized {
            inner,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<T> Serialize for Unrecognized<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

pub fn load_config(path: &str, cli_override: impl OverrideConfig) -> RwConfig
where
{
    let mut config = if path.is_empty() {
        tracing::warn!("risingwave.toml not found, using default config.");
        RwConfig::default()
    } else {
        let config_str = fs::read_to_string(path)
            .with_context(|| format!("failed to open config file at `{path}`"))
            .unwrap();
        toml::from_str(config_str.as_str())
            .context("failed to parse config file")
            .unwrap()
    };
    cli_override.r#override(&mut config);
    config
}

pub trait OverrideConfig {
    fn r#override(&self, config: &mut RwConfig);
}

impl<T: OverrideConfig> OverrideConfig for &T {
    fn r#override(&self, config: &mut RwConfig) {
        T::r#override(self, config)
    }
}

/// For non-user-facing components where the CLI arguments do not override the config file.
#[derive(Clone, Copy)]
pub struct NoOverride;

impl OverrideConfig for NoOverride {
    fn r#override(&self, _config: &mut RwConfig) {}
}
