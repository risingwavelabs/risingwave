// Copyright 2026 RisingWave Labs
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

/// The section `[session_init]` in `risingwave.toml`.
///
/// It seeds selected persisted session parameters into the Meta store during **cluster
/// bootstrap only**. The precedence is:
///
/// 1. Persisted value in Meta store (`session_parameter`)
/// 2. Explicit value in `[session_init]`
/// 3. Built-in `SessionConfig::default()`
///
/// Editing `[session_init]` after a cluster has been bootstrapped does not change the effective
/// value of an already-persisted parameter. See the RFC for details.
///
/// Each field uses `Option<String>` so that a parameter omitted from `risingwave.toml` (`None`)
/// can be distinguished from a parameter explicitly configured to `"default"` (`Some("default")`).
/// The value syntax is identical to the SQL/session syntax of the same parameter, e.g. `default`,
/// `adaptive`, `0`, `<n>`, `bounded(<n>)`, `ratio(<r>)`.
#[serde_with::apply(Option => #[serde(with = "none_as_empty_string")])]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct SessionInitConfig {
    #[serde(default)]
    pub streaming_parallelism: Option<String>,

    #[serde(default)]
    pub streaming_parallelism_for_backfill: Option<String>,

    #[serde(default)]
    pub streaming_parallelism_for_table: Option<String>,

    #[serde(default)]
    pub streaming_parallelism_for_source: Option<String>,

    #[serde(default)]
    pub streaming_parallelism_for_sink: Option<String>,

    #[serde(default)]
    pub streaming_parallelism_for_index: Option<String>,

    #[serde(default)]
    pub streaming_parallelism_for_materialized_view: Option<String>,
}

impl SessionInitConfig {
    /// Returns the explicitly-configured `(session parameter entry name, value)` pairs.
    /// Parameters omitted from `[session_init]` are not included.
    pub fn entries(&self) -> Vec<(&'static str, &str)> {
        [
            ("streaming_parallelism", &self.streaming_parallelism),
            (
                "streaming_parallelism_for_backfill",
                &self.streaming_parallelism_for_backfill,
            ),
            (
                "streaming_parallelism_for_table",
                &self.streaming_parallelism_for_table,
            ),
            (
                "streaming_parallelism_for_source",
                &self.streaming_parallelism_for_source,
            ),
            (
                "streaming_parallelism_for_sink",
                &self.streaming_parallelism_for_sink,
            ),
            (
                "streaming_parallelism_for_index",
                &self.streaming_parallelism_for_index,
            ),
            (
                "streaming_parallelism_for_materialized_view",
                &self.streaming_parallelism_for_materialized_view,
            ),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.as_deref().map(|value| (name, value)))
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_init_entries_distinguishes_omitted_from_default() {
        let config: RwConfig = toml::from_str(
            r#"
            [session_init]
            streaming_parallelism = "bounded(8)"
            streaming_parallelism_for_table = "default"
            "#,
        )
        .unwrap();

        // Omitted fields are `None`; an explicit `default` is `Some("default")`.
        assert_eq!(
            config.session_init.streaming_parallelism.as_deref(),
            Some("bounded(8)")
        );
        assert_eq!(
            config
                .session_init
                .streaming_parallelism_for_table
                .as_deref(),
            Some("default")
        );
        assert_eq!(config.session_init.streaming_parallelism_for_sink, None);

        // Only explicitly-configured parameters are reported.
        assert_eq!(
            config.session_init.entries(),
            vec![
                ("streaming_parallelism", "bounded(8)"),
                ("streaming_parallelism_for_table", "default"),
            ]
        );
    }

    #[test]
    fn test_session_init_default_is_empty() {
        assert!(SessionInitConfig::default().entries().is_empty());
    }
}
