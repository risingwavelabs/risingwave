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

//! [`SessionInitConfig`] is used to initialize selected session parameters persisted in Meta store.

use std::collections::HashMap;

use risingwave_common_proc_macro::ConfigDoc;
use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;
use serde_with::{DisplayFromStr, serde_as};

use super::Unrecognized;
use crate::session_config::parallelism::{ConfigBackfillParallelism, ConfigParallelism};
use crate::session_config::{SessionConfig, SessionConfigError};

pub type ExplicitSessionInitParams = HashMap<String, String>;

/// The section `[session_init]` in `risingwave.toml`.
///
/// Values in this section are only used to initialize persisted session parameters in Meta store.
/// After the parameters are persisted, later changes to this section will not override the stored
/// values on existing clusters.
#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct SessionInitConfig {
    /// Initial value for the persisted session parameter `streaming_parallelism`.
    /// This setting is only used while initializing the cluster-level default session parameters in
    /// Meta store and is ignored on existing clusters after the parameter is persisted.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub streaming_parallelism: Option<ConfigParallelism>,

    /// Initial value for the persisted session parameter `streaming_parallelism_for_backfill`.
    /// This setting is only used while initializing the cluster-level default session parameters in
    /// Meta store and is ignored on existing clusters after the parameter is persisted.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub streaming_parallelism_for_backfill: Option<ConfigBackfillParallelism>,

    /// Initial value for the persisted session parameter `streaming_parallelism_for_table`.
    /// This setting is only used while initializing the cluster-level default session parameters in
    /// Meta store and is ignored on existing clusters after the parameter is persisted.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub streaming_parallelism_for_table: Option<ConfigParallelism>,

    /// Initial value for the persisted session parameter `streaming_parallelism_for_sink`.
    /// This setting is only used while initializing the cluster-level default session parameters in
    /// Meta store and is ignored on existing clusters after the parameter is persisted.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub streaming_parallelism_for_sink: Option<ConfigParallelism>,

    /// Initial value for the persisted session parameter `streaming_parallelism_for_index`.
    /// This setting is only used while initializing the cluster-level default session parameters in
    /// Meta store and is ignored on existing clusters after the parameter is persisted.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub streaming_parallelism_for_index: Option<ConfigParallelism>,

    /// Initial value for the persisted session parameter `streaming_parallelism_for_source`.
    /// This setting is only used while initializing the cluster-level default session parameters in
    /// Meta store and is ignored on existing clusters after the parameter is persisted.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub streaming_parallelism_for_source: Option<ConfigParallelism>,

    /// Initial value for the persisted session parameter
    /// `streaming_parallelism_for_materialized_view`.
    /// This setting is only used while initializing the cluster-level default session parameters in
    /// Meta store and is ignored on existing clusters after the parameter is persisted.
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub streaming_parallelism_for_materialized_view: Option<ConfigParallelism>,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

impl SessionInitConfig {
    pub fn into_init_session_config(
        self,
    ) -> Result<(SessionConfig, ExplicitSessionInitParams), SessionConfigError> {
        let mut session_config = SessionConfig::default();
        let mut explicit_params = HashMap::new();
        let reporter = &mut ();

        macro_rules! apply_if_some {
            ($field:expr, $name:literal) => {
                if let Some(value) = $field {
                    let value = value.to_string();
                    session_config.set($name, value.clone(), reporter)?;
                    explicit_params.insert($name.to_owned(), value);
                }
            };
        }

        apply_if_some!(self.streaming_parallelism, "streaming_parallelism");
        apply_if_some!(
            self.streaming_parallelism_for_backfill,
            "streaming_parallelism_for_backfill"
        );
        apply_if_some!(
            self.streaming_parallelism_for_table,
            "streaming_parallelism_for_table"
        );
        apply_if_some!(
            self.streaming_parallelism_for_sink,
            "streaming_parallelism_for_sink"
        );
        apply_if_some!(
            self.streaming_parallelism_for_index,
            "streaming_parallelism_for_index"
        );
        apply_if_some!(
            self.streaming_parallelism_for_source,
            "streaming_parallelism_for_source"
        );
        apply_if_some!(
            self.streaming_parallelism_for_materialized_view,
            "streaming_parallelism_for_materialized_view"
        );

        Ok((session_config, explicit_params))
    }

    pub fn for_docs() -> Self {
        Self {
            streaming_parallelism: Some(ConfigParallelism::Default),
            streaming_parallelism_for_backfill: Some(ConfigBackfillParallelism::Default),
            streaming_parallelism_for_table: Some(ConfigParallelism::Default),
            streaming_parallelism_for_sink: Some(ConfigParallelism::Default),
            streaming_parallelism_for_index: Some(ConfigParallelism::Default),
            streaming_parallelism_for_source: Some(ConfigParallelism::Default),
            streaming_parallelism_for_materialized_view: Some(ConfigParallelism::Default),
            unrecognized: Default::default(),
        }
    }
}
