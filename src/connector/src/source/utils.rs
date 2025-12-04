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

/// Define a source module that is gated by a feature.
///
/// This is to allow some heavy or unpopular source implementations (and their dependencies) to be disabled
/// at compile time, in order to decrease compilation time and binary size.
///
/// When the feature is disabled, this macro generates standalone dummy implementations for the source,
/// which return errors indicating the feature is not enabled. The generated types are concrete structs
/// (not type aliases) to avoid conflicts with macro-generated `TryFrom`/`From` impls in `impl_split`.
#[allow(unused_macros)]
macro_rules! feature_gated_source_mod {
    ($mod_name:ident, $source_name:literal) => {
        crate::source::utils::feature_gated_source_mod!($mod_name, $mod_name, $source_name);
    };
    ($mod_name:ident, $struct_prefix:ident, $source_name:literal) => {
        paste::paste! {
        #[cfg(feature = "source-" $source_name)]
        pub mod $mod_name;

        #[cfg(not(feature = "source-" $source_name))]
        pub mod $mod_name {
            use std::collections::HashMap;

            use anyhow::anyhow;
            use async_trait::async_trait;
            use risingwave_common::types::JsonbVal;
            use serde::{Deserialize, Serialize};

            use crate::error::{ConnectorError, ConnectorResult};
            use crate::parser::ParserConfig;
            use crate::source::{
                BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef,
                SourceProperties, SplitEnumerator, SplitId, SplitMetaData, SplitReader, UnknownFields,
            };
            use crate::with_options::WithOptions;

            pub const [<$source_name:upper _CONNECTOR>]: &'static str = $source_name;

            fn err_feature_not_enabled() -> ConnectorError {
                ConnectorError::from(anyhow!(
                    "Feature `source-{}` is not enabled at compile time. \
                    Please enable it in `Cargo.toml` and rebuild.",
                    $source_name
                ))
            }

            #[doc = "A dummy source properties that always returns an error, as the feature `source-" $source_name "` is currently not enabled."]
            #[derive(Clone, Debug, Deserialize, WithOptions)]
            pub struct [<$struct_prefix:camel Properties>] {
                #[serde(flatten)]
                pub unknown_fields: HashMap<String, String>,
            }

            impl crate::enforce_secret::EnforceSecret for [<$struct_prefix:camel Properties>] {
                const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {};
            }

            impl UnknownFields for [<$struct_prefix:camel Properties>] {
                fn unknown_fields(&self) -> HashMap<String, String> {
                    self.unknown_fields.clone()
                }
            }

            impl SourceProperties for [<$struct_prefix:camel Properties>] {
                type Split = [<$struct_prefix:camel Split>];
                type SplitEnumerator = [<$struct_prefix:camel SplitEnumerator>];
                type SplitReader = [<$struct_prefix:camel SplitReader>];

                const SOURCE_NAME: &'static str = [<$source_name:upper _CONNECTOR>];
            }

            #[doc = "A dummy split that always returns an error, as the feature `source-" $source_name "` is currently not enabled."]
            #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
            pub struct [<$struct_prefix:camel Split>] {
                _private: (),
            }

            impl SplitMetaData for [<$struct_prefix:camel Split>] {
                fn id(&self) -> SplitId {
                    "feature_not_enabled".into()
                }

                fn encode_to_json(&self) -> JsonbVal {
                    serde_json::to_value(self).unwrap().into()
                }

                fn restore_from_json(_value: JsonbVal) -> ConnectorResult<Self> {
                    Err(err_feature_not_enabled())
                }

                fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
                    Err(err_feature_not_enabled())
                }
            }

            #[doc = "A dummy split enumerator that always returns an error, as the feature `source-" $source_name "` is currently not enabled."]
            pub struct [<$struct_prefix:camel SplitEnumerator>];

            #[async_trait]
            impl SplitEnumerator for [<$struct_prefix:camel SplitEnumerator>] {
                type Properties = [<$struct_prefix:camel Properties>];
                type Split = [<$struct_prefix:camel Split>];

                async fn new(
                    _properties: Self::Properties,
                    _context: SourceEnumeratorContextRef,
                ) -> ConnectorResult<Self> {
                    Err(err_feature_not_enabled())
                }

                async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
                    Err(err_feature_not_enabled())
                }
            }

            #[doc = "A dummy split reader that always returns an error, as the feature `source-" $source_name "` is currently not enabled."]
            pub struct [<$struct_prefix:camel SplitReader>];

            #[async_trait]
            impl SplitReader for [<$struct_prefix:camel SplitReader>] {
                type Properties = [<$struct_prefix:camel Properties>];
                type Split = [<$struct_prefix:camel Split>];

                async fn new(
                    _properties: Self::Properties,
                    _splits: Vec<Self::Split>,
                    _parser_config: ParserConfig,
                    _source_ctx: SourceContextRef,
                    _columns: Option<Vec<Column>>,
                ) -> ConnectorResult<Self> {
                    Err(err_feature_not_enabled())
                }

                fn into_stream(self) -> BoxSourceChunkStream {
                    Box::pin(futures::stream::once(async {
                        Err(err_feature_not_enabled())
                    }))
                }
            }
        }
        }
    };
}
pub(super) use feature_gated_source_mod;
