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

/// Dummy trait implementation for a source when the feature is not enabled at compile time.
pub(crate) mod dummy {
    use std::collections::HashMap;

    use anyhow::anyhow;
    use async_trait::async_trait;
    use risingwave_common::types::JsonbVal;
    use serde::{Deserialize, Serialize};

    use crate::error::{ConnectorError, ConnectorResult};
    use crate::parser::ParserConfig;
    use crate::source::base::{
        SourceProperties, SplitId, SplitMetaData, SplitReader, UnknownFields,
    };
    use crate::source::{BoxSourceChunkStream, SourceContextRef, SplitEnumerator, SplitImpl};
    use crate::with_options::WithOptions;

    #[allow(dead_code)]
    pub fn err_feature_not_enabled(source_name: &'static str) -> ConnectorError {
        ConnectorError::from(anyhow!(
            "RisingWave is not compiled with feature `source-{}`",
            source_name
        ))
    }

    /// Implement this trait will bring a dummy source implementation that always returns an error.
    pub trait FeatureNotEnabledSourceMarker:
        Send + Sync + Clone + std::fmt::Debug + 'static
    {
        #[allow(dead_code)]
        const SOURCE_NAME: &'static str;
    }

    /// A dummy split that always returns an error.
    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
    pub struct FeatureNotEnabledSplit<S: FeatureNotEnabledSourceMarker> {
        #[serde(skip)]
        _marker: std::marker::PhantomData<S>,
    }

    impl<S: FeatureNotEnabledSourceMarker> SplitMetaData for FeatureNotEnabledSplit<S> {
        fn id(&self) -> SplitId {
            "feature_not_enabled".into()
        }

        fn encode_to_json(&self) -> JsonbVal {
            serde_json::to_value(self).unwrap().into()
        }

        fn restore_from_json(_value: JsonbVal) -> crate::error::ConnectorResult<Self> {
            Err(err_feature_not_enabled(S::SOURCE_NAME))
        }

        fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
            Err(err_feature_not_enabled(S::SOURCE_NAME))
        }
    }

    impl<S: FeatureNotEnabledSourceMarker> TryFrom<SplitImpl> for FeatureNotEnabledSplit<S> {
        type Error = ConnectorError;

        fn try_from(_value: SplitImpl) -> Result<Self, Self::Error> {
            Err(err_feature_not_enabled(S::SOURCE_NAME))
        }
    }

    impl<S: FeatureNotEnabledSourceMarker> From<FeatureNotEnabledSplit<S>> for SplitImpl {
        fn from(_: FeatureNotEnabledSplit<S>) -> Self {
            unreachable!("FeatureNotEnabledSplit should never be converted to SplitImpl")
        }
    }

    /// A dummy split enumerator that always returns an error.
    #[allow(dead_code)]
    pub struct FeatureNotEnabledSplitEnumerator<S: FeatureNotEnabledSourceMarker>(
        std::marker::PhantomData<S>,
    );

    #[async_trait]
    impl<S: FeatureNotEnabledSourceMarker> SplitEnumerator for FeatureNotEnabledSplitEnumerator<S> {
        type Properties = FeatureNotEnabledProperties<S>;
        type Split = FeatureNotEnabledSplit<S>;

        async fn new(
            _properties: Self::Properties,
            _context: crate::source::SourceEnumeratorContextRef,
        ) -> ConnectorResult<Self> {
            Err(err_feature_not_enabled(S::SOURCE_NAME))
        }

        async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
            Err(err_feature_not_enabled(S::SOURCE_NAME))
        }
    }

    /// A dummy split reader that always returns an error.
    #[allow(dead_code)]
    pub struct FeatureNotEnabledSplitReader<S: FeatureNotEnabledSourceMarker>(
        std::marker::PhantomData<S>,
    );

    #[async_trait]
    impl<S: FeatureNotEnabledSourceMarker> SplitReader for FeatureNotEnabledSplitReader<S> {
        type Properties = FeatureNotEnabledProperties<S>;
        type Split = FeatureNotEnabledSplit<S>;

        async fn new(
            _properties: Self::Properties,
            _state: Vec<Self::Split>,
            _parser_config: ParserConfig,
            _source_ctx: SourceContextRef,
            _columns: Option<Vec<crate::source::Column>>,
        ) -> ConnectorResult<Self> {
            Err(err_feature_not_enabled(S::SOURCE_NAME))
        }

        fn into_stream(self) -> BoxSourceChunkStream {
            Box::pin(futures::stream::once(async {
                Err(err_feature_not_enabled(S::SOURCE_NAME))
            }))
        }
    }

    /// A dummy source properties that always returns an error.
    #[allow(dead_code)]
    #[derive(Clone, Debug, Deserialize, PartialEq)]
    pub struct FeatureNotEnabledProperties<S: FeatureNotEnabledSourceMarker> {
        #[serde(skip)]
        _marker: std::marker::PhantomData<S>,
        #[serde(flatten)]
        pub unknown_fields: HashMap<String, String>,
    }

    impl<S: FeatureNotEnabledSourceMarker> Default for FeatureNotEnabledProperties<S> {
        fn default() -> Self {
            Self {
                _marker: std::marker::PhantomData,
                unknown_fields: HashMap::new(),
            }
        }
    }

    impl<S: FeatureNotEnabledSourceMarker> UnknownFields for FeatureNotEnabledProperties<S> {
        fn unknown_fields(&self) -> HashMap<String, String> {
            self.unknown_fields.clone()
        }
    }

    impl<S: FeatureNotEnabledSourceMarker> WithOptions for FeatureNotEnabledProperties<S> {}

    impl<S: FeatureNotEnabledSourceMarker> crate::enforce_secret::EnforceSecret
        for FeatureNotEnabledProperties<S>
    {
        const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {};
    }

    impl<S: FeatureNotEnabledSourceMarker + Clone + std::fmt::Debug> SourceProperties
        for FeatureNotEnabledProperties<S>
    {
        type Split = FeatureNotEnabledSplit<S>;
        type SplitEnumerator = FeatureNotEnabledSplitEnumerator<S>;
        type SplitReader = FeatureNotEnabledSplitReader<S>;

        const SOURCE_NAME: &'static str = S::SOURCE_NAME;
    }
}

/// Define a source module that is gated by a feature.
///
/// This is to allow some heavy or unpopular source implementations (and their dependencies) to be disabled
/// at compile time, in order to decrease compilation time and binary size.
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
            use crate::source::utils::dummy::{
                FeatureNotEnabledSourceMarker, FeatureNotEnabledProperties,
                FeatureNotEnabledSplit, FeatureNotEnabledSplitEnumerator, FeatureNotEnabledSplitReader,
            };
            pub struct [<$struct_prefix:camel NotEnabled>];
            pub const [<$source_name:upper _CONNECTOR>]: &'static str = $source_name;
            impl FeatureNotEnabledSourceMarker for [<$struct_prefix:camel NotEnabled>] {
                const SOURCE_NAME: &'static str = [<$source_name:upper _CONNECTOR>];
            }
            #[doc = "A dummy source properties that always returns an error, as the feature `source-" $source_name "` is currently not enabled."]
            pub type [<$struct_prefix:camel Properties>] = FeatureNotEnabledProperties<[<$struct_prefix:camel NotEnabled>]>;
            #[doc = "A dummy split that always returns an error, as the feature `source-" $source_name "` is currently not enabled."]
            pub type [<$struct_prefix:camel Split>] = FeatureNotEnabledSplit<[<$struct_prefix:camel NotEnabled>]>;
            #[doc = "A dummy split enumerator that always returns an error, as the feature `source-" $source_name "` is currently not enabled."]
            pub type [<$struct_prefix:camel SplitEnumerator>] = FeatureNotEnabledSplitEnumerator<[<$struct_prefix:camel NotEnabled>]>;
            #[doc = "A dummy split reader that always returns an error, as the feature `source-" $source_name "` is currently not enabled."]
            pub type [<$struct_prefix:camel SplitReader>] = FeatureNotEnabledSplitReader<[<$struct_prefix:camel NotEnabled>]>;
        }
        }
    };
}
pub(super) use feature_gated_source_mod;
