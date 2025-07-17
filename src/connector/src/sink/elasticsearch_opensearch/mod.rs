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

pub mod elasticsearch_converter;
pub mod elasticsearch_opensearch_client;
pub mod elasticsearch_opensearch_config;
pub mod elasticsearch_opensearch_formatter;

pub const ES_SINK: &str = "elasticsearch";
pub const OPENSEARCH_SINK: &str = "opensearch";

cfg_if::cfg_if! {
    if #[cfg(feature = "sink-elasticsearch")] {
        pub mod elasticsearch;
        pub use elasticsearch::ElasticSearchSink;
    } else {
        use crate::sink::utils::dummy::{FeatureNotEnabledSinkMarker, FeatureNotEnabledSink};
        pub struct ElasticSearchNotEnabled;
        impl FeatureNotEnabledSinkMarker for ElasticSearchNotEnabled {
            const SINK_NAME: &'static str = ES_SINK;
        }
        pub type ElasticSearchSink = FeatureNotEnabledSink<ElasticSearchNotEnabled>;
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "sink-opensearch")] {
        pub mod opensearch;
        pub use opensearch::OpenSearchSink;
    } else {
        use crate::sink::utils::dummy::{FeatureNotEnabledSinkMarker, FeatureNotEnabledSink};
        pub struct OpenSearchNotEnabled;
        impl FeatureNotEnabledSinkMarker for OpenSearchNotEnabled {
            const SINK_NAME: &'static str = OPENSEARCH_SINK;
        }
        pub type OpenSearchSink = FeatureNotEnabledSink<OpenSearchNotEnabled>;
    }
}
