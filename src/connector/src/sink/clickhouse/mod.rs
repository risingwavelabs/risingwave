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

pub const CLICKHOUSE_SINK: &str = "clickhouse";

cfg_if::cfg_if! {
    if #[cfg(feature = "sink-clickhouse")] {
        mod imp;
        pub use imp::{ClickHouseSink, ClickHouseConfig};
    } else {
        use crate::sink::utils::dummy::{FeatureNotEnabledSinkMarker, FeatureNotEnabledSink};
        pub struct ClickHouseNotEnabled;
        impl FeatureNotEnabledSinkMarker for ClickHouseNotEnabled {
            const SINK_NAME: &'static str = CLICKHOUSE_SINK;
        }
        pub type ClickHouseSink = FeatureNotEnabledSink<ClickHouseNotEnabled>;
        pub struct ClickHouseConfig;
    }
}