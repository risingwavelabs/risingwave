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

pub const MONGODB_SINK: &str = "mongodb";

cfg_if::cfg_if! {
    if #[cfg(feature = "sink-mongodb")] {
        mod imp;
        pub use imp::{MongodbSink, MongodbConfig};
    } else {
        use crate::sink::utils::dummy::{FeatureNotEnabledSinkMarker, FeatureNotEnabledSink};
        pub struct MongodbNotEnabled;
        impl FeatureNotEnabledSinkMarker for MongodbNotEnabled {
            const SINK_NAME: &'static str = MONGODB_SINK;
        }
        pub type MongodbSink = FeatureNotEnabledSink<MongodbNotEnabled>;
        pub struct MongodbConfig;
    }
}