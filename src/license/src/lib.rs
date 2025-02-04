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

#![feature(let_chains)]

mod cpu;
mod feature;
mod key;
mod manager;

pub use feature::*;
pub use key::*;
pub use manager::*;
use risingwave_pb::telemetry::PbTelemetryEventStage;
use risingwave_telemetry_event::report_event_common;

pub(crate) fn report_telemetry(feature: &Feature, feature_name: &str, success_flag: bool) {
    if !matches!(feature, Feature::TestPaid) {
        let mut attr_builder = jsonbb::Builder::<Vec<u8>>::new();
        attr_builder.begin_object();
        attr_builder.add_string("success");
        attr_builder.add_value(jsonbb::ValueRef::Bool(success_flag));
        attr_builder.end_object();
        let attr = attr_builder.finish();

        report_event_common(
            PbTelemetryEventStage::Unspecified,
            feature_name,
            0,
            None,
            None,
            Some(attr),
            "paywall".to_owned(),
        );
    }
}
