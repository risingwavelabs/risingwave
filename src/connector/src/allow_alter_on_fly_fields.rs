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

// THIS FILE IS AUTO_GENERATED. DO NOT EDIT
// UPDATE WITH: ./risedev generate-with-options

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

macro_rules! use_source_properties {
    ({ $({ $variant_name:ident, $prop_name:ty, $split:ty }),* }) => {
        $(
            #[allow(unused_imports)]
            pub(super) use $prop_name;
        )*
    };
}

mod source_properties {
    use crate::for_all_sources;

    for_all_sources!(use_source_properties);
}

mod sink_properties {
    use crate::use_all_sink_configs;

    use_all_sink_configs!();
}

/// Map of source connector names to their changeable field names
pub static SOURCE_CHANGEABLE_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {
    use source_properties::*;
    let mut map = HashMap::new();
    // KafkaProperties
    map.try_insert(
        std::any::type_name::<KafkaProperties>().to_owned(),
        [
            "properties.sync.call.timeout".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
});

/// Map of sink connector names to their changeable field names
pub static SINK_CHANGEABLE_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    // KafkaConfig
    map.try_insert(
        std::any::type_name::<KafkaConfig>().to_owned(),
        [
            "properties.sync.call.timeout".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
});

/// Get all source connector names that have changeable fields
pub fn get_source_connectors_with_changeable_fields() -> Vec<&'static str> {
    SOURCE_CHANGEABLE_FIELDS.keys().map(|s| s.as_str()).collect()
}

/// Get all sink connector names that have changeable fields
pub fn get_sink_connectors_with_changeable_fields() -> Vec<&'static str> {
    SINK_CHANGEABLE_FIELDS.keys().map(|s| s.as_str()).collect()
}

