// Copyright 2024 RisingWave Labs
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

use prometheus::core::{AtomicU64, GenericGaugeVec};

pub type UintGaugeVec = GenericGaugeVec<AtomicU64>;

#[macro_export]
macro_rules! register_gauge_vec {
    ($TYPE:ident, $OPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let gauge_vec = $TYPE::new($OPTS, $LABELS_NAMES).unwrap();
        $REGISTRY
            .register(Box::new(gauge_vec.clone()))
            .map(|_| gauge_vec)
    }};
}

#[macro_export]
macro_rules! register_uint_gauge_vec_with_registry {
    ($OPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        use $crate::UintGaugeVec;
        $crate::register_gauge_vec!(UintGaugeVec, $OPTS, $LABELS_NAMES, $REGISTRY)
    }};

    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        register_uint_gauge_vec_with_registry!(
            prometheus::opts!($NAME, $HELP),
            $LABELS_NAMES,
            $REGISTRY
        )
    }};
}
