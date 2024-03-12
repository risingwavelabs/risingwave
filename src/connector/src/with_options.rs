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

use std::collections::{BTreeMap, HashMap};

use crate::source::iceberg::ICEBERG_CONNECTOR;
use crate::source::{
    GCS_CONNECTOR, KAFKA_CONNECTOR, OPENDAL_S3_CONNECTOR, POSIX_FS_CONNECTOR, UPSTREAM_SOURCE_KEY,
};

/// Marker trait for `WITH` options. Only for `#[derive(WithOptions)]`, should not be used manually.
///
/// This is used to ensure the `WITH` options types have reasonable structure.
///
/// TODO: add this bound for sink. There's a `SourceProperties` trait for sources, but no similar
/// things for sinks.
pub trait WithOptions {
    #[doc(hidden)]
    #[inline(always)]
    fn assert_receiver_is_with_options(&self) {}
}

// Currently CDC properties are handled specially.
// - It simply passes HashMap to Java DBZ.
// - It's not handled by serde.
// - It contains fields other than WITH options.
// TODO: remove the workaround here. And also use #[derive] for it.

impl<T: crate::source::cdc::CdcSourceTypeTrait> WithOptions
    for crate::source::cdc::CdcProperties<T>
{
}

// impl the trait for value types

impl<T: WithOptions> WithOptions for Option<T> {}
impl WithOptions for Vec<String> {}
impl WithOptions for HashMap<String, String> {}

impl WithOptions for String {}
impl WithOptions for bool {}
impl WithOptions for usize {}
impl WithOptions for u32 {}
impl WithOptions for u64 {}
impl WithOptions for i32 {}
impl WithOptions for i64 {}
impl WithOptions for f64 {}
impl WithOptions for std::time::Duration {}
impl WithOptions for crate::sink::kafka::CompressionCodec {}
impl WithOptions for nexmark::config::RateShape {}
impl WithOptions for nexmark::event::EventType {}

pub trait Get {
    fn get(&self, key: &str) -> Option<&String>;
}

impl Get for HashMap<String, String> {
    fn get(&self, key: &str) -> Option<&String> {
        self.get(key)
    }
}

impl Get for BTreeMap<String, String> {
    fn get(&self, key: &str) -> Option<&String> {
        self.get(key)
    }
}

/// Utility methods for `WITH` properties (`HashMap` and `BTreeMap`).
pub trait WithPropertiesExt: Get {
    #[inline(always)]
    fn get_connector(&self) -> Option<String> {
        self.get(UPSTREAM_SOURCE_KEY).map(|s| s.to_lowercase())
    }

    #[inline(always)]
    fn is_kafka_connector(&self) -> bool {
        let Some(connector) = self.get_connector() else {
            return false;
        };
        connector == KAFKA_CONNECTOR
    }

    #[inline(always)]
    fn is_cdc_connector(&self) -> bool {
        let Some(connector) = self.get_connector() else {
            return false;
        };
        connector.contains("-cdc")
    }

    #[inline(always)]
    fn is_iceberg_connector(&self) -> bool {
        let Some(connector) = self.get_connector() else {
            return false;
        };
        connector == ICEBERG_CONNECTOR
    }

    fn connector_need_pk(&self) -> bool {
        // Currently only iceberg connector doesn't need primary key
        !self.is_iceberg_connector()
    }

    fn is_new_fs_connector(&self) -> bool {
        self.get(UPSTREAM_SOURCE_KEY)
            .map(|s| {
                s.eq_ignore_ascii_case(OPENDAL_S3_CONNECTOR)
                    || s.eq_ignore_ascii_case(POSIX_FS_CONNECTOR)
                    || s.eq_ignore_ascii_case(GCS_CONNECTOR)
            })
            .unwrap_or(false)
    }
}

impl<T: Get> WithPropertiesExt for T {}
