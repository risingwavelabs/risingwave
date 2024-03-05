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

use std::collections::HashMap;

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
impl WithOptions for crate::common::QualityOfService {}
impl WithOptions for crate::sink::kafka::CompressionCodec {}
impl WithOptions for nexmark::config::RateShape {}
impl WithOptions for nexmark::event::EventType {}
