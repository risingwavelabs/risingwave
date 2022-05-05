// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod config;
pub mod enumerator;
pub mod source;
pub mod split;

pub use enumerator::*;
pub use split::*;

const NEXMARK_CONFIG_SPLIT_NUM: &str = "nexmark.split.num";
/// The total event count of Bid + Auction + Person
const NEXMARK_CONFIG_EVENT_NUM: &str = "nexmark.event.num";
const NEXMARK_CONFIG_TABLE_TYPE: &str = "nexmark.table.type";
const NEXMARK_CONFIG_MAX_CHUNK_SIZE: &str = "nexmark.max.chunk.size";
/// The event time gap will be like the time gap in the generated data, default false
const NEXMARK_CONFIG_USE_REAL_TIME: &str = "nexmark.use.real.time";
/// Minimal gap between two events, default 100000, so that the default max throughput is 10000
const NEXMARK_CONFIG_MIN_EVENT_GAP_IN_NS: &str = "nexmark.min.event.gap.in.ns";
/// Base time unit for the `Nexmark` benchmark.
const NEXMARK_BASE_TIME: usize = 1_436_918_400_000;
const NEXMARK_MAX_FETCH_MESSAGES: u64 = 1024;
