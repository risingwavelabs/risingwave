// Copyright 2023 RisingWave Labs
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

use std::sync::LazyLock;
use std::time::Duration;

/// The target name for the root span while pgwire is handling a query.
pub const PGWIRE_ROOT_SPAN_TARGET: &str = "pgwire_root_span";
/// The target name for the event when pgwire finishes handling a query.
pub const PGWIRE_QUERY_LOG: &str = "pgwire_query_log";
/// The target name for the event when pgwire does not finish handling a query in time.
pub const PGWIRE_SLOW_QUERY_LOG: &str = "pgwire_slow_query_log";

/// The period of logging ongoing slow queries.
pub static SLOW_QUERY_LOG_PERIOD: LazyLock<Duration> = LazyLock::new(|| {
    std::env::var("RW_SLOW_QUERY_LOG_PERIOD_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(60))
});
