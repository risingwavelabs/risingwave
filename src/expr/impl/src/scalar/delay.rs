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

use std::time::Duration;

use risingwave_common::types::{F64, Interval};
use risingwave_expr::function;

/// Makes the current session's process sleep until the given number of seconds have elapsed.
///
/// ```slt
/// query I
/// SELECT pg_sleep(1.5);
/// ----
/// NULL
/// ```
#[function("pg_sleep(float8)", volatile)]
async fn pg_sleep(second: F64) {
    tokio::time::sleep(Duration::from_secs_f64(second.0)).await;
}

/// Makes the current session's process sleep until the given interval has elapsed.
///
/// ```slt
/// query I
/// SELECT pg_sleep_for('1 second');
/// ----
/// NULL
/// ```
#[function("pg_sleep_for(interval)", volatile)]
async fn pg_sleep_for(interval: Interval) {
    // we only use the microsecond part of the interval
    let usecs = if interval.is_positive() {
        interval.usecs() as u64
    } else {
        // return if the interval is not positive
        return;
    };
    let duration = Duration::from_micros(usecs);
    tokio::time::sleep(duration).await;
}
