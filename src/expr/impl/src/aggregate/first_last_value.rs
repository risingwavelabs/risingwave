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

use risingwave_expr::aggregate;

/// Note that different from `min` and `max`, `first_value` doesn't ignore `NULL` values.
///
/// ```slt
/// statement ok
/// create table t(v1 int, ts int);
///
/// statement ok
/// insert into t values (null, 1), (2, 2), (null, 3);
///
/// query I
/// select first_value(v1 order by ts) from t;
/// ----
/// NULL
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("first_value(*) -> auto", state = "ref", shortcurcuit_if = "true" /* always shortcurcuit */)]
fn first_value<T>(_: Option<T>, input: Option<T>) -> Option<T> {
    input // always shortcurcuit immediately, so the output is always the first value
}

/// Note that different from `min` and `max`, `last_value` doesn't ignore `NULL` values.
///
/// ```slt
/// statement ok
/// create table t(v1 int, ts int);
///
/// statement ok
/// insert into t values (null, 1), (2, 2), (null, 3);
///
/// query I
/// select last_value(v1 order by ts) from t;
/// ----
/// NULL
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("last_value(*) -> auto", state = "ref")] // TODO(): `last_value(any) -> any`
fn last_value<T>(_: Option<T>, input: Option<T>) -> Option<T> {
    input
}

#[aggregate("internal_last_seen_value(*) -> auto", state = "ref", internal)]
fn internal_last_seen_value<T>(state: T, input: T, retract: bool) -> T {
    if retract {
        state
    } else {
        input
    }
}
