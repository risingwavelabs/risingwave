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

use risingwave_common::types::{Datum, ScalarRefImpl};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate;
use risingwave_expr::aggregate::AggStateDyn;

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
#[aggregate("first_value(any) -> any")]
fn first_value(state: &mut FirstValueState, input: Option<ScalarRefImpl<'_>>) {
    if state.0.is_none() {
        state.0 = Some(input.map(|x| x.into_scalar_impl()));
    }
}

#[derive(Debug, Clone, Default, EstimateSize)]
struct FirstValueState(Option<Datum>);

impl AggStateDyn for FirstValueState {}

impl From<&FirstValueState> for Datum {
    fn from(state: &FirstValueState) -> Self {
        if let Some(state) = &state.0 {
            state.clone()
        } else {
            None
        }
    }
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
#[aggregate("last_value(*) -> auto", state = "ref")] // TODO(rc): `last_value(any) -> any`
fn last_value<T>(_: Option<T>, input: Option<T>) -> Option<T> {
    input
}

#[aggregate("internal_last_seen_value(*) -> auto", state = "ref", internal)]
fn internal_last_seen_value<T>(state: T, input: T, retract: bool) -> T {
    if retract { state } else { input }
}
