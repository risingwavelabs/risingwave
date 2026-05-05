// Copyright 2026 RisingWave Labs
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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The catalog `pg_aggregate` stores aggregate-function metadata.
/// Hasura only uses it to filter aggregate routines out of function tracking.
#[system_catalog(
    view,
    "pg_catalog.pg_aggregate",
    "SELECT oid AS aggfnoid FROM pg_catalog.pg_proc WHERE prokind = 'a'"
)]
#[derive(Fields)]
struct PgAggregate {
    aggfnoid: i32,
}
