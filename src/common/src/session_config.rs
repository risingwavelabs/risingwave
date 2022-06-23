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

/// If `RW_IMPLICIT_FLUSH` is on, then every INSERT/UPDATE/DELETE statement will block
/// until the entire dataflow is refreshed. In other words, every related table & MV will
/// be able to see the write.
pub const IMPLICIT_FLUSH: &str = "RW_IMPLICIT_FLUSH";

/// A temporary config variable to force query running in either local or distributed mode.
/// It will be removed in the future.
pub const QUERY_MODE: &str = "QUERY_MODE";

/// To force the usage of delta join in streaming execution.
pub const DELTA_JOIN: &str = "RW_FORCE_DELTA_JOIN";

/// see <https://www.postgresql.org/docs/current/runtime-config-client.html#:~:text=for%20more%20information.-,extra_float_digits,-(integer)>
pub const EXTRA_FLOAT_DIGITS: &str = "EXTRA_FLOAT_DIGITS";
