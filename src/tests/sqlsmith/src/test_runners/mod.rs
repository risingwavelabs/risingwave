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

//! Contains test runners:
//! - fuzzing: For crashing testing the database with generated batch, stream queries.
//! - differential testing: For testing the database with generated batch,
//!   stream queries and comparing their results.

mod diff;
mod fuzzing;

mod utils;

pub use diff::run_differential_testing;
pub use fuzzing::{generate, run, run_pre_generated};
