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

/// Wrapper and helper functions to help implement [`Executor`] for `TopN` variants
mod utils;
use utils::*;

mod top_n_cache;
mod top_n_state;
use top_n_cache::{CacheKey, TopNCache, TopNCacheTrait};
use top_n_state::ManagedTopNState;

// `TopN` variants
mod group_top_n;
mod group_top_n_appendonly;
mod top_n_appendonly;
mod top_n_plain;

pub use group_top_n::GroupTopNExecutor;
pub use group_top_n_appendonly::AppendOnlyGroupTopNExecutor;
pub use top_n_appendonly::AppendOnlyTopNExecutor;
pub use top_n_plain::TopNExecutor;
