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

mod compact_task_assignment;
mod compaction_group_config;
mod compaction_status;
mod pinned_snapshot;
mod pinned_version;
mod version;
mod version_delta;
mod version_stats;

pub use compaction_group_config::CompactionGroup;
pub use compaction_status::*;
pub use pinned_snapshot::*;
pub use pinned_version::*;
pub use version::*;
pub use version_delta::*;

/// Column family names for hummock.
/// Deprecated `cf_name` should be reserved for backward compatibility.
const HUMMOCK_VERSION_CF_NAME: &str = "cf/hummock_0";
const HUMMOCK_VERSION_DELTA_CF_NAME: &str = "cf/hummock_1";
const HUMMOCK_PINNED_VERSION_CF_NAME: &str = "cf/hummock_2";
const HUMMOCK_PINNED_SNAPSHOT_CF_NAME: &str = "cf/hummock_3";
const HUMMOCK_COMPACTION_STATUS_CF_NAME: &str = "cf/hummock_4";
const HUMMOCK_COMPACT_TASK_ASSIGNMENT: &str = "cf/hummock_5";
const HUMMOCK_COMPACTION_GROUP_CONFIG_CF_NAME: &str = "cf/hummock_6";
const HUMMOCK_VERSION_STATS_CF_NAME: &str = "cf/hummock_7";
