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

//! Main-graph transient job that drives the pk-index compaction resolver pipeline
//! while the streaming writer is paused.

mod control;
mod render;

pub(crate) use control::{CompactionResolveJob, CompactionResolveJobRegistry};
pub(crate) use render::{build_resolver_stream_node, output_file_paths, render_resolver_fragment};
