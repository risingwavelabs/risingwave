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

use crate::cmd_impl::await_tree::tree::TreeView;
use crate::cmd_impl::await_tree::utils::extract_actor_traces;

pub fn transcribe(path: String) -> anyhow::Result<()> {
    let actor_traces = extract_actor_traces(&path)
        .map_err(|e| anyhow::anyhow!("Failed to extract actor traces from file: {}", e))?;
    for (actor_id, trace) in actor_traces {
        let tree: TreeView = serde_json::from_str(&trace)
            .map_err(|e| anyhow::anyhow!("Failed to parse actor trace JSON: {}", e))?;
        println!(">> Actor {}", actor_id);
        println!("{}", tree);
    }
    Ok(())
}
