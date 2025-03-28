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

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

/// Check `impl Display for StackTraceResponseOutput<'_>` for the format of the file.
pub fn extract_actor_traces<P: AsRef<Path>>(path: P) -> anyhow::Result<HashMap<u32, String>> {
    let file = File::open(path)?;
    let reader = io::BufReader::new(file);

    let mut actor_traces = HashMap::new();
    let mut in_actor_traces = false;
    let mut current_actor_id = None;
    let mut current_trace = String::new();

    for line in reader.lines() {
        let line = line?;

        // Detect the start of the Actor Traces section
        if line == "--- Actor Traces ---" {
            in_actor_traces = true;
            continue;
        }

        // Stop parsing if a new section is encountered
        if line.starts_with("---") && in_actor_traces {
            if let Some(actor_id) = current_actor_id {
                actor_traces.insert(actor_id, current_trace.trim().to_string());
            }
            break;
        }

        // Parse Actor ID
        if in_actor_traces && line.starts_with(">> Actor ") {
            // Save the previous actor trace before processing the next one
            if let Some(actor_id) = current_actor_id {
                actor_traces.insert(actor_id, current_trace.trim().to_string());
            }
            // Extract actor_id
            if let Some(id_str) = line.strip_prefix(">> Actor ") {
                if let Ok(actor_id) = id_str.trim().parse::<u32>() {
                    current_actor_id = Some(actor_id);
                    current_trace.clear(); // Clear trace for the next actor
                }
            }
        } else if in_actor_traces {
            // Accumulate trace content for the current actor
            current_trace.push_str(&line);
            current_trace.push('\n');
        }
    }

    // Store the last actor's trace if any
    if let Some(actor_id) = current_actor_id {
        actor_traces.insert(actor_id, current_trace.trim().to_string());
    }

    Ok(actor_traces)
}
