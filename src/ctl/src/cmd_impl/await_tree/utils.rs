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

use crate::cmd_impl::await_tree::tree::{SpanNodeView, SpanView, TreeView};

/// Check `impl Display for StackTraceResponseOutput<'_>` for the format of the file.
pub(crate) fn extract_actor_traces<P: AsRef<Path>>(
    path: P,
) -> anyhow::Result<HashMap<u32, String>> {
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
                actor_traces.insert(actor_id, current_trace.trim().to_owned());
            }
            break;
        }

        // Parse Actor ID
        if in_actor_traces && line.starts_with(">> Actor ") {
            // Save the previous actor trace before processing the next one
            if let Some(actor_id) = current_actor_id {
                actor_traces.insert(actor_id, current_trace.trim().to_owned());
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
        actor_traces.insert(actor_id, current_trace.trim().to_owned());
    }

    Ok(actor_traces)
}

/// The process of converting the tree to text is not lossless—information such as
/// `node_id` will be lost. Consequently, this function can only restore information
/// to the best extent possible. Fields like `current` and `node_id` cannot be
/// recovered, but this loss does not affect our bottleneck detection. In the
/// function, we will set all `node_id` values to 0 and `current` to 100.
pub fn parse_tree_view_from_text(input: &str) -> anyhow::Result<TreeView> {
    let mut tree: Option<SpanNodeView> = None;
    let mut detached: Vec<SpanNodeView> = Vec::new();
    let mut node_stack: Vec<(usize, SpanNodeView)> = Vec::new();

    for line in input.lines() {
        let mut line = line.trim_end(); // Remove trailing spaces

        // Check for detached span
        if line.starts_with("[Detached ") {
            if let Some((_, node)) = node_stack.pop() {
                detached.push(node);
            }
            continue;
        }

        if let Some(stripped) = line.strip_suffix("<== current") {
            line = stripped.trim_end(); // Remove and trim again
        }

        // Check for span definition line
        if let Some((span_name, rest)) = line.split_once('[') {
            let name = span_name.trim().to_owned();

            // Extract elapsed time from the format `[elapsed_ns]`
            if let Some(elapsed_str) = rest.strip_suffix(']') {
                let elapsed_ns = parse_elapsed_ns(elapsed_str.trim());
                let is_long_running =
                    elapsed_ns >= 10_000_000_000 && !elapsed_str.starts_with("!!!");

                let span_view = SpanView {
                    name,
                    is_verbose: false,
                    is_long_running,
                };

                let id = node_stack.len();
                let new_node = SpanNodeView {
                    id: 0, // id cannot be recovered, we set it to 0
                    span: span_view,
                    elapsed_ns,
                    children: Vec::new(),
                };

                // Determine the depth of the current line (2 spaces per depth level)
                let depth = line.chars().take_while(|&c| c == ' ').count() / 2;

                if depth == 0 {
                    // Root span
                    if let Some((_, root)) = node_stack.pop() {
                        detached.push(root);
                    }
                    node_stack.push((id, new_node));
                } else {
                    // Check if the depth decreased, pop stack if necessary
                    while node_stack.len() > depth {
                        let (_, node) = node_stack.pop().unwrap();
                        if let Some((_, parent)) = node_stack.last_mut() {
                            parent.children.push(node);
                        }
                    }

                    // Push the new node onto the stack
                    node_stack.push((id, new_node));
                }
            }
        }
    }

    // Properly build the tree by attaching remaining nodes to their parents
    while let Some((_, node)) = node_stack.pop() {
        if let Some((_, parent)) = node_stack.last_mut() {
            parent.children.push(node);
        } else {
            tree = Some(node); // The last node in the stack is the root
        }
    }

    Ok(TreeView {
        current: usize::MAX, // Always set to an unreachable number
        tree: tree.unwrap(),
        detached,
    })
}

/// Parses the elapsed time in nanoseconds from a string.
///
/// # Example Input:
/// - "123456789ns"
/// - "!!! 12.345s"
fn parse_elapsed_ns(s: &str) -> u128 {
    if s.starts_with("!!!") {
        let s = s.trim_start_matches("!!!").trim();
        parse_time_str(s)
    } else {
        parse_time_str(s)
    }
}

/// Converts a formatted time string to nanoseconds.
///
/// # Supported Formats:
/// - "12.345s" → 12,345,000,000 ns
/// - "123ms" → 123,000,000 ns
/// - "456789ns" → 456,789 ns
fn parse_time_str(s: &str) -> u128 {
    if let Some(ms) = s.strip_suffix("ms") {
        ms.parse::<f64>().unwrap_or(0.0) as u128 * 1_000_000
    } else if let Some(ns) = s.strip_suffix("ns") {
        ns.parse::<u128>().unwrap_or(0)
    } else if let Some(s) = s.strip_suffix('s') {
        (s.parse::<f64>().unwrap_or(0.0) * 1_000_000_000.0) as u128
    } else {
        0
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;

    use crate::cmd_impl::await_tree::utils::parse_tree_view_from_text;

    #[tokio::test]
    async fn test_parse_tree_view_from_text_1() -> Result<()> {
        let input = r#"Actor 132: `mv` [21.285s]
  Epoch 8251479171792896 [!!! 21.283s]
    Materialize 8400000007 [!!! 21.283s]
      Project 8400000006 [!!! 21.280s]
        HashAgg 8400000005 [!!! 21.280s]
          Merge 8400000004 [0.001s]  <== current
"#;
        let tree_view = parse_tree_view_from_text(input)?;

        let expected = r#"Actor 132: `mv` [21.285s]
  Epoch 8251479171792896 [!!! 21.283s]
    Materialize 8400000007 [!!! 21.283s]
      Project 8400000006 [!!! 21.280s]
        HashAgg 8400000005 [!!! 21.280s]
          Merge 8400000004 [0.001s]
"#;
        assert_eq!(tree_view.to_string(), expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_tree_view_from_text_2() -> Result<()> {
        let input = r#"Actor 132: `mv` [21.285s]
  Epoch 8251479171792896 [!!! 21.283s]
    Materialize 8400000007 [!!! 21.283s]
      Project 8400000006 [!!! 21.280s]
        HashAgg 8400000005 [!!! 21.280s]
          Merge 8400000004 [0.001s]  <== current
        HashAgg 8400000005 [!!! 21.380s]
"#;
        let tree_view = parse_tree_view_from_text(input)?;

        let expected = r#"Actor 132: `mv` [21.285s]
  Epoch 8251479171792896 [!!! 21.283s]
    Materialize 8400000007 [!!! 21.283s]
      Project 8400000006 [!!! 21.280s]
        HashAgg 8400000005 [!!! 21.280s]
          Merge 8400000004 [0.001s]
        HashAgg 8400000005 [!!! 21.380s]
"#;
        assert_eq!(tree_view.to_string(), expected);
        Ok(())
    }
}
