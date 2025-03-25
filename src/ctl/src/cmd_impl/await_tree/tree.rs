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

use std::fmt::Write;

use serde::Deserialize;

/// Please refer to https://github.com/risingwavelabs/await-tree/blob/main/src/context.rs for the original definition.
/// This is for loading await tree info from the JSON output of `Tree`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TreeView {
    /// ID of the currently active span
    pub current: usize,

    /// The root span tree
    pub tree: SpanNodeView,

    /// Detached subtrees
    pub detached: Vec<SpanNodeView>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SpanNodeView {
    /// Unique identifier in the arena
    pub id: usize,

    /// Span metadata
    pub span: SpanView,

    /// Elapsed time in nanoseconds
    pub elapsed_ns: u128,

    /// Recursive children
    pub children: Vec<SpanNodeView>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SpanView {
    /// Span name (likely String or interned)
    pub name: String,

    /// Whether this span is verbose
    #[allow(dead_code)]
    pub is_verbose: bool,

    /// Whether this span is long-running
    pub is_long_running: bool,
}

impl std::fmt::Display for TreeView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_node(
            f: &mut std::fmt::Formatter<'_>,
            node: &SpanNodeView,
            depth: usize,
            current_id: usize,
        ) -> std::fmt::Result {
            // Indentation
            f.write_str(&" ".repeat(depth * 2))?;

            // Span name
            f.write_str(&node.span.name)?;

            // Elapsed time
            let elapsed_secs = node.elapsed_ns as f64 / 1_000_000_000.0;
            write!(
                f,
                " [{}{:.3}s]",
                if !node.span.is_long_running && elapsed_secs >= 10.0 {
                    "!!! "
                } else {
                    ""
                },
                elapsed_secs
            )?;

            // Current span marker
            if node.id == current_id {
                f.write_str("  <== current")?;
            }

            f.write_char('\n')?;

            // Format children recursively
            let mut children = node.children.clone();
            children.sort_by_key(|n| n.elapsed_ns); // mimic sort by start_time
            for child in &children {
                fmt_node(f, child, depth + 1, current_id)?;
            }

            Ok(())
        }
        // Format the main tree
        fmt_node(f, &self.tree, 0, self.current)?;

        // Format detached spans
        for node in &self.detached {
            writeln!(f, "[Detached {}]", node.id)?;
            fmt_node(f, node, 1, self.current)?;
        }

        Ok(())
    }
}
