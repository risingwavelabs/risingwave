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

// FIXME: This is a false-positive clippy test, remove this while bumping toolchain.
// https://github.com/tokio-rs/tokio/issues/4836
// https://github.com/rust-lang/rust-clippy/issues/8493
#![expect(clippy::declare_interior_mutable_const)]

use std::cell::RefCell;
use std::fmt::{Debug, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use indextree::{Arena, NodeId};
use itertools::Itertools;

use crate::manager::StackTraceReport;
use crate::SpanValue;

/// Node in the span tree.
#[derive(Debug)]
struct SpanNode {
    /// The span value.
    span: SpanValue,

    /// The time when this span was started, or the future was first polled.
    start_time: coarsetime::Instant,
}

impl SpanNode {
    /// Create a new node with the given value.
    fn new(span: SpanValue) -> Self {
        Self {
            span,
            start_time: coarsetime::Instant::now(),
        }
    }
}

/// The id of a trace context. We will check the id recorded in the traced future against the
/// current task local context before trying to update the span.
pub(crate) type ContextId = u64;

/// The task local trace context.
#[derive(Debug)]
pub(crate) struct TraceContext {
    /// The id of the context.
    id: ContextId,

    /// Whether to report the detached spans, that is, spans that are not able to be polled now.
    report_detached: bool,

    /// The arena for allocating span nodes in this context.
    arena: Arena<SpanNode>,

    /// The root span node.
    root: NodeId,

    /// The current span node. This is the node that is currently being polled.
    current: NodeId,
}

impl std::fmt::Display for TraceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_node(
            f: &mut std::fmt::Formatter<'_>,
            arena: &Arena<SpanNode>,
            node: NodeId,
            depth: usize,
            current: NodeId,
        ) -> std::fmt::Result {
            f.write_str(&" ".repeat(depth * 2))?;

            let inner = arena[node].get();
            f.write_str(inner.span.as_ref())?;

            let elapsed: Duration = inner.start_time.elapsed().into();
            f.write_fmt(format_args!(
                " [{}{:?}]",
                if depth > 0 && elapsed.as_secs() >= 1 {
                    "!!! "
                } else {
                    ""
                },
                elapsed
            ))?;

            if depth > 0 && node == current {
                f.write_str("  <== current")?;
            }

            f.write_char('\n')?;
            for child in node
                .children(arena)
                .sorted_by(|&a, &b| arena[a].get().span.cmp(&arena[b].get().span))
            {
                fmt_node(f, arena, child, depth + 1, current)?;
            }

            Ok(())
        }

        fmt_node(f, &self.arena, self.root, 0, self.current)?;

        // Print all detached spans. May hurt the performance so make it optional.
        if self.report_detached {
            for node in self.arena.iter().filter(|n| !n.is_removed()) {
                let id = self.arena.get_node_id(node).unwrap();
                if id == self.root {
                    continue;
                }
                if node.parent().is_none()
                    && node.next_sibling().is_none()
                    && node.previous_sibling().is_none()
                {
                    f.write_str("[??? Detached]\n")?;
                    fmt_node(f, &self.arena, id, 1, self.current)?;
                }
            }
        }

        Ok(())
    }
}

impl TraceContext {
    /// Create a new stack trace context with the given root span.
    pub fn new(root_span: SpanValue, report_detached: bool) -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);
        let id = ID.fetch_add(1, Ordering::SeqCst);

        let mut arena = Arena::new();
        let root = arena.new_node(SpanNode::new(root_span));

        Self {
            id,
            report_detached,
            arena,
            root,
            current: root,
        }
    }

    /// Get the count of active span nodes in this context.
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn active_node_count(&self) -> usize {
        self.arena.iter().filter(|n| !n.is_removed()).count()
    }

    /// Get the report of the current state of the stack trace.
    pub fn to_report(&self) -> StackTraceReport {
        let report = format!("{}", self);
        StackTraceReport {
            report,
            capture_time: std::time::Instant::now(),
        }
    }

    /// Push a new span as a child of current span, used for future firstly polled.
    ///
    /// Returns the new current span.
    pub fn push(&mut self, span: SpanValue) -> NodeId {
        let child = self.arena.new_node(SpanNode::new(span));
        self.current.append(child, &mut self.arena);
        self.current = child;
        child
    }

    /// Step in the current span to the given child, used for future polled again.
    ///
    /// If the child is not actually a child of the current span, it means we are using a new future
    /// to poll it, so we need to detach it from the previous parent, and attach it to the current
    /// span.
    pub fn step_in(&mut self, child: NodeId) {
        if !self.current.children(&self.arena).contains(&child) {
            // Actually we can always call this even if `child` is already a child of `current`.
            self.current.append(child, &mut self.arena);
        }
        self.current = child;
    }

    /// Pop the current span to the parent, used for future ready.
    ///
    /// Note that there might still be some children of this node, like `select_stream.next()`.
    /// The children might be polled again later, and will be attached as the children of a new
    /// span.
    pub fn pop(&mut self) {
        let parent = self.arena[self.current]
            .parent()
            .expect("the root node should not be popped");
        self.remove_and_detach(self.current);
        self.current = parent;
    }

    /// Step out the current span to the parent, used for future pending.
    pub fn step_out(&mut self) {
        let parent = self.arena[self.current]
            .parent()
            .expect("the root node should not be stepped out");
        self.current = parent;
    }

    /// Remove the current span and detach the children, used for future aborting.
    ///
    /// The children might be polled again later, and will be attached as the children of a new
    /// span.
    pub fn remove_and_detach(&mut self, node: NodeId) {
        node.detach(&mut self.arena);
        // Removing detached `node` makes children detached.
        node.remove(&mut self.arena);
    }

    /// Get the context id.
    pub fn id(&self) -> ContextId {
        self.id
    }

    /// Get the current span node id.
    pub fn current(&self) -> NodeId {
        self.current
    }
}

tokio::task_local! {
    pub(crate) static TRACE_CONTEXT: RefCell<TraceContext>
}

pub(crate) fn with_context<F, R>(f: F) -> R
where
    F: FnOnce(&mut TraceContext) -> R,
{
    TRACE_CONTEXT.with(|trace_context| {
        let mut trace_context = trace_context.borrow_mut();
        f(&mut trace_context)
    })
}

pub(crate) fn try_with_context<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut TraceContext) -> R,
{
    TRACE_CONTEXT
        .try_with(|trace_context| {
            let mut trace_context = trace_context.borrow_mut();
            f(&mut trace_context)
        })
        .ok()
}

/// Get the current context. Returns `None` if we're not traced.
///
/// This is useful if you want to check which component or runtime task is calling this function.
pub fn current_context() -> Option<String> {
    try_with_context(|c| c.to_string())
}
