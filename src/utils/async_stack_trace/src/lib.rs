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

#![feature(generators)]
#![feature(map_try_insert)]
#![feature(lint_reasons)]

use std::pin::Pin;
use std::task::Poll;

use context::ContextId;
use futures::Future;
use indextree::NodeId;
use pin_project::{pin_project, pinned_drop};
use triomphe::Arc;

use crate::context::{try_with_context, with_context};

mod context;
mod manager;

pub use context::current_context;
pub use manager::{StackTraceManager, StackTraceReport, TraceConfig, TraceReporter};

/// A cheaply-cloneable span string.
#[derive(Debug, Clone)]
pub enum SpanValue {
    Slice(&'static str),
    Shared(Arc<String>),
}

impl Default for SpanValue {
    fn default() -> Self {
        Self::Slice("")
    }
}
impl From<&'static str> for SpanValue {
    fn from(s: &'static str) -> Self {
        Self::Slice(s)
    }
}
impl From<String> for SpanValue {
    fn from(s: String) -> Self {
        Self::Shared(Arc::new(s))
    }
}
impl AsRef<str> for SpanValue {
    fn as_ref(&self) -> &str {
        match self {
            Self::Slice(s) => s,
            Self::Shared(s) => s.as_str(),
        }
    }
}
impl PartialEq for SpanValue {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}
impl Eq for SpanValue {}
impl Ord for SpanValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}
impl PartialOrd for SpanValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// State for stack traced future.
enum StackTracedState {
    Initial(SpanValue),
    Polled {
        /// The node associated with this future.
        this_node: NodeId,
        // The id of the context where this future is first polled.
        this_context: ContextId,
    },
    Ready,
    /// The stack trace is disabled due to `verbose` configuration.
    Disabled,
}

/// The future for [`StackTrace::stack_trace`].
#[pin_project(PinnedDrop)]
pub struct StackTraced<F: Future> {
    #[pin]
    inner: F,

    /// Whether the span is a verbose one.
    is_verbose: bool,

    /// The state of this traced future.
    state: StackTracedState,
}

impl<F: Future> StackTraced<F> {
    fn new(inner: F, span: impl Into<SpanValue>, is_verbose: bool) -> Self {
        Self {
            inner,
            is_verbose,
            state: StackTracedState::Initial(span.into()),
        }
    }
}

impl<F: Future> Future for StackTraced<F> {
    type Output = F::Output;

    // TODO: may optionally enable based on the features
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let is_verbose = self.is_verbose;
        let this = self.project();

        // For assertion.
        let old_current = try_with_context(|c| c.current());

        let this_node = match this.state {
            StackTracedState::Initial(span) => {
                match try_with_context(|c| (c.id(), c.verbose() >= is_verbose)) {
                    // The tracing for this span is disabled according to the verbose configuration.
                    Some((_, false)) => {
                        *this.state = StackTracedState::Disabled;
                        return this.inner.poll(cx);
                    }
                    // First polled
                    Some((current_context, true)) => {
                        // First polled, push a new span to the context.
                        let node = with_context(|c| c.push(std::mem::take(span)));
                        *this.state = StackTracedState::Polled {
                            this_node: node,
                            this_context: current_context,
                        };
                        node
                    }
                    // Not in a context
                    None => return this.inner.poll(cx),
                }
            }
            StackTracedState::Polled {
                this_node,
                this_context,
            } => {
                match try_with_context(|c| c.id()) {
                    // Context correct
                    Some(current_context) if current_context == *this_context => {
                        // Polled before, just step in.
                        with_context(|c| c.step_in(*this_node));
                        *this_node
                    }
                    // Context changed
                    Some(_) => {
                        tracing::warn!("stack traced future is polled in a different context as it was first polled, won't be traced now");
                        return this.inner.poll(cx);
                    }
                    // Out of context
                    None => {
                        tracing::warn!("stack traced future is not polled in a traced context, while it was when first polled, won't be traced now");
                        return this.inner.poll(cx);
                    }
                }
            }
            StackTracedState::Ready => unreachable!("the traced future should always be fused"),
            StackTracedState::Disabled => return this.inner.poll(cx),
        };

        // The current node must be the this_node.
        assert_eq!(this_node, with_context(|c| c.current()));

        let r = match this.inner.poll(cx) {
            // The future is ready, clean-up this span by popping from the context.
            Poll::Ready(output) => {
                with_context(|c| c.pop());
                *this.state = StackTracedState::Ready;
                Poll::Ready(output)
            }
            // Still pending, just step out.
            Poll::Pending => {
                with_context(|c| c.step_out());
                Poll::Pending
            }
        };

        // The current node must be the same as we started with.
        assert_eq!(old_current.unwrap(), with_context(|c| c.current()));

        r
    }
}

#[pinned_drop]
impl<F: Future> PinnedDrop for StackTraced<F> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let current_context = try_with_context(|c| c.id());

        match this.state {
            StackTracedState::Polled {
                this_node,
                this_context,
            } => match current_context {
                // Context correct
                Some(current_context) if current_context == *this_context => {
                    with_context(|c| c.remove_and_detach(*this_node));
                }
                // Context changed
                Some(_) => {
                    tracing::warn!("stack traced future is dropped in a different context as it was first polled, cannot clean up!");
                }
                // Out of context
                None => {
                    tracing::warn!("stack traced future is not in a traced context, while it was when first polled, cannot clean up!");
                }
            },
            StackTracedState::Initial(_) | StackTracedState::Ready | StackTracedState::Disabled => {
            }
        }
    }
}

pub trait StackTrace: Future + Sized {
    /// Wrap this future, so that we're able to check the stack trace and find where and why this
    /// future is pending, with [`StackTraceReport`] and [`StackTraceManager`].
    fn stack_trace(self, span: impl Into<SpanValue>) -> StackTraced<Self> {
        StackTraced::new(self, span, false)
    }

    fn verbose_stack_trace(self, span: impl Into<SpanValue>) -> StackTraced<Self> {
        StackTraced::new(self, span, true)
    }
}
impl<F> StackTrace for F where F: Future {}

#[cfg(test)]
mod tests;
