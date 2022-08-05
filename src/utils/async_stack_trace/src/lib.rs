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
use std::sync::Arc;
use std::task::Poll;

use context::ContextId;
use futures::future::Fuse;
use futures::{Future, FutureExt};
use indextree::NodeId;
use pin_project::{pin_project, pinned_drop};

use crate::context::{with_context, TRACE_CONTEXT};

mod context;
mod manager;

pub use manager::{StackTraceManager, StackTraceReport, TraceReporter};
pub type SpanValue = Arc<str>;

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
}

/// The future for [`StackTrace::stack_trace`].
#[pin_project(PinnedDrop)]
pub struct StackTraced<F: Future> {
    #[pin]
    inner: F,

    /// The state of this traced future.
    state: StackTracedState,
}

impl<F: Future> StackTraced<F> {
    fn new(inner: F, span: impl Into<SpanValue>) -> Self {
        Self {
            inner,
            state: StackTracedState::Initial(span.into()),
        }
    }
}

impl<F: Future> Future for StackTraced<F> {
    type Output = F::Output;

    // TODO: may disable based on features
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let current_context = TRACE_CONTEXT.try_with(|c| c.borrow().id());

        // For assertion.
        let old_current = TRACE_CONTEXT.try_with(|c| c.borrow().current());

        let this_node = match this.state {
            StackTracedState::Initial(span) => {
                match current_context {
                    // First polled
                    Ok(current_context) => {
                        // First polled, push a new span to the context.
                        let node = with_context(|mut c| c.push(span.clone()));
                        *this.state = StackTracedState::Polled {
                            this_node: node,
                            this_context: current_context,
                        };
                        node
                    }
                    // Not in a context
                    Err(_) => return this.inner.poll(cx),
                }
            }
            StackTracedState::Polled {
                this_node,
                this_context,
            } => {
                match current_context {
                    // Context correct
                    Ok(current_context) if current_context == *this_context => {
                        // Polled before, just step in.
                        with_context(|mut c| c.step_in(*this_node));
                        *this_node
                    }
                    // Context changed
                    Ok(_) => {
                        tracing::warn!("stack traced future is polled in a different context as it was first polled, won't be traced now");
                        return this.inner.poll(cx);
                    }
                    // Out of context
                    Err(_) => {
                        tracing::warn!("stack traced future is not polled in a traced context, while it was when first polled, won't be traced now");
                        return this.inner.poll(cx);
                    }
                }
            }
            StackTracedState::Ready => unreachable!("the traced future should always be fused"),
        };

        // The current node must be the this_node.
        assert_eq!(this_node, with_context(|c| c.current()));

        let r = match this.inner.poll(cx) {
            // The future is ready, clean-up this span by popping from the context.
            Poll::Ready(output) => {
                with_context(|mut c| c.pop());
                *this.state = StackTracedState::Ready;
                Poll::Ready(output)
            }
            // Still pending, just step out.
            Poll::Pending => {
                with_context(|mut c| c.step_out());
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
        let current_context = TRACE_CONTEXT.try_with(|c| c.borrow().id());

        match this.state {
            StackTracedState::Polled {
                this_node,
                this_context,
            } => match current_context {
                // Context correct
                Ok(current_context) if current_context == *this_context => {
                    with_context(|mut c| c.remove_and_detach(*this_node));
                }
                // Context changed
                Ok(_) => {
                    tracing::warn!("stack traced future is dropped in a different context as it was first polled, cannot clean up!");
                }
                // Out of context
                Err(_) => {
                    tracing::warn!("stack traced future is not in a traced context, while it was when first polled, cannot clean up!");
                }
            },
            StackTracedState::Initial(_) | StackTracedState::Ready => {}
        }
    }
}

pub trait StackTrace: Future + Sized {
    /// Wrap this future, so that we're able to check the stack trace and find where and why this
    /// future is pending, with [`StackTraceReport`] and [`StackTraceManager`].
    fn stack_trace(self, span: impl Into<SpanValue>) -> Fuse<StackTraced<Self>> {
        StackTraced::new(self, span).fuse()
    }
}
impl<F> StackTrace for F where F: Future {}

#[cfg(test)]
mod tests;
