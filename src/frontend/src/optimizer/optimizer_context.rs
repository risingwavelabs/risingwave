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

use core::convert::{From, Into};
use core::fmt::Formatter;
use core::marker::Sync;
use core::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::cell::RefCell;
use std::sync::Arc;

use crate::expr::CorrelatedId;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::PlanNodeId;
use crate::session::SessionImpl;
use crate::WithOptions;

pub struct OptimizerContext {
    pub session_ctx: Arc<SessionImpl>,
    // We use `AtomicI32` here because `Arc<T>` implements `Send` only when `T: Send + Sync`.
    pub next_id: AtomicI32,
    /// For debugging purposes, store the SQL string in Context
    pub sql: Arc<str>,

    /// it indicates whether the explain mode is verbose for explain statement
    pub explain_verbose: AtomicBool,

    /// it indicates whether the explain mode is trace for explain statement
    pub explain_trace: AtomicBool,
    /// Store the trace of optimizer
    pub optimizer_trace: RefCell<Vec<String>>,
    /// Store correlated id
    pub next_correlated_id: AtomicU32,
    /// Store options or properties from the `with` clause
    pub with_options: WithOptions,
}

#[derive(Clone, Debug)]
pub struct OptimizerContextRef {
    inner: Arc<OptimizerContext>,
}

impl !Sync for OptimizerContextRef {}

impl From<OptimizerContext> for OptimizerContextRef {
    fn from(inner: OptimizerContext) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl OptimizerContextRef {
    pub fn inner(&self) -> &OptimizerContext {
        &self.inner
    }

    pub fn next_plan_node_id(&self) -> PlanNodeId {
        // It's safe to use `fetch_add` and `Relaxed` ordering since we have marked
        // `QueryContextRef` not `Sync`.
        let next_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        PlanNodeId(next_id)
    }

    pub fn next_correlated_id(&self) -> CorrelatedId {
        self.inner
            .next_correlated_id
            .fetch_add(1, Ordering::Relaxed)
    }

    pub fn is_explain_verbose(&self) -> bool {
        self.inner.explain_verbose.load(Ordering::Acquire)
    }

    pub fn is_explain_trace(&self) -> bool {
        self.inner.explain_trace.load(Ordering::Acquire)
    }

    pub fn trace(&self, str: impl Into<String>) {
        self.inner.optimizer_trace.borrow_mut().push(str.into());
        self.inner
            .optimizer_trace
            .borrow_mut()
            .push("\n".to_string());
    }

    pub fn take_trace(&self) -> Vec<String> {
        self.inner.optimizer_trace.borrow_mut().drain(..).collect()
    }
}

impl OptimizerContext {
    pub fn new_with_handler_args(handler_args: HandlerArgs) -> Self {
        Self::new(
            handler_args.session,
            handler_args.sql,
            handler_args.with_options,
        )
    }

    pub fn new(session_ctx: Arc<SessionImpl>, sql: Arc<str>, with_options: WithOptions) -> Self {
        Self {
            session_ctx,
            next_id: AtomicI32::new(0),
            sql,
            explain_verbose: AtomicBool::new(false),
            explain_trace: AtomicBool::new(false),
            optimizer_trace: RefCell::new(vec![]),
            next_correlated_id: AtomicU32::new(1),
            with_options,
        }
    }

    // TODO(TaoWu): Remove the async.
    #[cfg(test)]
    #[expect(clippy::unused_async)]
    pub async fn mock() -> OptimizerContextRef {
        Self {
            session_ctx: Arc::new(SessionImpl::mock()),
            next_id: AtomicI32::new(0),
            sql: Arc::from(""),
            explain_verbose: AtomicBool::new(false),
            explain_trace: AtomicBool::new(false),
            optimizer_trace: RefCell::new(vec![]),
            next_correlated_id: AtomicU32::new(1),
            with_options: Default::default(),
        }
        .into()
    }
}

impl std::fmt::Debug for OptimizerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext {{ current id = {} }}",
            self.next_id.load(Ordering::Relaxed)
        )
    }
}
