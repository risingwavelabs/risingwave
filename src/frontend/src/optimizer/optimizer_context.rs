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

use core::convert::Into;
use core::fmt::Formatter;
use std::cell::{RefCell, RefMut};
use std::rc::Rc;
use std::sync::Arc;

use risingwave_sqlparser::ast::{ExplainOptions, ExplainType};

use crate::expr::{CorrelatedId, SessionTimezone};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::PlanNodeId;
use crate::session::SessionImpl;
use crate::WithOptions;

const RESERVED_ID_NUM: u16 = 10000;

pub struct OptimizerContext {
    session_ctx: Arc<SessionImpl>,
    /// Store plan node id
    next_plan_node_id: RefCell<i32>,
    /// The original SQL string, used for debugging.
    sql: String,
    /// Normalized SQL string. See [`HandlerArgs::normalize_sql`].
    normalized_sql: String,
    /// Explain options
    explain_options: ExplainOptions,
    /// Store the trace of optimizer
    optimizer_trace: RefCell<Vec<String>>,
    /// Store the optimized logical plan of optimizer
    logical_explain: RefCell<Option<String>>,
    /// Store correlated id
    next_correlated_id: RefCell<u32>,
    /// Store options or properties from the `with` clause
    with_options: WithOptions,
    /// Store the Session Timezone and whether it was used.
    session_timezone: RefCell<SessionTimezone>,
    /// Store expr display id.
    next_expr_display_id: RefCell<usize>,
}

pub type OptimizerContextRef = Rc<OptimizerContext>;

impl OptimizerContext {
    /// Create a new [`OptimizerContext`] from the given [`HandlerArgs`], with empty
    /// [`ExplainOptions`].
    pub fn from_handler_args(handler_args: HandlerArgs) -> Self {
        Self::new(handler_args, ExplainOptions::default())
    }

    /// Create a new [`OptimizerContext`] from the given [`HandlerArgs`] and [`ExplainOptions`].
    pub fn new(handler_args: HandlerArgs, explain_options: ExplainOptions) -> Self {
        let session_timezone = RefCell::new(SessionTimezone::new(
            handler_args.session.config().get_timezone().to_owned(),
        ));
        Self {
            session_ctx: handler_args.session,
            next_plan_node_id: RefCell::new(RESERVED_ID_NUM.into()),
            sql: handler_args.sql,
            normalized_sql: handler_args.normalized_sql,
            explain_options,
            optimizer_trace: RefCell::new(vec![]),
            logical_explain: RefCell::new(None),
            next_correlated_id: RefCell::new(0),
            with_options: handler_args.with_options,
            session_timezone,
            next_expr_display_id: RefCell::new(RESERVED_ID_NUM.into()),
        }
    }

    // TODO(TaoWu): Remove the async.
    #[cfg(test)]
    #[expect(clippy::unused_async)]
    pub async fn mock() -> OptimizerContextRef {
        Self {
            session_ctx: Arc::new(SessionImpl::mock()),
            next_plan_node_id: RefCell::new(0),
            sql: "".to_owned(),
            normalized_sql: "".to_owned(),
            explain_options: ExplainOptions::default(),
            optimizer_trace: RefCell::new(vec![]),
            logical_explain: RefCell::new(None),
            next_correlated_id: RefCell::new(0),
            with_options: Default::default(),
            session_timezone: RefCell::new(SessionTimezone::new("UTC".into())),
            next_expr_display_id: RefCell::new(0),
        }
        .into()
    }

    pub fn next_plan_node_id(&self) -> PlanNodeId {
        *self.next_plan_node_id.borrow_mut() += 1;
        PlanNodeId(*self.next_plan_node_id.borrow())
    }

    pub fn get_plan_node_id(&self) -> i32 {
        *self.next_plan_node_id.borrow()
    }

    pub fn set_plan_node_id(&self, next_plan_node_id: i32) {
        *self.next_plan_node_id.borrow_mut() = next_plan_node_id;
    }

    pub fn next_expr_display_id(&self) -> usize {
        *self.next_expr_display_id.borrow_mut() += 1;
        *self.next_expr_display_id.borrow()
    }

    pub fn get_expr_display_id(&self) -> usize {
        *self.next_expr_display_id.borrow()
    }

    pub fn set_expr_display_id(&self, expr_display_id: usize) {
        *self.next_expr_display_id.borrow_mut() = expr_display_id;
    }

    pub fn next_correlated_id(&self) -> CorrelatedId {
        *self.next_correlated_id.borrow_mut() += 1;
        *self.next_correlated_id.borrow()
    }

    pub fn is_explain_verbose(&self) -> bool {
        self.explain_options.verbose
    }

    pub fn is_explain_trace(&self) -> bool {
        self.explain_options.trace
    }

    pub fn explain_type(&self) -> ExplainType {
        self.explain_options.explain_type.clone()
    }

    pub fn is_explain_logical(&self) -> bool {
        self.explain_type() == ExplainType::Logical
    }

    pub fn trace(&self, str: impl Into<String>) {
        // If explain type is logical, do not store the trace for any optimizations beyond logical.
        if self.is_explain_logical() && self.logical_explain.borrow().is_some() {
            return;
        }
        let mut optimizer_trace = self.optimizer_trace.borrow_mut();
        let string = str.into();
        tracing::trace!("{}", string);
        optimizer_trace.push(string);
        optimizer_trace.push("\n".to_string());
    }

    pub fn store_logical(&self, str: impl Into<String>) {
        *self.logical_explain.borrow_mut() = Some(str.into())
    }

    pub fn take_logical(&self) -> Option<String> {
        self.logical_explain.borrow_mut().take()
    }

    pub fn take_trace(&self) -> Vec<String> {
        self.optimizer_trace.borrow_mut().drain(..).collect()
    }

    pub fn with_options(&self) -> &WithOptions {
        &self.with_options
    }

    pub fn session_ctx(&self) -> &Arc<SessionImpl> {
        &self.session_ctx
    }

    /// Return the original SQL.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Return the normalized SQL.
    pub fn normalized_sql(&self) -> &str {
        &self.normalized_sql
    }

    pub fn session_timezone(&self) -> RefMut<'_, SessionTimezone> {
        self.session_timezone.borrow_mut()
    }

    /// Appends any information that the optimizer needs to alert the user about to the PG NOTICE
    pub fn append_notice(&self, notice: &mut String) {
        if let Some(warning) = self.session_timezone.borrow().warning() {
            notice.push_str(&warning);
        }
    }

    pub fn get_session_timezone(&self) -> String {
        self.session_timezone.borrow().timezone()
    }
}

impl std::fmt::Debug for OptimizerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext {{ next_plan_node_id = {}, sql = {}, explain_options = {}, next_correlated_id = {}, with_options = {:?} }}",
            self.next_plan_node_id.borrow(),
            self.sql,
            self.explain_options,
            self.next_correlated_id.borrow(),
            &self.with_options
        )
    }
}
