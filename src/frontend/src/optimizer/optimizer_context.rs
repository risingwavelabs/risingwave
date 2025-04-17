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

use core::fmt::Formatter;
use std::cell::{Cell, RefCell, RefMut};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use risingwave_sqlparser::ast::{ExplainFormat, ExplainOptions, ExplainType};

use super::property::WatermarkGroupId;
use crate::PlanRef;
use crate::binder::ShareId;
use crate::expr::{CorrelatedId, SessionTimezone};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::PlanNodeId;
use crate::session::SessionImpl;
use crate::utils::{OverwriteOptions, WithOptions};

const RESERVED_ID_NUM: u16 = 10000;

type PhantomUnsend = PhantomData<Rc<()>>;

pub struct OptimizerContext {
    session_ctx: Arc<SessionImpl>,
    /// The original SQL string, used for debugging.
    sql: Arc<str>,
    /// Normalized SQL string. See [`HandlerArgs::normalize_sql`].
    normalized_sql: String,
    /// Explain options
    explain_options: ExplainOptions,
    /// Store the trace of optimizer
    optimizer_trace: RefCell<Vec<String>>,
    /// Store the optimized logical plan of optimizer
    logical_explain: RefCell<Option<String>>,
    /// Store options or properties from the `with` clause
    with_options: WithOptions,
    /// Store the Session Timezone and whether it was used.
    session_timezone: RefCell<SessionTimezone>,
    /// Total number of optimization rules have been applied.
    total_rule_applied: RefCell<usize>,
    /// Store the configs can be overwritten in with clause
    /// if not specified, use the value from session variable.
    overwrite_options: OverwriteOptions,
    /// Store the mapping between `share_id` and the corresponding
    /// `PlanRef`, used by rcte's planning. (e.g., in `LogicalCteRef`)
    rcte_cache: RefCell<HashMap<ShareId, PlanRef>>,

    /// Last assigned plan node ID.
    last_plan_node_id: Cell<i32>,
    /// Last assigned correlated ID.
    last_correlated_id: Cell<u32>,
    /// Last assigned expr display ID.
    last_expr_display_id: Cell<usize>,
    /// Last assigned watermark group ID.
    last_watermark_group_id: Cell<u32>,

    _phantom: PhantomUnsend,
}

pub(in crate::optimizer) struct LastAssignedIds {
    last_plan_node_id: i32,
    last_correlated_id: u32,
    last_expr_display_id: usize,
    last_watermark_group_id: u32,
}

pub type OptimizerContextRef = Rc<OptimizerContext>;

impl OptimizerContext {
    /// Create a new [`OptimizerContext`] from the given [`HandlerArgs`], with empty
    /// [`ExplainOptions`].
    pub fn from_handler_args(handler_args: HandlerArgs) -> Self {
        Self::new(handler_args, ExplainOptions::default())
    }

    /// Create a new [`OptimizerContext`] from the given [`HandlerArgs`] and [`ExplainOptions`].
    pub fn new(mut handler_args: HandlerArgs, explain_options: ExplainOptions) -> Self {
        let session_timezone = RefCell::new(SessionTimezone::new(
            handler_args.session.config().timezone().to_owned(),
        ));
        let overwrite_options = OverwriteOptions::new(&mut handler_args);
        Self {
            session_ctx: handler_args.session,
            sql: handler_args.sql,
            normalized_sql: handler_args.normalized_sql,
            explain_options,
            optimizer_trace: RefCell::new(vec![]),
            logical_explain: RefCell::new(None),
            with_options: handler_args.with_options,
            session_timezone,
            total_rule_applied: RefCell::new(0),
            overwrite_options,
            rcte_cache: RefCell::new(HashMap::new()),

            last_plan_node_id: Cell::new(RESERVED_ID_NUM.into()),
            last_correlated_id: Cell::new(0),
            last_expr_display_id: Cell::new(RESERVED_ID_NUM.into()),
            last_watermark_group_id: Cell::new(RESERVED_ID_NUM.into()),

            _phantom: Default::default(),
        }
    }

    // TODO(TaoWu): Remove the async.
    #[cfg(test)]
    #[expect(clippy::unused_async)]
    pub async fn mock() -> OptimizerContextRef {
        Self {
            session_ctx: Arc::new(SessionImpl::mock()),
            sql: Arc::from(""),
            normalized_sql: "".to_owned(),
            explain_options: ExplainOptions::default(),
            optimizer_trace: RefCell::new(vec![]),
            logical_explain: RefCell::new(None),
            with_options: Default::default(),
            session_timezone: RefCell::new(SessionTimezone::new("UTC".into())),
            total_rule_applied: RefCell::new(0),
            overwrite_options: OverwriteOptions::default(),
            rcte_cache: RefCell::new(HashMap::new()),

            last_plan_node_id: Cell::new(0),
            last_correlated_id: Cell::new(0),
            last_expr_display_id: Cell::new(0),
            last_watermark_group_id: Cell::new(0),

            _phantom: Default::default(),
        }
        .into()
    }

    pub fn next_plan_node_id(&self) -> PlanNodeId {
        PlanNodeId(self.last_plan_node_id.update(|id| id + 1))
    }

    pub fn next_correlated_id(&self) -> CorrelatedId {
        self.last_correlated_id.update(|id| id + 1)
    }

    pub fn next_expr_display_id(&self) -> usize {
        self.last_expr_display_id.update(|id| id + 1)
    }

    pub fn next_watermark_group_id(&self) -> WatermarkGroupId {
        self.last_watermark_group_id.update(|id| id + 1)
    }

    pub(in crate::optimizer) fn backup_elem_ids(&self) -> LastAssignedIds {
        LastAssignedIds {
            last_plan_node_id: self.last_plan_node_id.get(),
            last_correlated_id: self.last_correlated_id.get(),
            last_expr_display_id: self.last_expr_display_id.get(),
            last_watermark_group_id: self.last_watermark_group_id.get(),
        }
    }

    /// This should only be called in [`crate::optimizer::plan_node::reorganize_elements_id`].
    pub(in crate::optimizer) fn reset_elem_ids(&self) {
        self.last_plan_node_id.set(0);
        self.last_correlated_id.set(0);
        self.last_expr_display_id.set(0);
        self.last_watermark_group_id.set(0);
    }

    pub(in crate::optimizer) fn restore_elem_ids(&self, backup: LastAssignedIds) {
        self.last_plan_node_id.set(backup.last_plan_node_id);
        self.last_correlated_id.set(backup.last_correlated_id);
        self.last_expr_display_id.set(backup.last_expr_display_id);
        self.last_watermark_group_id
            .set(backup.last_watermark_group_id);
    }

    pub fn add_rule_applied(&self, num: usize) {
        *self.total_rule_applied.borrow_mut() += num;
    }

    pub fn total_rule_applied(&self) -> usize {
        *self.total_rule_applied.borrow()
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

    pub fn explain_format(&self) -> ExplainFormat {
        self.explain_options.explain_format.clone()
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
        tracing::trace!(target: "explain_trace", "\n{}", string);
        optimizer_trace.push(string);
        optimizer_trace.push("\n".to_owned());
    }

    pub fn warn_to_user(&self, str: impl Into<String>) {
        self.session_ctx().notice_to_user(str);
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

    pub fn overwrite_options(&self) -> &OverwriteOptions {
        &self.overwrite_options
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

    pub fn get_session_timezone(&self) -> String {
        self.session_timezone.borrow().timezone()
    }

    pub fn get_rcte_cache_plan(&self, id: &ShareId) -> Option<PlanRef> {
        self.rcte_cache.borrow().get(id).cloned()
    }

    pub fn insert_rcte_cache_plan(&self, id: ShareId, plan: PlanRef) {
        self.rcte_cache.borrow_mut().insert(id, plan);
    }
}

impl std::fmt::Debug for OptimizerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext {{ sql = {}, explain_options = {}, with_options = {:?}, last_plan_node_id = {}, last_correlated_id = {} }}",
            self.sql,
            self.explain_options,
            self.with_options,
            self.last_plan_node_id.get(),
            self.last_correlated_id.get(),
        )
    }
}
