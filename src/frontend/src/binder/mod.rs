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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_common::session_config::SearchPath;
use risingwave_sqlparser::ast::Statement;

mod bind_context;
mod common;
mod create;
mod delete;
mod expr;
mod insert;
mod query;
mod relation;
mod select;
mod set_expr;
mod statement;
mod struct_field;
mod update;
mod values;

pub use bind_context::{BindContext, LateralBindContext};
pub(crate) use common::derive_order_type_from_order_by_expr;
pub use delete::BoundDelete;
pub use expr::{bind_data_type, bind_struct_field};
pub use insert::BoundInsert;
use pgwire::pg_server::{Session, SessionId};
pub use query::BoundQuery;
pub use relation::{
    BoundBaseTable, BoundJoin, BoundShare, BoundSource, BoundSystemTable, BoundWatermark,
    BoundWindowTableFunction, Relation, WindowTableFunctionKind,
};
use risingwave_common::error::ErrorCode;
pub use select::{BoundDistinct, BoundSelect};
pub use set_expr::*;
pub use statement::BoundStatement;
pub use update::BoundUpdate;
pub use values::BoundValues;

use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::ViewId;
use crate::session::{AuthContext, SessionImpl};

pub type ShareId = usize;

/// `Binder` binds the identifiers in AST to columns in relations
pub struct Binder {
    // TODO: maybe we can only lock the database, but not the whole catalog.
    catalog: CatalogReadGuard,
    db_name: String,
    session_id: SessionId,
    context: BindContext,
    auth_context: Arc<AuthContext>,
    bind_timestamp_ms: u64,
    /// A stack holding contexts of outer queries when binding a subquery.
    /// It also holds all of the lateral contexts for each respective
    /// subquery.
    ///
    /// See [`Binder::bind_subquery_expr`] for details.
    upper_subquery_contexts: Vec<(BindContext, Vec<LateralBindContext>)>,

    /// A stack holding contexts of left-lateral `TableFactor`s.
    ///
    /// We need a separate stack as `CorrelatedInputRef` depth is
    /// determined by the upper subquery context depth, not the lateral context stack depth.
    lateral_contexts: Vec<LateralBindContext>,

    next_subquery_id: usize,
    next_values_id: usize,
    /// The `ShareId` is used to identify the share relation which could be a CTE, a source, a view
    /// and so on.
    next_share_id: ShareId,

    search_path: SearchPath,
    /// Whether the Binder is binding an MV.
    in_create_mv: bool,

    /// `ShareId`s identifying shared views.
    shared_views: HashMap<ViewId, ShareId>,
}

impl Binder {
    fn new_inner(session: &SessionImpl, in_create_mv: bool) -> Binder {
        let now_ms = session
            .env()
            .hummock_snapshot_manager()
            .latest_snapshot_current_epoch()
            .as_unix_millis();
        Binder {
            catalog: session.env().catalog_reader().read_guard(),
            db_name: session.database().to_string(),
            session_id: session.id(),
            context: BindContext::new(),
            auth_context: session.auth_context(),
            bind_timestamp_ms: now_ms,
            upper_subquery_contexts: vec![],
            lateral_contexts: vec![],
            next_subquery_id: 0,
            next_values_id: 0,
            next_share_id: 0,
            search_path: session.config().get_search_path(),
            in_create_mv,
            shared_views: HashMap::new(),
        }
    }

    pub fn new(session: &SessionImpl) -> Binder {
        Self::new_inner(session, false)
    }

    pub fn new_for_stream(session: &SessionImpl) -> Binder {
        Self::new_inner(session, true)
    }

    /// Bind a [`Statement`].
    pub fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt)
    }

    fn push_context(&mut self) {
        let new_context = std::mem::take(&mut self.context);
        self.context.cte_to_relation = new_context.cte_to_relation.clone();
        let new_lateral_contexts = std::mem::take(&mut self.lateral_contexts);
        self.upper_subquery_contexts
            .push((new_context, new_lateral_contexts));
    }

    fn pop_context(&mut self) -> Result<()> {
        let (old_context, old_lateral_contexts) = self
            .upper_subquery_contexts
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("Popping non-existent context".to_string()))?;
        self.context = old_context;
        self.lateral_contexts = old_lateral_contexts;
        Ok(())
    }

    fn push_lateral_context(&mut self) {
        let new_context = std::mem::take(&mut self.context);
        self.context.cte_to_relation = new_context.cte_to_relation.clone();
        self.lateral_contexts.push(LateralBindContext {
            is_visible: false,
            context: new_context,
        });
    }

    fn pop_and_merge_lateral_context(&mut self) -> Result<()> {
        let mut old_context = self
            .lateral_contexts
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("Popping non-existent context".to_string()))?
            .context;
        old_context.merge_context(self.context.clone())?;
        self.context = old_context;
        Ok(())
    }

    fn try_mark_lateral_as_visible(&mut self) {
        if let Some(mut ctx) = self.lateral_contexts.pop() {
            ctx.is_visible = true;
            self.lateral_contexts.push(ctx);
        }
    }

    fn try_mark_lateral_as_invisible(&mut self) {
        if let Some(mut ctx) = self.lateral_contexts.pop() {
            ctx.is_visible = false;
            self.lateral_contexts.push(ctx);
        }
    }

    fn next_subquery_id(&mut self) -> usize {
        let id = self.next_subquery_id;
        self.next_subquery_id += 1;
        id
    }

    fn next_values_id(&mut self) -> usize {
        let id = self.next_values_id;
        self.next_values_id += 1;
        id
    }

    fn next_share_id(&mut self) -> ShareId {
        let id = self.next_share_id;
        self.next_share_id += 1;
        id
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::Binder;
    use crate::session::SessionImpl;

    #[cfg(test)]
    pub fn mock_binder() -> Binder {
        Binder::new(&SessionImpl::mock())
    }
}

/// The column name stored in [`BindContext`] for a column without an alias.
pub const UNNAMED_COLUMN: &str = "?column?";
/// The table name stored in [`BindContext`] for a subquery without an alias.
const UNNAMED_SUBQUERY: &str = "?subquery?";
/// The table name stored in [`BindContext`] for a column group.
const COLUMN_GROUP_PREFIX: &str = "?column_group_id?";
