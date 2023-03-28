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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_common::session_config::SearchPath;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_sqlparser::ast::Statement;

mod bind_context;
mod bind_param;
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
    /// Whether the Binder is binding an MV/SINK.
    in_streaming: bool,

    /// `ShareId`s identifying shared views.
    shared_views: HashMap<ViewId, ShareId>,

    /// The included relations while binding a query.
    including_relations: HashSet<u32>,

    param_types: ParameterTypes,
}

/// `ParameterTypes` is used to record the types of the parameters during binding. It works
/// following the rules:
/// 1. At the beginning, it contains the user specified parameters type.
/// 2. When the binder encounters a parameter, it will record it as unknown(call `record_new_param`)
/// if it didn't exist in `ParameterTypes`.
/// 3. When the binder encounters a cast on parameter, if it's a unknown type, the cast function
/// will record the target type as infer type for that parameter(call `record_infer_type`). If the
/// parameter has been inferred, the cast function will act as a normal cast.
/// 4. After bind finished:
///     (a) parameter not in `ParameterTypes` means that the user didn't specify it and it didn't
/// occur in the query. `export` will return error if there is a kind of
/// parameter. This rule is compatible with PostgreSQL
///     (b) parameter is None means that it's a unknown type. The user didn't specify it
/// and we can't infer it in the query. We will treat it as VARCHAR type finally. This rule is
/// compatible with PostgreSQL.
///     (c) parameter is Some means that it's a known type.
#[derive(Clone, Debug)]
pub struct ParameterTypes(Arc<RwLock<HashMap<u64, Option<DataType>>>>);

impl ParameterTypes {
    pub fn new(specified_param_types: Vec<DataType>) -> Self {
        let map = specified_param_types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| ((index + 1) as u64, Some(data_type)))
            .collect::<HashMap<u64, Option<DataType>>>();
        Self(Arc::new(RwLock::new(map)))
    }

    pub fn has_infer(&self, index: u64) -> bool {
        self.0.read().unwrap().get(&index).unwrap().is_some()
    }

    pub fn read_type(&self, index: u64) -> Option<DataType> {
        self.0.read().unwrap().get(&index).unwrap().clone()
    }

    pub fn record_new_param(&mut self, index: u64) {
        self.0.write().unwrap().entry(index).or_insert(None);
    }

    pub fn record_infer_type(&mut self, index: u64, data_type: DataType) {
        assert!(
            !self.has_infer(index),
            "The parameter has been inferred, should not be inferred again."
        );
        self.0
            .write()
            .unwrap()
            .get_mut(&index)
            .unwrap()
            .replace(data_type);
    }

    pub fn export(&self) -> Result<Vec<DataType>> {
        let types = self
            .0
            .read()
            .unwrap()
            .clone()
            .into_iter()
            .sorted_by_key(|(index, _)| *index)
            .collect::<Vec<_>>();

        // Check if all the parameters have been inferred.
        for ((index, _), expect_index) in types.iter().zip_eq_debug(1_u64..=types.len() as u64) {
            if *index != expect_index {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "Cannot infer the type of the parameter {}.",
                    expect_index
                ))
                .into());
            }
        }

        Ok(types
            .into_iter()
            .map(|(_, data_type)| data_type.unwrap_or(DataType::Varchar))
            .collect::<Vec<_>>())
    }
}

impl Binder {
    fn new_inner(session: &SessionImpl, in_streaming: bool, param_types: Vec<DataType>) -> Binder {
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
            in_streaming,
            shared_views: HashMap::new(),
            including_relations: HashSet::new(),
            param_types: ParameterTypes::new(param_types),
        }
    }

    pub fn new(session: &SessionImpl) -> Binder {
        Self::new_inner(session, false, vec![])
    }

    pub fn new_with_param_types(session: &SessionImpl, param_types: Vec<DataType>) -> Binder {
        Self::new_inner(session, false, param_types)
    }

    pub fn new_for_stream(session: &SessionImpl) -> Binder {
        Self::new_inner(session, true, vec![])
    }

    /// Bind a [`Statement`].
    pub fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt)
    }

    pub fn export_param_types(&self) -> Result<Vec<DataType>> {
        self.param_types.export()
    }

    pub fn including_relations(&self) -> Vec<u32> {
        self.including_relations.iter().cloned().collect_vec()
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
    use risingwave_common::types::DataType;

    use super::Binder;
    use crate::session::SessionImpl;

    #[cfg(test)]
    pub fn mock_binder() -> Binder {
        Binder::new(&SessionImpl::mock())
    }

    #[cfg(test)]
    pub fn mock_binder_with_param_types(param_types: Vec<DataType>) -> Binder {
        Binder::new_with_param_types(&SessionImpl::mock(), param_types)
    }
}

/// The column name stored in [`BindContext`] for a column without an alias.
pub const UNNAMED_COLUMN: &str = "?column?";
/// The table name stored in [`BindContext`] for a subquery without an alias.
const UNNAMED_SUBQUERY: &str = "?subquery?";
/// The table name stored in [`BindContext`] for a column group.
const COLUMN_GROUP_PREFIX: &str = "?column_group_id?";
