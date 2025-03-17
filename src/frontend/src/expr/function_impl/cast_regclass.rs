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

use risingwave_common::session_config::SearchPath;
use risingwave_expr::{ExprError, capture_context, function};
use risingwave_sqlparser::parser::{Parser, ParserError};
use thiserror::Error;
use thiserror_ext::AsReport;

use super::context::{AUTH_CONTEXT, CATALOG_READER, DB_NAME, SEARCH_PATH};
use crate::Binder;
use crate::binder::ResolveQualifiedNameError;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::{CatalogError, CatalogReader};
use crate::session::AuthContext;

#[derive(Error, Debug)]
enum ResolveRegclassError {
    #[error("parse object name failed: {0}")]
    Parser(#[from] ParserError),
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("resolve qualified name error: {0}")]
    ResolveQualifiedName(#[from] ResolveQualifiedNameError),
}

impl From<ResolveRegclassError> for ExprError {
    fn from(e: ResolveRegclassError) -> Self {
        match e {
            ResolveRegclassError::Parser(e) => ExprError::Parse(e.to_report_string().into()),
            ResolveRegclassError::Catalog(e) => ExprError::InvalidParam {
                name: "name",
                reason: e.to_report_string().into(),
            },
            ResolveRegclassError::ResolveQualifiedName(e) => ExprError::InvalidParam {
                name: "name",
                reason: e.to_report_string().into(),
            },
        }
    }
}

#[capture_context(CATALOG_READER, AUTH_CONTEXT, SEARCH_PATH, DB_NAME)]
fn resolve_regclass_impl(
    catalog: &CatalogReader,
    auth_context: &AuthContext,
    search_path: &SearchPath,
    db_name: &str,
    class_name: &str,
) -> Result<u32, ExprError> {
    resolve_regclass_inner(catalog, auth_context, search_path, db_name, class_name)
        .map_err(Into::into)
}

fn resolve_regclass_inner(
    catalog: &CatalogReader,
    auth_context: &AuthContext,
    search_path: &SearchPath,
    db_name: &str,
    class_name: &str,
) -> Result<u32, ResolveRegclassError> {
    // We use the full parser here because this function needs to accept every legal way
    // of identifying an object in PG SQL as a valid value for the varchar
    // literal.  For example: 'foo', 'public.foo', '"my table"', and
    // '"my schema".foo' must all work as values passed pg_table_size.
    let obj = Parser::parse_object_name_str(class_name)?;

    let (schema_name, class_name) = Binder::resolve_schema_qualified_name(db_name, obj)?;
    let schema_path = SchemaPath::new(schema_name.as_deref(), search_path, &auth_context.user_name);
    Ok(catalog
        .read_guard()
        .get_id_by_class_name(db_name, schema_path, &class_name)?)
}

#[function("cast_regclass(varchar) -> int4")]
fn cast_regclass(class_name: &str) -> Result<i32, ExprError> {
    let oid = resolve_regclass_impl_captured(class_name)?;
    Ok(oid as i32)
}
