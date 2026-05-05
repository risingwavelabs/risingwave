// Copyright 2026 RisingWave Labs
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

use risingwave_common::catalog::FunctionId;
use risingwave_expr::{ExprError, Result, capture_context, function};

use super::context::{CATALOG_READER, DB_NAME};
use crate::catalog::CatalogReader;
use crate::catalog::function_catalog::FunctionCatalog;

#[function("pg_get_functiondef(int4) -> varchar")]
fn pg_get_functiondef(oid: i32, writer: &mut impl std::fmt::Write) -> Result<()> {
    pg_get_functiondef_impl_captured(oid, writer)
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn pg_get_functiondef_impl(
    catalog: &CatalogReader,
    db_name: &str,
    oid: i32,
    writer: &mut impl std::fmt::Write,
) -> Result<()> {
    let catalog_reader = catalog.read_guard();
    let function_id = FunctionId::new(oid as u32);

    let Some((schema_name, function)) =
        catalog_reader
            .iter_schemas(db_name)
            .ok()
            .and_then(|mut schemas| {
                schemas.find_map(|schema| {
                    schema
                        .get_function_by_id(function_id)
                        .map(|function| (schema.name(), function.as_ref()))
                })
            })
    else {
        return Err(ExprError::InvalidParam {
            name: "oid",
            reason: format!("function does not exist: {oid}").into(),
        });
    };

    write!(
        writer,
        "{}",
        render_function_definition(&schema_name, function)
    )
    .unwrap();
    Ok(())
}

fn render_function_definition(schema_name: &str, function: &FunctionCatalog) -> String {
    let args = function
        .arg_types
        .iter()
        .enumerate()
        .map(|(idx, ty)| match function.arg_names.get(idx) {
            Some(name) if !name.is_empty() => format!("{name} {ty}"),
            _ => format!("{ty}"),
        })
        .collect::<Vec<_>>()
        .join(", ");

    let returns = if function.kind.is_table() {
        format!("SETOF {}", function.return_type)
    } else {
        function.return_type.to_string()
    };

    let body = function.body.clone().unwrap_or_default();

    format!(
        "CREATE FUNCTION {schema_name}.{}({args}) RETURNS {returns} LANGUAGE {} AS $$\n{body}\n$$",
        function.name, function.language
    )
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::FunctionId;
    use risingwave_common::types::DataType;
    use risingwave_pb::id::UserId;

    use super::*;
    use crate::catalog::function_catalog::FunctionKind;

    #[test]
    fn test_render_function_definition() {
        let function = FunctionCatalog {
            id: FunctionId::new(42),
            name: "hasura_fn".to_owned(),
            owner: UserId::new(7),
            kind: FunctionKind::Scalar,
            arg_names: vec!["a".to_owned()],
            arg_types: vec![DataType::Int32],
            return_type: DataType::Boolean,
            language: "sql".to_owned(),
            runtime: None,
            name_in_runtime: None,
            body: Some("select $1 is not null".to_owned()),
            link: None,
            compressed_binary: None,
            always_retry_on_network_error: false,
            is_async: None,
            is_batched: None,
            created_at_epoch: None,
            created_at_cluster_version: None,
        };

        let rendered = render_function_definition("public", &function);
        assert!(rendered.contains("CREATE FUNCTION public.hasura_fn(a integer) RETURNS boolean"));
        assert!(rendered.contains("LANGUAGE sql"));
        assert!(rendered.contains("select $1 is not null"));
    }
}
