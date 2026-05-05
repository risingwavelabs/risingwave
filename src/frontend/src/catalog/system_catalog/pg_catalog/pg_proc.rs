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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::function_catalog::{FunctionCatalog, FunctionKind};
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;
use crate::user::user_catalog::UserCatalog;

/// The catalog `pg_proc` stores information about functions, procedures, aggregate functions, and
/// window functions (collectively also known as routines).
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-proc.html`
#[derive(Fields)]
struct PgProc {
    #[primary_key]
    oid: i32,
    proname: String,
    pronamespace: i32,
    proowner: i32,
    pronargdefaults: i32,
    // Data type of the return value, refer to pg_type.
    prorettype: i32,
    prokind: String,
    proargtypes: Vec<i32>,
    proallargtypes: Option<Vec<i32>>,
    proargnames: Option<Vec<String>>,
    // `i` = immutable, `s` = stable, `v` = volatile.
    provolatile: String,
    provariadic: i32,
    proretset: bool,
}

#[system_catalog(table, "pg_catalog.pg_proc")]
fn read_pg_proc(reader: &SysCatalogReaderImpl) -> Result<Vec<PgProc>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let user_reader = reader.user_info_reader.read_guard();
    let current_user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .expect("user not found");
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

    Ok(schemas
        .flat_map(|schema| read_pg_proc_in_schema(current_user, schema))
        .collect())
}

fn read_pg_proc_in_schema(current_user: &UserCatalog, schema: &SchemaCatalog) -> Vec<PgProc> {
    schema
        .iter_function_with_acl(current_user)
        .map(|function| pg_proc_row(function, schema.id().as_raw_id() as i32))
        .collect()
}

fn pg_proc_row(function: &FunctionCatalog, schema_id: i32) -> PgProc {
    let proargtypes: Vec<i32> = function.arg_types.iter().map(|ty| ty.to_oid()).collect();
    let proargnames = (!function.arg_names.is_empty()).then(|| function.arg_names.clone());

    PgProc {
        oid: function.id.as_raw_id() as i32,
        proname: function.name.clone(),
        pronamespace: schema_id,
        proowner: function.owner.as_raw_id() as i32,
        pronargdefaults: 0,
        prorettype: function.return_type.to_oid(),
        prokind: match function.kind {
            FunctionKind::Aggregate => "a",
            FunctionKind::Scalar | FunctionKind::Table => "f",
        }
        .to_owned(),
        proallargtypes: None,
        proargtypes,
        proargnames,
        // RisingWave user-defined functions do not mutate database state.
        // Mark them as stable so Hasura can track them as query-safe functions.
        provolatile: "s".to_owned(),
        provariadic: 0,
        proretset: matches!(function.kind, FunctionKind::Table),
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::FunctionId;
    use risingwave_common::types::DataType;
    use risingwave_pb::id::UserId;

    use super::*;

    #[test]
    fn test_pg_proc_row_scalar_function() {
        let function = FunctionCatalog {
            id: FunctionId::new(42),
            name: "hasura_fn".to_owned(),
            owner: UserId::new(7),
            kind: FunctionKind::Scalar,
            arg_names: vec!["a".to_owned(), "b".to_owned()],
            arg_types: vec![DataType::Int32, DataType::Varchar],
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

        let row = pg_proc_row(&function, 9);
        assert_eq!(row.oid, 42);
        assert_eq!(row.proname, "hasura_fn");
        assert_eq!(row.pronamespace, 9);
        assert_eq!(row.proowner, 7);
        assert_eq!(row.prorettype, DataType::Boolean.to_oid());
        assert_eq!(row.prokind, "f");
        assert_eq!(row.provolatile, "s");
        assert!(!row.proretset);
        assert_eq!(row.proargnames, Some(vec!["a".to_owned(), "b".to_owned()]));
        assert_eq!(
            row.proargtypes,
            vec![DataType::Int32.to_oid(), DataType::Varchar.to_oid()]
        );
    }
}
