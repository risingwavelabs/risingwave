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

use anyhow::anyhow;

use crate::WithOptionsSecResolved;
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::UPSTREAM_SOURCE_KEY;
use crate::source::cdc::external::{
    DATABASE_NAME_KEY, SCHEMA_NAME_KEY, SchemaTableName, TABLE_NAME_KEY,
};
use crate::source::cdc::{
    CdcSourceExternalTableIdentifier, MARIADB_CDC_CONNECTOR, MYSQL_CDC_CONNECTOR,
    POSTGRES_CDC_CONNECTOR, Postgres, SQL_SERVER_CDC_CONNECTOR, SqlServer,
};

/// Derive the connect properties of a CDC table from the properties of its source and the
/// `external_table_name` in its `TABLE '...'` clause.
///
/// Returns (`connect_properties`, `external_table_name`) where `external_table_name` keeps the
/// user's spelling:
/// - For SQL Server: 'schema.table' (2 parts) gets the database name of the source prepended, so
///   that it names the table the same way as Debezium's topic, 'database.schema.table'.
/// - For MySQL/Postgres: Returns the original `external_table_name` unchanged.
pub fn derive_connect_properties_for_cdc_table(
    source_with_properties: &WithOptionsSecResolved,
    external_table_name: String,
) -> ConnectorResult<(WithOptionsSecResolved, String)> {
    let source_database_name: &str = source_with_properties
        .get("database.name")
        .ok_or_else(|| anyhow!("The source with properties does not contain 'database.name'"))?
        .as_str();
    let mut with_options = source_with_properties.clone();
    if let Some(connector) = source_with_properties.get(UPSTREAM_SOURCE_KEY) {
        match connector.as_str() {
            MYSQL_CDC_CONNECTOR | MARIADB_CDC_CONNECTOR => {
                // MySQL doesn't allow '.' in database name and table name, so we can split the
                // external table name by '.' to get the table name
                let (db_name, table_name) = external_table_name.split_once('.').ok_or_else(|| {
                    anyhow!("The upstream table name must contain database name prefix, e.g. 'database.table'")
                })?;
                // We allow multiple database names in the source definition
                if !source_database_name
                    .split(',')
                    .map(|s| s.trim())
                    .any(|name| name == db_name)
                {
                    return Err(anyhow!(
                        "The database name `{}` in the FROM clause is not included in the database name `{}` in source definition",
                        db_name,
                        source_database_name
                    ).into());
                }
                with_options.handle_update(
                    [
                        (DATABASE_NAME_KEY.into(), db_name.into()),
                        (TABLE_NAME_KEY.into(), table_name.into()),
                    ]
                    .into(),
                    Default::default(),
                )?;
                // Return original external_table_name unchanged for MySQL
                return Ok((with_options, external_table_name));
            }
            POSTGRES_CDC_CONNECTOR => {
                let (schema_name, table_name) =
                    parse_postgres_cdc_external_table_name(&external_table_name)?;

                // insert 'schema.name' into connect properties
                with_options.handle_update(
                    [
                        (SCHEMA_NAME_KEY.into(), schema_name),
                        (TABLE_NAME_KEY.into(), table_name),
                    ]
                    .into(),
                    Default::default(),
                )?;
                // Return original external_table_name unchanged for Postgres
                return Ok((with_options, external_table_name));
            }
            SQL_SERVER_CDC_CONNECTOR => {
                // SQL Server external table name must be in one of two formats:
                // 1. 'schemaName.tableName' (2 parts) - database is already specified in source
                // 2. 'databaseName.schemaName.tableName' (3 parts) - for explicit verification
                //
                // We do NOT allow single table name (e.g., 't') because:
                // - Unlike database name (already in source), schema name is NOT pre-specified
                // - User must explicitly provide schema (even if it's 'dbo')
                let invalid_format = || {
                    anyhow!(
                        "Invalid table name format '{}'. Expected 'schema.table' or 'database.schema.table'.",
                        external_table_name
                    )
                };
                let parts = SqlServer::split_quoted_identifier(&external_table_name)
                    .ok_or_else(invalid_format)?;
                let (schema_name, table_name, external_table_name) = match parts.as_slice() {
                    [db_name, schema_name, table_name] => {
                        // Format: database.schema.table
                        // Verify that the database name matches the one in source definition
                        if db_name != source_database_name {
                            return Err(anyhow!(
                                "The database name '{}' in FROM clause does not match the database name '{}' specified in source definition. \
                                 You can either use 'schema.table' format (recommended) or ensure the database name matches.",
                                db_name,
                                source_database_name
                            ).into());
                        }
                        (schema_name, table_name, external_table_name)
                    }
                    [schema_name, table_name] => {
                        // Format: schema.table (recommended)
                        // Database name is taken from source definition. It may contain `]`, so
                        // wrap it in `[]` and escape `]` to keep `database.schema.table` idempotent.
                        let external_table_name = format!(
                            "[{}].{}",
                            source_database_name.replace(']', "]]"),
                            external_table_name
                        );
                        (schema_name, table_name, external_table_name)
                    }
                    [_] => {
                        // Format: table only
                        // Reject with clear error message
                        return Err(anyhow!(
                            "Invalid table name format '{}'. For SQL Server CDC, you must specify the schema name. \
                             Use 'schema.table' format (e.g., 'dbo.{}') or 'database.schema.table' format (e.g., '{}.dbo.{}').",
                            external_table_name,
                            external_table_name,
                            source_database_name,
                            external_table_name
                        ).into());
                    }
                    _ => return Err(invalid_format().into()),
                };

                // Insert schema and table names into connector properties
                with_options.handle_update(
                    [
                        (SCHEMA_NAME_KEY.into(), schema_name.clone()),
                        (TABLE_NAME_KEY.into(), table_name.clone()),
                    ]
                    .into(),
                    Default::default(),
                )?;
                return Ok((with_options, external_table_name));
            }
            _ => {
                return Err(ConnectorError::from(anyhow!(
                    "connector {} is not supported for cdc table",
                    connector
                )));
            }
        };
    }
    Err(anyhow!("The source properties do not contain a CDC connector").into())
}

/// Parse the schema/table name from the CDC `TABLE` clause.
///
/// Column names do not need the same parsing here: wildcard schema derivation reads
/// them from PostgreSQL catalogs after the exact table has been identified.
fn parse_postgres_cdc_external_table_name(
    external_table_name: &str,
) -> ConnectorResult<(String, String)> {
    let parts = Postgres::split_quoted_identifier(external_table_name).ok_or_else(|| {
        anyhow!(
            "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
            external_table_name
        )
    })?;

    if let [schema_name, table_name] = parts.as_slice() {
        Ok((schema_name.clone(), table_name.clone()))
    } else {
        Err(
            anyhow!("The upstream table name must contain schema name prefix, e.g. 'public.table'")
                .into(),
        )
    }
}

/// Re-derive table options after a source change using the same rules as table creation.
/// An existing table must continue to identify the same upstream relation.
pub fn refresh_cdc_table_connect_properties(
    source_properties: &WithOptionsSecResolved,
    external_table_name: String,
    previous_connector: &str,
    previous_database: &str,
    previous_table: &SchemaTableName,
) -> ConnectorResult<(WithOptionsSecResolved, String)> {
    if source_properties
        .get(UPSTREAM_SOURCE_KEY)
        .map(String::as_str)
        != Some(previous_connector)
    {
        return Err(
            anyhow!("Cannot change the connector of a source with dependent CDC tables").into(),
        );
    }
    let (properties, external_table_name) =
        derive_connect_properties_for_cdc_table(source_properties, external_table_name)?;
    if properties.get(DATABASE_NAME_KEY).map(String::as_str) != Some(previous_database)
        || SchemaTableName::from_properties(&properties) != *previous_table
    {
        return Err(anyhow!(
            "Cannot change the upstream identity of CDC table '{}' through a source update",
            external_table_name
        )
        .into());
    }
    Ok((properties, external_table_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_postgres_cdc_external_table_name() {
        for (input, expected) in [
            ("public.Note", ("public", "Note")),
            ("public.\"Note\"", ("public", "Note")),
            (
                "\"Mixed.Schema\".\"Note.Table\"",
                ("Mixed.Schema", "Note.Table"),
            ),
            ("public.\"Note\"\"Archive\"", ("public", "Note\"Archive")),
        ] {
            assert_eq!(
                parse_postgres_cdc_external_table_name(input).unwrap(),
                (expected.0.to_owned(), expected.1.to_owned()),
                "input: {input}"
            );
        }

        for input in [
            "Note",
            "public.",
            ".Note",
            "public.\"Note",
            "public.\"Note\"Archive",
            "public.Note.Archive",
        ] {
            assert!(
                parse_postgres_cdc_external_table_name(input).is_err(),
                "input should be rejected: {input}"
            );
        }
    }

    fn sql_server_source_properties(database_name: &str) -> WithOptionsSecResolved {
        WithOptionsSecResolved::new(
            [
                (UPSTREAM_SOURCE_KEY, SQL_SERVER_CDC_CONNECTOR),
                (DATABASE_NAME_KEY, database_name),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect(),
            Default::default(),
        )
    }

    #[test]
    fn test_derive_sql_server_connect_properties() {
        for (database_name, input, expected_external_table_name, expected_table_name) in [
            ("repro", "dbo.orders", "[repro].dbo.orders", "orders"),
            ("repro", "repro.dbo.orders", "repro.dbo.orders", "orders"),
            (
                "repro",
                "[repro].dbo.orders",
                "[repro].dbo.orders",
                "orders",
            ),
            ("repro", "dbo.[a.b]", "[repro].dbo.[a.b]", "a.b"),
            ("repro", "repro.dbo.[a.b]", "repro.dbo.[a.b]", "a.b"),
            ("repro", "dbo.[a]]b]", "[repro].dbo.[a]]b]", "a]b"),
            ("my.db", "dbo.orders", "[my.db].dbo.orders", "orders"),
            ("x]y", "dbo.orders", "[x]]y].dbo.orders", "orders"),
        ] {
            let source_properties = sql_server_source_properties(database_name);
            let (props, external_table_name) =
                derive_connect_properties_for_cdc_table(&source_properties, input.to_owned())
                    .unwrap();
            assert_eq!(
                external_table_name, expected_external_table_name,
                "input: {input}"
            );
            assert_eq!(props.get(SCHEMA_NAME_KEY).unwrap(), "dbo", "input: {input}");
            assert_eq!(
                props.get(TABLE_NAME_KEY).unwrap(),
                expected_table_name,
                "input: {input}"
            );

            let (props_again, external_table_name_again) = derive_connect_properties_for_cdc_table(
                &source_properties,
                external_table_name.clone(),
            )
            .unwrap();
            assert_eq!(
                external_table_name_again, external_table_name,
                "input: {input}"
            );
            assert_eq!(
                props_again.into_parts().0,
                props.into_parts().0,
                "input: {input}"
            );
        }

        for input in [
            "orders",
            "other.dbo.orders",
            "repro.dbo.a.b",
            "dbo.[a.b",
            "dbo.[a]b]",
            "dbo.a[b",
        ] {
            assert!(
                derive_connect_properties_for_cdc_table(
                    &sql_server_source_properties("repro"),
                    input.to_owned()
                )
                .is_err(),
                "input should be rejected: {input}"
            );
        }
    }

    fn source_properties(connector: &str, database: &str) -> WithOptionsSecResolved {
        WithOptionsSecResolved::without_secrets(
            [
                ("connector".into(), connector.into()),
                ("database.name".into(), database.into()),
                ("schema.name".into(), "source_schema".into()),
                ("table.name".into(), "source_filter".into()),
                ("hostname".into(), "old_host".into()),
                ("password".into(), "old_password".into()),
            ]
            .into(),
        )
    }

    #[test]
    fn test_refresh_uses_creation_rules_for_all_connectors() {
        for (connector, database, name, expected_database, expected_schema, expected_table) in [
            (
                MYSQL_CDC_CONNECTOR,
                "other_db, orders_db",
                "orders_db.orders",
                "orders_db",
                "orders_db",
                "orders",
            ),
            (
                MARIADB_CDC_CONNECTOR,
                "other_db, orders_db",
                "orders_db.orders",
                "orders_db",
                "orders_db",
                "orders",
            ),
            (
                POSTGRES_CDC_CONNECTOR,
                "orders_db",
                "\"Sales.Schema\".\"Order\"\"History\"",
                "orders_db",
                "Sales.Schema",
                "Order\"History",
            ),
            (
                SQL_SERVER_CDC_CONNECTOR,
                "orders.db",
                "[Sales.Schema].[Order]]History]",
                "orders.db",
                "Sales.Schema",
                "Order]History",
            ),
        ] {
            let mut source = source_properties(connector, database);
            let (previous, name) =
                derive_connect_properties_for_cdc_table(&source, name.into()).unwrap();
            let identity = SchemaTableName::from_properties(&previous);
            assert_eq!(previous[DATABASE_NAME_KEY], expected_database);
            assert_eq!(identity.schema_name, expected_schema);
            assert_eq!(identity.table_name, expected_table);

            if matches!(connector, MYSQL_CDC_CONNECTOR | MARIADB_CDC_CONNECTOR) {
                // Removing unrelated databases and adding new ones remains valid.
                source.insert(DATABASE_NAME_KEY.into(), "orders_db, added_db".into());
            }
            let secret = risingwave_pb::secret::PbSecretRef {
                secret_id: 99.into(),
                ..Default::default()
            };
            source
                .handle_update(
                    [
                        ("hostname".into(), "new_host".into()),
                        (SCHEMA_NAME_KEY.into(), "updated_source_schema".into()),
                        (TABLE_NAME_KEY.into(), "updated_source_filter".into()),
                    ]
                    .into(),
                    [("password".into(), secret)].into(),
                )
                .unwrap();
            let (refreshed, refreshed_name) = refresh_cdc_table_connect_properties(
                &source,
                name.clone(),
                connector,
                expected_database,
                &identity,
            )
            .unwrap();
            let (created, created_name) =
                derive_connect_properties_for_cdc_table(&source, name.clone()).unwrap();
            assert_eq!(refreshed_name, created_name);
            assert_eq!(refreshed.as_plaintext(), created.as_plaintext());
            assert_eq!(refreshed.as_secret(), created.as_secret());
            assert_eq!(refreshed["hostname"], "new_host");
            assert!(!refreshed.contains_key("password"));
            assert_eq!(refreshed.as_secret()["password"], secret);

            source
                .handle_update(
                    [("password".into(), "new_password".into())].into(),
                    Default::default(),
                )
                .unwrap();
            let (refreshed, _) = refresh_cdc_table_connect_properties(
                &source,
                name,
                connector,
                expected_database,
                &identity,
            )
            .unwrap();
            assert_eq!(refreshed["password"], "new_password");
            assert!(refreshed.as_secret().is_empty());
        }
    }

    #[test]
    fn test_refresh_rejects_identity_changes() {
        for (connector, database, name) in [
            (
                MYSQL_CDC_CONNECTOR,
                "orders_db,other_db",
                "orders_db.orders",
            ),
            (
                MARIADB_CDC_CONNECTOR,
                "orders_db,other_db",
                "orders_db.orders",
            ),
            (POSTGRES_CDC_CONNECTOR, "orders_db", "sales.orders"),
            (SQL_SERVER_CDC_CONNECTOR, "orders_db", "sales.orders"),
        ] {
            let mut source = source_properties(connector, database);
            let (previous, name) =
                derive_connect_properties_for_cdc_table(&source, name.into()).unwrap();
            let identity = SchemaTableName::from_properties(&previous);
            source.insert(DATABASE_NAME_KEY.into(), "other_db".into());
            assert!(
                refresh_cdc_table_connect_properties(
                    &source,
                    name.clone(),
                    connector,
                    "orders_db",
                    &identity,
                )
                .is_err(),
                "connector: {connector}"
            );
            source.insert(DATABASE_NAME_KEY.into(), database.into());
            source.insert("connector".into(), "unsupported-cdc".into());
            assert!(
                refresh_cdc_table_connect_properties(
                    &source,
                    name,
                    connector,
                    "orders_db",
                    &identity,
                )
                .is_err()
            );
        }
        // Legacy SQL Server descriptors may still contain a two-part identifier.
        let source = source_properties(SQL_SERVER_CDC_CONNECTOR, "other_db");
        assert!(
            refresh_cdc_table_connect_properties(
                &source,
                "sales.orders".into(),
                SQL_SERVER_CDC_CONNECTOR,
                "orders_db",
                &SchemaTableName {
                    schema_name: "sales".into(),
                    table_name: "orders".into()
                },
            )
            .is_err()
        );
    }

    #[test]
    fn test_table_identifiers_override_source_secret_refs() {
        for connector in [POSTGRES_CDC_CONNECTOR, SQL_SERVER_CDC_CONNECTOR] {
            let mut source = source_properties(connector, "orders_db");
            source
                .handle_update(
                    Default::default(),
                    [
                        (
                            SCHEMA_NAME_KEY.into(),
                            risingwave_pb::secret::PbSecretRef::default(),
                        ),
                        (
                            TABLE_NAME_KEY.into(),
                            risingwave_pb::secret::PbSecretRef::default(),
                        ),
                    ]
                    .into(),
                )
                .unwrap();
            let (properties, _) =
                derive_connect_properties_for_cdc_table(&source, "sales.orders".into()).unwrap();
            assert_eq!(properties[SCHEMA_NAME_KEY], "sales");
            assert_eq!(properties[TABLE_NAME_KEY], "orders");
            assert!(properties.as_secret().is_empty());
        }
        let mut source = source_properties(MYSQL_CDC_CONNECTOR, "orders_db");
        source
            .handle_update(
                Default::default(),
                [(
                    TABLE_NAME_KEY.into(),
                    risingwave_pb::secret::PbSecretRef::default(),
                )]
                .into(),
            )
            .unwrap();
        let (properties, _) =
            derive_connect_properties_for_cdc_table(&source, "orders_db.orders".into()).unwrap();
        assert_eq!(properties[TABLE_NAME_KEY], "orders");
        assert!(properties.as_secret().is_empty());
    }
}
