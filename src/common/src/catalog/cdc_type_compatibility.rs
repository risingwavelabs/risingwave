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

use risingwave_pb::catalog::table::CdcTableType as PbCdcTableType;

use crate::catalog::postgres_point_type;
use crate::types::DataType;

pub fn cdc_source_column_type_compatible(
    cdc_table_type: PbCdcTableType,
    upstream_type_name: &str,
    rw_type: &DataType,
    char_max_length: Option<i64>,
    is_unsigned: bool,
    postgres_udt_name: Option<&str>,
    postgres_array_element_type_name: Option<&str>,
    postgres_array_element_udt_name: Option<&str>,
) -> bool {
    let upstream_type_name = upstream_type_name.to_ascii_lowercase();

    match cdc_table_type {
        PbCdcTableType::Mysql => mysql_source_column_type_compatible(
            &upstream_type_name,
            rw_type,
            char_max_length,
            is_unsigned,
        ),
        PbCdcTableType::Sqlserver => {
            sql_server_source_column_type_compatible(&upstream_type_name, rw_type)
        }
        PbCdcTableType::Postgres | PbCdcTableType::Citus => postgres_source_column_type_compatible(
            &upstream_type_name,
            rw_type,
            char_max_length,
            postgres_udt_name,
            postgres_array_element_type_name,
            postgres_array_element_udt_name,
        ),
        PbCdcTableType::Unspecified | PbCdcTableType::Mongo => false,
    }
}

/// Whether an existing CDC table column type can be preserved when an auto schema
/// change event reconstructs the same upstream column as `mapped_type`.
///
/// Debezium schema change events carry full table schemas, but the meta service
/// receives only the RW type already mapped from the upstream type. These rules
/// are therefore connector-specific and mirror creation-time compatibility for
/// the cases where source metadata is still distinguishable from the canonical
/// mapped RW type.
pub fn cdc_auto_schema_change_existing_type_compatible(
    cdc_table_type: PbCdcTableType,
    existing_type: &DataType,
    mapped_type: &DataType,
) -> bool {
    if existing_type == mapped_type {
        return true;
    }

    if matches!(
        cdc_table_type,
        PbCdcTableType::Postgres | PbCdcTableType::Citus
    ) && matches!(
        (existing_type, mapped_type),
        (DataType::List(_), DataType::List(_))
    ) {
        // BACKWARD COMPATIBILITY:
        // Previously, array element types were not validated at table creation,
        // if we were to validate them now on auto-schema change, thew new schema cannot be updated,
        // and will result in a schema mismatch
        return true;
    }

    auto_schema_change_source_type_candidates(cdc_table_type, mapped_type)
        .iter()
        .any(|candidate| {
            cdc_source_column_type_compatible(
                cdc_table_type,
                candidate.upstream_type_name,
                existing_type,
                candidate.char_max_length,
                candidate.is_unsigned,
                candidate.postgres_udt_name,
                None,
                None,
            )
        })
}

fn mysql_source_column_type_compatible(
    mysql_type: &str,
    rw_type: &DataType,
    char_max_length: Option<i64>,
    is_unsigned: bool,
) -> bool {
    // For creation-time validation, `mysql_type` comes from MySQL information schema DATA_TYPE,
    // with unsigned/length metadata passed separately. For auto schema change, meta only has the
    // already-mapped RW type, so callers pass one of the synthetic candidates below that uses the
    // same normalized type strings.
    if is_unsigned {
        match mysql_type {
            "tinyint" => {
                return matches!(rw_type, DataType::Int16 | DataType::Int32 | DataType::Int64);
            }
            "smallint" => return matches!(rw_type, DataType::Int32 | DataType::Int64),
            "mediumint" | "int" => return rw_type == &DataType::Int64,
            "bigint" => {
                return matches!(rw_type, DataType::Int64 | DataType::Decimal);
            }
            _ => {}
        }
    }

    match mysql_type {
        "tinyint" => {
            rw_type == &DataType::Boolean
                || matches!(rw_type, DataType::Int16 | DataType::Int32 | DataType::Int64)
        }
        "smallint" => matches!(rw_type, DataType::Int16 | DataType::Int32 | DataType::Int64),
        "mediumint" | "int" => matches!(rw_type, DataType::Int32 | DataType::Int64),
        "bigint" => matches!(rw_type, DataType::Int64 | DataType::Decimal),
        "boolean" | "bool" => rw_type == &DataType::Boolean,
        "enum" | "char" | "varchar" | "text" | "tinytext" | "mediumtext" => {
            rw_type == &DataType::Varchar
        }
        "longtext" => matches!(rw_type, DataType::Bytea | DataType::Varchar),
        "float" | "real" => matches!(rw_type, DataType::Float32 | DataType::Float64),
        "double" => rw_type == &DataType::Float64,
        "numeric" | "decimal" => rw_type == &DataType::Decimal,
        "date" => rw_type == &DataType::Date,
        "time" => rw_type == &DataType::Time,
        "datetime" => rw_type == &DataType::Timestamp,
        "timestamp" => rw_type == &DataType::Timestamptz,
        "json" => rw_type == &DataType::Jsonb,
        "bit" => {
            if char_max_length == Some(1) {
                rw_type == &DataType::Boolean
            } else {
                rw_type == &DataType::Bytea
            }
        }
        "tinyblob" | "blob" | "mediumblob" | "longblob" | "binary" | "varbinary" => {
            rw_type == &DataType::Bytea
        }
        "year" => rw_type == &DataType::Int32,
        _ => false,
    }
}

fn postgres_source_column_type_compatible(
    postgres_type: &str,
    rw_type: &DataType,
    char_max_length: Option<i64>,
    udt_name: Option<&str>,
    array_element_type_name: Option<&str>,
    array_element_udt_name: Option<&str>,
) -> bool {
    // For creation-time validation, `postgres_type` comes from PostgreSQL information schema
    // DATA_TYPE, such as ARRAY or USER-DEFINED, and is lowercased by the public entry point. For
    // auto schema change, meta only has the already-mapped RW type, so callers pass synthetic
    // candidates below that intentionally reuse these normalized validation tokens.
    match postgres_type {
        "boolean" => rw_type == &DataType::Boolean,
        "bit" => char_max_length.is_none_or(|length| length == 1) && rw_type == &DataType::Boolean,
        "smallint" => rw_type == &DataType::Int16,
        "integer" => rw_type == &DataType::Int32,
        "bigint" | "oid" => rw_type == &DataType::Int64,
        "real" => rw_type == &DataType::Float32,
        "double precision" => rw_type == &DataType::Float64,
        "character varying" | "character" | "char" => rw_type == &DataType::Varchar,
        "text" | "xml" | "uuid" | "inet" | "cidr" | "macaddr" | "macaddr8" | "int4range"
        | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            rw_type == &DataType::Varchar
        }
        "timestamp with time zone" | "timestamptz" => rw_type == &DataType::Timestamptz,
        "timestamp without time zone" | "timestamp" => rw_type == &DataType::Timestamp,
        "time with time zone" | "timetz" | "time without time zone" | "time" => {
            rw_type == &DataType::Time
        }
        "interval" => rw_type == &DataType::Interval,
        "bytea" | "geometry" | "geography" => rw_type == &DataType::Bytea,
        "json" | "jsonb" => rw_type == &DataType::Jsonb,
        "date" => rw_type == &DataType::Date,
        "numeric" => matches!(
            rw_type,
            DataType::Decimal | DataType::Int256 | DataType::Varchar
        ),
        "money" => rw_type == &DataType::Decimal,
        "point" => rw_type == &postgres_point_type(),
        "array" => {
            let (DataType::List(list_type), Some(element_type_name)) =
                (rw_type, array_element_type_name)
            else {
                return false;
            };

            postgres_source_column_type_compatible(
                &element_type_name.to_ascii_lowercase(),
                list_type.elem(),
                None,
                array_element_udt_name,
                None,
                None,
            )
        }
        "user-defined" => match udt_name.map(str::to_ascii_lowercase).as_deref() {
            Some("citext") => rw_type == &DataType::Varchar,
            Some("geometry" | "geography") => rw_type == &DataType::Bytea,
            Some("vector") => rw_type == &DataType::Vector,
            Some("ltree" | "hstore") | None => false,
            Some(_) => rw_type == &DataType::Varchar,
        },
        _ => false,
    }
}

fn sql_server_source_column_type_compatible(sql_server_type: &str, rw_type: &DataType) -> bool {
    // For creation-time validation, `sql_server_type` comes from SQL Server information schema
    // DATA_TYPE. For auto schema change, meta only has the already-mapped RW type, so callers pass
    // one of the synthetic candidates below that uses the same normalized type strings.
    match sql_server_type {
        "bit" | "boolean" => rw_type == &DataType::Boolean,
        "tinyint" | "smallint" => {
            matches!(rw_type, DataType::Int16 | DataType::Int32 | DataType::Int64)
        }
        "integer" | "int" => matches!(rw_type, DataType::Int32 | DataType::Int64),
        "bigint" => rw_type == &DataType::Int64,
        "money" | "decimal" | "numeric" => rw_type == &DataType::Decimal,
        "float" | "real" => matches!(rw_type, DataType::Float32 | DataType::Float64),
        "double" | "double precision" => rw_type == &DataType::Float64,
        "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext" | "xml"
        | "uniqueidentifier" => rw_type == &DataType::Varchar,
        "binary" | "varbinary" => rw_type == &DataType::Bytea,
        "date" => rw_type == &DataType::Date,
        "time" => rw_type == &DataType::Time,
        "datetime" | "datetime2" | "smalldatetime" => rw_type == &DataType::Timestamp,
        "datetimeoffset" => rw_type == &DataType::Timestamptz,
        _ => false,
    }
}

#[derive(Clone, Copy)]
struct SourceTypeCompatibilityInput {
    upstream_type_name: &'static str,
    char_max_length: Option<i64>,
    is_unsigned: bool,
    postgres_udt_name: Option<&'static str>,
}

impl SourceTypeCompatibilityInput {
    const fn new(upstream_type_name: &'static str) -> Self {
        Self {
            upstream_type_name,
            char_max_length: None,
            is_unsigned: false,
            postgres_udt_name: None,
        }
    }

    const fn with_char_max_length(mut self, char_max_length: i64) -> Self {
        self.char_max_length = Some(char_max_length);
        self
    }

    const fn unsigned(mut self) -> Self {
        self.is_unsigned = true;
        self
    }
}

fn auto_schema_change_source_type_candidates(
    cdc_table_type: PbCdcTableType,
    mapped_type: &DataType,
) -> Vec<SourceTypeCompatibilityInput> {
    match cdc_table_type {
        PbCdcTableType::Mysql => mysql_auto_schema_change_source_type_candidates(mapped_type),
        PbCdcTableType::Sqlserver => {
            sql_server_auto_schema_change_source_type_candidates(mapped_type)
        }
        PbCdcTableType::Postgres | PbCdcTableType::Citus => {
            postgres_auto_schema_change_source_type_candidates(mapped_type)
        }
        PbCdcTableType::Unspecified | PbCdcTableType::Mongo => vec![],
    }
}

fn mysql_auto_schema_change_source_type_candidates(
    mapped_type: &DataType,
) -> Vec<SourceTypeCompatibilityInput> {
    use SourceTypeCompatibilityInput as Candidate;

    match mapped_type {
        DataType::Boolean => vec![
            Candidate::new("bool"),
            Candidate::new("boolean"),
            Candidate::new("bit").with_char_max_length(1),
        ],
        DataType::Int16 => vec![
            Candidate::new("tinyint"),
            Candidate::new("tinyint").unsigned(),
            Candidate::new("smallint"),
        ],
        DataType::Int32 => vec![
            Candidate::new("smallint").unsigned(),
            Candidate::new("mediumint"),
            Candidate::new("int"),
            Candidate::new("year"),
        ],
        DataType::Int64 => vec![Candidate::new("int").unsigned(), Candidate::new("bigint")],
        DataType::Decimal => vec![
            Candidate::new("bigint").unsigned(),
            Candidate::new("decimal"),
            Candidate::new("numeric"),
        ],
        DataType::Varchar => vec![Candidate::new("longtext")],
        DataType::Float32 => vec![Candidate::new("float"), Candidate::new("real")],
        DataType::Float64 => vec![Candidate::new("double")],
        _ => vec![],
    }
}

fn postgres_auto_schema_change_source_type_candidates(
    mapped_type: &DataType,
) -> Vec<SourceTypeCompatibilityInput> {
    use SourceTypeCompatibilityInput as Candidate;

    match mapped_type {
        DataType::Decimal => vec![Candidate::new("numeric")],
        _ => vec![],
    }
}

fn sql_server_auto_schema_change_source_type_candidates(
    mapped_type: &DataType,
) -> Vec<SourceTypeCompatibilityInput> {
    use SourceTypeCompatibilityInput as Candidate;

    match mapped_type {
        DataType::Int16 => vec![Candidate::new("tinyint"), Candidate::new("smallint")],
        DataType::Int32 => vec![Candidate::new("int"), Candidate::new("integer")],
        DataType::Float32 => vec![Candidate::new("real")],
        DataType::Float64 => vec![Candidate::new("float")],
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_source_column_type_compatibility() {
        assert!(cdc_source_column_type_compatible(
            PbCdcTableType::Mysql,
            "int",
            &DataType::Int64,
            None,
            true,
            None,
            None,
            None,
        ));
        assert!(!cdc_source_column_type_compatible(
            PbCdcTableType::Mysql,
            "int",
            &DataType::Int32,
            None,
            true,
            None,
            None,
            None,
        ));
        assert!(cdc_source_column_type_compatible(
            PbCdcTableType::Mysql,
            "bit",
            &DataType::Boolean,
            Some(1),
            false,
            None,
            None,
            None,
        ));
        assert!(cdc_source_column_type_compatible(
            PbCdcTableType::Mysql,
            "bit",
            &DataType::Bytea,
            Some(8),
            false,
            None,
            None,
            None,
        ));
    }

    #[test]
    fn test_postgres_source_column_type_compatibility() {
        assert!(cdc_source_column_type_compatible(
            PbCdcTableType::Postgres,
            "numeric",
            &DataType::Int256,
            None,
            false,
            None,
            None,
            None,
        ));
        assert!(cdc_source_column_type_compatible(
            PbCdcTableType::Postgres,
            "USER-DEFINED",
            &DataType::Varchar,
            None,
            false,
            Some("mood"),
            None,
            None,
        ));
        assert!(!cdc_source_column_type_compatible(
            PbCdcTableType::Postgres,
            "bit",
            &DataType::Bytea,
            Some(8),
            false,
            None,
            None,
            None,
        ));
        assert!(cdc_source_column_type_compatible(
            PbCdcTableType::Postgres,
            "array",
            &DataType::Int32.list(),
            None,
            false,
            Some("_int4"),
            Some("integer"),
            Some("int4"),
        ));
        assert!(!cdc_source_column_type_compatible(
            PbCdcTableType::Postgres,
            "array",
            &DataType::Int64.list(),
            None,
            false,
            Some("_int4"),
            Some("integer"),
            Some("int4"),
        ));
    }

    #[test]
    fn test_sql_server_source_column_type_compatibility() {
        assert!(cdc_source_column_type_compatible(
            PbCdcTableType::Sqlserver,
            "int",
            &DataType::Int64,
            None,
            false,
            None,
            None,
            None,
        ));
        assert!(cdc_source_column_type_compatible(
            PbCdcTableType::Sqlserver,
            "real",
            &DataType::Float64,
            None,
            false,
            None,
            None,
            None,
        ));
        assert!(!cdc_source_column_type_compatible(
            PbCdcTableType::Sqlserver,
            "bigint",
            &DataType::Decimal,
            None,
            false,
            None,
            None,
            None,
        ));
    }

    #[test]
    fn test_mysql_cdc_auto_schema_change_existing_type_compatibility() {
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Mysql,
            &DataType::Boolean,
            &DataType::Int16,
        ));
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Mysql,
            &DataType::Int64,
            &DataType::Int32,
        ));
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Mysql,
            &DataType::Float64,
            &DataType::Float32,
        ));
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Mysql,
            &DataType::Int64,
            &DataType::Decimal,
        ));
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Mysql,
            &DataType::Bytea,
            &DataType::Varchar,
        ));

        assert!(!cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Mysql,
            &DataType::Varchar,
            &DataType::Int32,
        ));
    }

    #[test]
    fn test_sql_server_cdc_auto_schema_change_existing_type_compatibility() {
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Sqlserver,
            &DataType::Int64,
            &DataType::Int32,
        ));
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Sqlserver,
            &DataType::Float32,
            &DataType::Float64,
        ));

        assert!(!cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Sqlserver,
            &DataType::Decimal,
            &DataType::Int64,
        ));
    }

    #[test]
    fn test_postgres_cdc_auto_schema_change_existing_type_compatibility() {
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Postgres,
            &DataType::Int256,
            &DataType::Decimal,
        ));
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Postgres,
            &DataType::Varchar,
            &DataType::Decimal,
        ));
        assert!(cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Postgres,
            &DataType::Int64.list(),
            &DataType::Int32.list(),
        ));

        assert!(!cdc_auto_schema_change_existing_type_compatible(
            PbCdcTableType::Postgres,
            &DataType::Int64,
            &DataType::Int32,
        ));
    }

    #[test]
    fn test_auto_schema_change_candidates_use_source_compatibility_type_strings() {
        let cases = [
            (PbCdcTableType::Mysql, DataType::Boolean),
            (PbCdcTableType::Mysql, DataType::Int16),
            (PbCdcTableType::Mysql, DataType::Int32),
            (PbCdcTableType::Mysql, DataType::Int64),
            (PbCdcTableType::Mysql, DataType::Decimal),
            (PbCdcTableType::Mysql, DataType::Varchar),
            (PbCdcTableType::Mysql, DataType::Float32),
            (PbCdcTableType::Mysql, DataType::Float64),
            (PbCdcTableType::Postgres, DataType::Decimal),
            (PbCdcTableType::Sqlserver, DataType::Int16),
            (PbCdcTableType::Sqlserver, DataType::Int32),
            (PbCdcTableType::Sqlserver, DataType::Float32),
            (PbCdcTableType::Sqlserver, DataType::Float64),
        ];

        for (cdc_table_type, mapped_type) in cases {
            for candidate in auto_schema_change_source_type_candidates(cdc_table_type, &mapped_type)
            {
                assert!(
                    cdc_source_column_type_compatible(
                        cdc_table_type,
                        candidate.upstream_type_name,
                        &mapped_type,
                        candidate.char_max_length,
                        candidate.is_unsigned,
                        candidate.postgres_udt_name,
                        None,
                        None,
                    ),
                    "candidate {:?} is not accepted as mapped type {:?} for {:?}",
                    candidate.upstream_type_name,
                    mapped_type,
                    cdc_table_type,
                );
            }
        }
    }
}
