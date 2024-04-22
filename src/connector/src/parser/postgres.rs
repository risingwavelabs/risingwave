// Copyright 2024 RisingWave Labs
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

use std::str::FromStr;
use std::sync::LazyLock;

use bytes::BytesMut;
use chrono::{NaiveDate, Utc};
use pg_bigdecimal::PgNumeric;
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Date, Decimal, Int256, Interval, JsonbVal, ListValue, ScalarImpl, Time, Timestamp,
    Timestamptz,
};
use rust_decimal::Decimal as RustDecimal;
use thiserror_ext::AsReport;
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, Kind, ToSql, Type};

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);

macro_rules! handle_list_data_type {
    ($row:expr, $i:expr, $name:expr, $type:ty, $builder:expr) => {
        let res = $row.try_get::<_, Option<Vec<$type>>>($i);
        match res {
            Ok(val) => {
                if let Some(v) = val {
                    v.into_iter()
                        .for_each(|val| $builder.append(Some(ScalarImpl::from(val))))
                }
            }
            Err(err) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        column = $name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed",
                    );
                }
            }
        }
    };
    ($row:expr, $i:expr, $name:expr, $type:ty, $builder:expr, $rw_type:ty) => {
        let res = $row.try_get::<_, Option<Vec<$type>>>($i);
        match res {
            Ok(val) => {
                if let Some(v) = val {
                    v.into_iter().for_each(|val| {
                        $builder.append(Some(ScalarImpl::from(<$rw_type>::from(val))))
                    })
                }
            }
            Err(err) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        column = $name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed",
                    );
                }
            }
        }
    };
}

macro_rules! handle_data_type {
    ($row:expr, $i:expr, $name:expr, $type:ty) => {{
        let res = $row.try_get::<_, Option<$type>>($i);
        match res {
            Ok(val) => val.map(|v| ScalarImpl::from(v)),
            Err(err) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        column = $name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed",
                    );
                }
                None
            }
        }
    }};
    ($row:expr, $i:expr, $name:expr, $type:ty, $rw_type:ty) => {{
        let res = $row.try_get::<_, Option<$type>>($i);
        match res {
            Ok(val) => val.map(|v| ScalarImpl::from(<$rw_type>::from(v))),
            Err(err) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::error!(
                        column = $name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed",
                    );
                }
                None
            }
        }
    }};
}

pub fn postgres_row_to_owned_row(row: tokio_postgres::Row, schema: &Schema) -> OwnedRow {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = {
            match &rw_field.data_type {
                DataType::Boolean => {
                    handle_data_type!(row, i, name, bool)
                }
                DataType::Int16 => {
                    handle_data_type!(row, i, name, i16)
                }
                DataType::Int32 => {
                    handle_data_type!(row, i, name, i32)
                }
                DataType::Int64 => {
                    handle_data_type!(row, i, name, i64)
                }
                DataType::Float32 => {
                    handle_data_type!(row, i, name, f32)
                }
                DataType::Float64 => {
                    handle_data_type!(row, i, name, f64)
                }
                DataType::Decimal => {
                    handle_data_type!(row, i, name, RustDecimal, Decimal)
                }
                DataType::Int256 => {
                    // Currently in order to handle the decimal beyond RustDecimal,
                    // we use the PgNumeric type to convert the decimal to a string.
                    // Then we convert the string to Int256.
                    // Note: It's only used to map the numeric type in upstream Postgres to RisingWave's rw_int256.
                    let res = row.try_get::<_, Option<PgNumeric>>(i);
                    match res {
                        Ok(val) => pg_numeric_to_rw_int256(val),
                        Err(err) => {
                            if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                tracing::error!(
                                    column = name,
                                    error = %err.as_report(),
                                    suppressed_count,
                                    "parse numeric column as pg_numeric failed",
                                );
                            }
                            None
                        }
                    }
                }
                DataType::Varchar => {
                    if let Kind::Enum(_) = row.columns()[i].type_().kind() {
                        // enum type needs to be handled separately
                        let res = row.try_get::<_, Option<EnumParser>>(i);
                        match res {
                            Ok(val) => val.map(|v| ScalarImpl::from(v.value)),
                            Err(err) => {
                                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                    tracing::error!(
                                        suppressed_count,
                                        column = name,
                                        error = %err.as_report(),
                                        "parse enum column failed",
                                    );
                                }
                                None
                            }
                        }
                    } else {
                        match *row.columns()[i].type_() {
                            // Since we don't support UUID natively, adapt it to a VARCHAR column
                            Type::UUID => {
                                let res = row.try_get::<_, Option<uuid::Uuid>>(i);
                                match res {
                                    Ok(val) => val.map(|v| ScalarImpl::from(v.to_string())),
                                    Err(err) => {
                                        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                            tracing::error!(
                                                suppressed_count,
                                                column = name,
                                                error = %err.as_report(),
                                                "parse uuid column failed",
                                            );
                                        }
                                        None
                                    }
                                }
                            }
                            // we support converting NUMERIC to VARCHAR implicitly
                            Type::NUMERIC => {
                                // Currently in order to handle the decimal beyond RustDecimal,
                                // we use the PgNumeric type to convert the decimal to a string.
                                // Note: It's only used to map the numeric type in upstream Postgres to RisingWave's varchar.
                                let res = row.try_get::<_, Option<PgNumeric>>(i);
                                match res {
                                    Ok(val) => pg_numeric_to_varchar(val),
                                    Err(err) => {
                                        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                            tracing::error!(
                                                column = name,
                                                error = %err.as_report(),
                                                suppressed_count,
                                                "parse numeric column as pg_numeric failed",
                                            );
                                        }
                                        None
                                    }
                                }
                            }
                            _ => {
                                handle_data_type!(row, i, name, String)
                            }
                        }
                    }
                }
                DataType::Date => {
                    handle_data_type!(row, i, name, NaiveDate, Date)
                }
                DataType::Time => {
                    handle_data_type!(row, i, name, chrono::NaiveTime, Time)
                }
                DataType::Timestamp => {
                    handle_data_type!(row, i, name, chrono::NaiveDateTime, Timestamp)
                }
                DataType::Timestamptz => {
                    handle_data_type!(row, i, name, chrono::DateTime<Utc>, Timestamptz)
                }
                DataType::Bytea => {
                    let res = row.try_get::<_, Option<Vec<u8>>>(i);
                    match res {
                        Ok(val) => val.map(|v| ScalarImpl::from(v.into_boxed_slice())),
                        Err(err) => {
                            if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                tracing::error!(
                                    suppressed_count,
                                    column = name,
                                    error = %err.as_report(),
                                    "parse column failed",
                                );
                            }
                            None
                        }
                    }
                }
                DataType::Jsonb => {
                    handle_data_type!(row, i, name, serde_json::Value, JsonbVal)
                }
                DataType::Interval => {
                    handle_data_type!(row, i, name, Interval)
                }
                DataType::List(dtype) => {
                    let mut builder = dtype.create_array_builder(0);
                    // enum list needs to be handled separately
                    if let Kind::Array(item_type) = row.columns()[i].type_().kind()
                        && let Kind::Enum(_) = item_type.kind()
                    {
                        let res = row.try_get::<_, Option<Vec<EnumParser>>>(i);
                        match res {
                            Ok(val) => {
                                if let Some(v) = val {
                                    v.into_iter().for_each(|val| {
                                        builder.append(Some(ScalarImpl::from(val.value)))
                                    });
                                }
                            }
                            Err(err) => {
                                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                    tracing::error!(
                                        suppressed_count,
                                        column = name,
                                        error = %err.as_report(),
                                        "parse enum column failed",
                                    );
                                }
                            }
                        }
                    } else {
                        match **dtype {
                            DataType::Boolean => {
                                handle_list_data_type!(row, i, name, bool, builder);
                            }
                            DataType::Int16 => {
                                handle_list_data_type!(row, i, name, i16, builder);
                            }
                            DataType::Int32 => {
                                handle_list_data_type!(row, i, name, i32, builder);
                            }
                            DataType::Int64 => {
                                handle_list_data_type!(row, i, name, i64, builder);
                            }
                            DataType::Float32 => {
                                handle_list_data_type!(row, i, name, f32, builder);
                            }
                            DataType::Float64 => {
                                handle_list_data_type!(row, i, name, f64, builder);
                            }
                            DataType::Decimal => {
                                handle_list_data_type!(row, i, name, RustDecimal, builder, Decimal);
                            }
                            DataType::Date => {
                                handle_list_data_type!(row, i, name, NaiveDate, builder, Date);
                            }
                            DataType::Varchar => {
                                match *row.columns()[i].type_() {
                                    // Since we don't support UUID natively, adapt it to a VARCHAR column
                                    Type::UUID_ARRAY => {
                                        let res = row.try_get::<_, Option<Vec<uuid::Uuid>>>(i);
                                        match res {
                                            Ok(val) => {
                                                if let Some(v) = val {
                                                    v.into_iter().for_each(|val| {
                                                        builder.append(Some(ScalarImpl::from(
                                                            val.to_string(),
                                                        )))
                                                    });
                                                }
                                            }
                                            Err(err) => {
                                                if let Ok(suppressed_count) = LOG_SUPPERSSER.check()
                                                {
                                                    tracing::error!(
                                                        suppressed_count,
                                                        column = name,
                                                        error = %err.as_report(),
                                                        "parse uuid column failed",
                                                    );
                                                }
                                            }
                                        };
                                    }
                                    Type::NUMERIC_ARRAY => {
                                        let res = row.try_get::<_, Option<Vec<PgNumeric>>>(i);
                                        match res {
                                            Ok(val) => {
                                                if let Some(v) = val {
                                                    v.into_iter().for_each(|val| {
                                                        builder.append(pg_numeric_to_varchar(Some(
                                                            val,
                                                        )))
                                                    });
                                                }
                                            }
                                            Err(err) => {
                                                if let Ok(suppressed_count) = LOG_SUPPERSSER.check()
                                                {
                                                    tracing::error!(
                                                        suppressed_count,
                                                        column = name,
                                                        error = %err.as_report(),
                                                        "parse numeric list column as pg_numeric list failed",
                                                    );
                                                }
                                            }
                                        };
                                    }
                                    _ => {
                                        handle_list_data_type!(row, i, name, String, builder);
                                    }
                                }
                            }
                            DataType::Time => {
                                handle_list_data_type!(
                                    row,
                                    i,
                                    name,
                                    chrono::NaiveTime,
                                    builder,
                                    Time
                                );
                            }
                            DataType::Timestamp => {
                                handle_list_data_type!(
                                    row,
                                    i,
                                    name,
                                    chrono::NaiveDateTime,
                                    builder,
                                    Timestamp
                                );
                            }
                            DataType::Timestamptz => {
                                handle_list_data_type!(
                                    row,
                                    i,
                                    name,
                                    chrono::DateTime<Utc>,
                                    builder,
                                    Timestamptz
                                );
                            }
                            DataType::Interval => {
                                handle_list_data_type!(row, i, name, Interval, builder);
                            }
                            DataType::Jsonb => {
                                handle_list_data_type!(
                                    row,
                                    i,
                                    name,
                                    serde_json::Value,
                                    builder,
                                    JsonbVal
                                );
                            }
                            DataType::Bytea => {
                                let res = row.try_get::<_, Option<Vec<Vec<u8>>>>(i);
                                match res {
                                    Ok(val) => {
                                        if let Some(v) = val {
                                            v.into_iter().for_each(|val| {
                                                builder.append(Some(ScalarImpl::from(
                                                    val.into_boxed_slice(),
                                                )))
                                            })
                                        }
                                    }
                                    Err(err) => {
                                        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                            tracing::error!(
                                                suppressed_count,
                                                column = name,
                                                error = %err.as_report(),
                                                "parse column failed",
                                            );
                                        }
                                    }
                                }
                            }
                            DataType::Int256 => {
                                let res = row.try_get::<_, Option<Vec<PgNumeric>>>(i);
                                match res {
                                    Ok(val) => {
                                        if let Some(v) = val {
                                            v.into_iter().for_each(|val| {
                                                builder.append(pg_numeric_to_rw_int256(Some(val)))
                                            });
                                        }
                                    }
                                    Err(err) => {
                                        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                                            tracing::error!(
                                                suppressed_count,
                                                column = name,
                                                error = %err.as_report(),
                                                "parse numeric list column as pg_numeric list failed",
                                            );
                                        }
                                    }
                                };
                            }
                            DataType::Struct(_) | DataType::List(_) | DataType::Serial => {
                                tracing::warn!(
                                    "unsupported List data type {:?}, set the List to empty",
                                    **dtype
                                );
                            }
                        };
                    }
                    Some(ScalarImpl::from(ListValue::new(builder.finish())))
                }
                DataType::Struct(_) | DataType::Serial => {
                    // Interval, Struct, List are not supported
                    tracing::warn!(rw_field.name, ?rw_field.data_type, "unsupported data type, set to null");
                    None
                }
            }
        };
        datums.push(datum);
    }
    OwnedRow::new(datums)
}

fn pg_numeric_to_rw_int256(val: Option<PgNumeric>) -> Option<ScalarImpl> {
    let string = pg_numeric_to_string(val)?;
    match Int256::from_str(string.as_str()) {
        Ok(num) => Some(ScalarImpl::from(num)),
        Err(err) => {
            if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                tracing::error!(
                    error = %err.as_report(),
                    suppressed_count,
                    "parse numeric string as rw_int256 failed",
                );
            }
            None
        }
    }
}

fn pg_numeric_to_varchar(val: Option<PgNumeric>) -> Option<ScalarImpl> {
    pg_numeric_to_string(val).map(ScalarImpl::from)
}

fn pg_numeric_to_string(val: Option<PgNumeric>) -> Option<String> {
    if let Some(pg_numeric) = val {
        // TODO(kexiang): NEGATIVE_INFINITY -> -Infinity, POSITIVE_INFINITY -> Infinity, NAN -> NaN
        // The current implementation is to ensure consistency with the behavior of cdc event parsor.
        match pg_numeric {
            PgNumeric::NegativeInf => Some(String::from("NEGATIVE_INFINITY")),
            PgNumeric::Normalized(big_decimal) => Some(big_decimal.to_string()),
            PgNumeric::PositiveInf => Some(String::from("POSITIVE_INFINITY")),
            PgNumeric::NaN => Some(String::from("NAN")),
        }
    } else {
        // NULL
        None
    }
}

#[derive(Clone, Debug)]
struct EnumParser {
    value: String,
}

impl<'a> FromSql<'a> for EnumParser {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        Ok(EnumParser {
            value: String::from_utf8_lossy(raw).into_owned(),
        })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }
}

impl ToSql for EnumParser {
    to_sql_checked!();

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match ty.kind() {
            Kind::Enum(e) => {
                if e.contains(&self.value) {
                    out.extend_from_slice(self.value.as_bytes());
                    Ok(IsNull::No)
                } else {
                    Err(format!(
                        "EnumParser value {} is not in the enum type {:?}",
                        self.value, e
                    )
                    .into())
                }
            }
            _ => Err("EnumParser can only be used with ENUM types".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use postgres::{Client, NoTls};

    use crate::parser::postgres::EnumParser;

    const DB: &str = "postgres";
    const USER: &str = "kexiang";

    #[ignore]
    #[test]
    fn enum_parser_integration_test() {
        let connect = format!(
            "host=localhost port=5432 user={} password={} dbname={}",
            USER, DB, DB
        );
        let mut dbconn = Client::connect(connect.as_str(), NoTls).unwrap();
        // allow type existed
        let _ = dbconn.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", &[]);
        dbconn
            .execute(
                "CREATE TABLE IF NOT EXISTS person(id int PRIMARY KEY, current_mood mood)",
                &[],
            )
            .unwrap();
        dbconn.execute("DELETE FROM person;", &[]).unwrap();
        dbconn
            .execute("INSERT INTO person VALUES (1, 'happy')", &[])
            .unwrap();

        // test from_sql
        let got: EnumParser = dbconn
            .query_one("SELECT * FROM person", &[])
            .unwrap()
            .get::<usize, Option<EnumParser>>(1)
            .unwrap();
        assert_eq!("happy", got.value.as_str());

        dbconn.execute("DELETE FROM person;", &[]).unwrap();

        // test to_sql
        dbconn
            .execute("INSERT INTO foobar VALUES ($1)", &[&got])
            .unwrap();

        let got_new: EnumParser = dbconn
            .query_one("SELECT * FROM person", &[])
            .unwrap()
            .get::<usize, Option<EnumParser>>(1)
            .unwrap();
        assert_eq!("happy", got_new.value.as_str());
        dbconn.execute("DROP TABLE person", &[]).unwrap();
        dbconn.execute("DROP TYPE mood", &[]).unwrap();
    }
}
