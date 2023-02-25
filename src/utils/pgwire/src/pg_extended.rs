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

use std::ops::Range;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use std::vec::IntoIter;

use anyhow::anyhow;
use bytes::Bytes;
use futures::stream::FusedStream;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use postgres_types::{FromSql, Type};
use regex::Regex;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{PsqlError, PsqlResult};
use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_message::{BeCommandCompleteMessage, BeMessage};
use crate::pg_protocol::{cstr_to_str, Conn};
use crate::pg_response::{PgResponse, RowSetResult};
use crate::pg_server::{Session, SessionManager};
use crate::types::{Format, FormatIterator, Row};

#[derive(Default)]
pub struct PgStatement {
    name: String,
    prepared_statement: PreparedStatement,
    row_description: Vec<PgFieldDescriptor>,
    is_query: bool,
}

impl PgStatement {
    pub fn new(
        name: String,
        prepared_statement: PreparedStatement,
        row_description: Vec<PgFieldDescriptor>,
        is_query: bool,
    ) -> Self {
        PgStatement {
            name,
            prepared_statement,
            row_description,
            is_query,
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn param_oid_desc(&self) -> Vec<i32> {
        self.prepared_statement
            .param_type_description()
            .into_iter()
            .map(|v| v.to_oid())
            .collect_vec()
    }

    pub fn row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_description.clone()
    }

    pub fn instance<VS>(
        &self,
        portal_name: String,
        params: &[Bytes],
        result_formats: Vec<Format>,
        param_formats: Vec<Format>,
    ) -> PsqlResult<PgPortal<VS>>
    where
        VS: Stream<Item = RowSetResult> + Unpin + Send,
    {
        let instance_query_string = self.prepared_statement.instance(params, &param_formats)?;

        let format_iter = FormatIterator::new(&result_formats, self.row_description.len())
            .map_err(|err| PsqlError::Internal(anyhow!(err)))?;
        let row_description: Vec<PgFieldDescriptor> = {
            let mut row_description = self.row_description.clone();
            row_description
                .iter_mut()
                .zip_eq_fast(format_iter)
                .for_each(|(desc, format)| {
                    if let Format::Binary = format {
                        desc.set_to_binary();
                    }
                });
            row_description
        };

        Ok(PgPortal {
            name: portal_name,
            query_string: instance_query_string,
            result_formats,
            is_query: self.is_query,
            row_description,
            result: None,
            row_cache: vec![].into_iter(),
        })
    }

    /// We define the statement start with ("select","values","show","with","describe") is query
    /// statement. Because these statement will return a result set.
    pub fn is_query(&self) -> bool {
        self.is_query
    }
}

pub struct PgPortal<VS>
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    name: String,
    query_string: String,
    result_formats: Vec<Format>,
    is_query: bool,
    row_description: Vec<PgFieldDescriptor>,
    result: Option<PgResponse<VS>>,
    row_cache: IntoIter<Row>,
}

impl<VS> PgPortal<VS>
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn query_string(&self) -> String {
        self.query_string.clone()
    }

    pub fn row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_description.clone()
    }

    /// When execute a query sql, execute will re-use the result if result will not be consumed
    /// completely. Detail can refer:https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY:~:text=Once%20a%20portal,ErrorResponse%2C%20or%20PortalSuspended.
    pub async fn execute<SM: SessionManager<VS>, S: AsyncWrite + AsyncRead + Unpin>(
        &mut self,
        session: Arc<SM::Session>,
        row_limit: usize,
        msg_stream: &mut Conn<S>,
    ) -> PsqlResult<()> {
        // Check if there is a result cache
        let result = if let Some(result) = &mut self.result {
            result
        } else {
            let result = session
                .run_statement(self.query_string.as_str(), self.result_formats.clone())
                .await
                .map_err(|err| PsqlError::ExecuteError(err))?;
            self.result = Some(result);
            self.result.as_mut().unwrap()
        };

        // Indicate all data from stream have been completely consumed.
        let mut query_end = false;
        let mut query_row_count = 0;

        if let Some(notice) = result.get_notice() {
            msg_stream.write_no_flush(&BeMessage::NoticeResponse(&notice))?;
        }

        if result.is_empty() {
            // Run the callback before sending the response.
            result.run_callback().await?;

            msg_stream.write_no_flush(&BeMessage::EmptyQueryResponse)?;
        } else if result.is_query() {
            // fetch row data
            // if row_limit is 0, fetch all rows
            // if row_limit > 0, fetch row_limit rows
            while row_limit == 0 || query_row_count < row_limit {
                if self.row_cache.len() > 0 {
                    for row in self.row_cache.by_ref() {
                        msg_stream.write_no_flush(&BeMessage::DataRow(&row))?;
                        query_row_count += 1;
                        if row_limit > 0 && query_row_count >= row_limit {
                            break;
                        }
                    }
                } else {
                    self.row_cache = if let Some(rows) = result
                        .values_stream()
                        .try_next()
                        .await
                        .map_err(|err| PsqlError::ExecuteError(err))?
                    {
                        rows.into_iter()
                    } else {
                        query_end = true;
                        break;
                    };
                }
            }
            // Check if the result is consumed completely.
            // If not, cache the result.
            if self.row_cache.len() == 0 && result.values_stream().peekable().is_terminated() {
                query_end = true;
            }
            if query_end {
                // Run the callback before sending the `CommandComplete` message.
                result.run_callback().await?;

                msg_stream.write_no_flush(&BeMessage::CommandComplete(
                    BeCommandCompleteMessage {
                        stmt_type: result.get_stmt_type(),
                        rows_cnt: query_row_count as i32,
                    },
                ))?;
            } else {
                msg_stream.write_no_flush(&BeMessage::PortalSuspended)?;
            }
        } else {
            // Run the callback before sending the `CommandComplete` message.
            result.run_callback().await?;

            msg_stream.write_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: result.get_stmt_type(),
                rows_cnt: result
                    .get_effected_rows_cnt()
                    .expect("row count should be set"),
            }))?;
        }

        // If the result is consumed completely or is not a query result, clear the cache.
        if query_end || !self.result.as_ref().unwrap().is_query() {
            self.result.take();
        }

        Ok(())
    }

    /// We define the statement start with ("select","values","show","with","describe") is query
    /// statement. Because these statement will return a result set.
    pub fn is_query(&self) -> bool {
        self.is_query
    }
}

#[derive(Default)]
pub struct PreparedStatement {
    raw_statement: String,

    /// Generic param information used for simplify replace_param().
    /// Range is the start and end index of the param in raw_statement.
    ///
    /// e.g.
    /// raw_statement : "select $1,$2"
    /// param_tokens : {{1,(7..9)},{2,(10..12)}}
    param_tokens: Vec<(usize, Range<usize>)>,

    param_types: Vec<DataType>,
}

static PARAMETER_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$[0-9][0-9]*::[a-zA-Z]+[0-9]*|\$[0-9][0-8]*").unwrap());

impl PreparedStatement {
    /// parse_statement is to parse the type information from raw_statement and
    /// provided_param_types.
    ///
    /// raw_statement is the sql statement may with generic param. (e.g. "select * from table where
    /// a = $1") provided_param_types is the type information provided by user.
    ///
    /// Why we need parse:
    /// The point is user may not provided type information in provided_param_types explicitly.
    /// - They may provide in the raw_statement implicitly (e.g. "select * from table where a =
    ///   $1::INT") .
    /// - Or they don't provide. In default, We will treat these unknow type as 'VARCHAR'.
    /// So we need to integrate these param information to generate a complete type
    /// information(PreparedStatement::param_types).
    pub fn parse_statement(
        raw_statement: String,
        provided_param_oid: Vec<i32>,
    ) -> PsqlResult<Self> {
        let provided_param_types = provided_param_oid
            .iter()
            .map(|x| DataType::from_oid(*x).map_err(|e| PsqlError::ParseError(Box::new(e))))
            .collect::<PsqlResult<Vec<_>>>()?;

        let generic_params: Vec<_> = PARAMETER_PATTERN
            .find_iter(raw_statement.as_str())
            .collect();

        if generic_params.is_empty() {
            return Ok(PreparedStatement {
                raw_statement,
                param_types: provided_param_types,
                param_tokens: vec![],
            });
        }

        let mut param_tokens = Vec::with_capacity(generic_params.len());
        let mut param_records: Vec<Option<DataType>> = vec![None; 1];

        // Parse the implicit type information.
        // e.g.
        // generic_params = {"$1::VARCHAR","$2::INT4","$3"}
        // param_record will be {Some(Type::VARCHAR),Some(Type::INT4),None}
        // None means the type information isn't provided implicitly. Such as '$3' above.
        for param_match in generic_params {
            let range = param_match.range();
            let mut param = param_match.as_str().split("::");
            let param_idx = param
                .next()
                .unwrap()
                .trim_start_matches('$')
                .parse::<usize>()
                .unwrap();
            let param_type = if let Some(str) = param.next() {
                Some(DataType::from_str(str).map_err(|_| {
                    PsqlError::ParseError(format!("Invalid type name {}", str).into())
                })?)
            } else {
                None
            };
            if param_idx > param_records.len() {
                param_records.resize(param_idx, None);
            }
            param_records[param_idx - 1] = param_type;
            param_tokens.push((param_idx, range));
        }

        // Integrate the param_records and provided_param_types.
        if provided_param_types.len() > param_records.len() {
            param_records.resize(provided_param_types.len(), None);
        }
        for (idx, param_record) in param_records.iter_mut().enumerate() {
            if let Some(param_record) = param_record {
                // Check consistency of param type.
                if idx < provided_param_types.len() && provided_param_types[idx] != *param_record {
                    return Err(PsqlError::ParseError(
                        format!("Type mismatch for parameter ${}", idx).into(),
                    ));
                }
                continue;
            }
            if idx < provided_param_types.len() {
                *param_record = Some(provided_param_types[idx].clone());
            } else {
                // If the type information isn't provided implicitly or explicitly, we just assign
                // it as VARCHAR.
                *param_record = Some(DataType::Varchar);
            }
        }

        let param_types = param_records.into_iter().map(|x| x.unwrap()).collect();

        Ok(PreparedStatement {
            raw_statement,
            param_tokens,
            param_types,
        })
    }

    fn parse_params(
        type_description: &[DataType],
        raw_params: &[Bytes],
        param_formats: &[Format],
    ) -> PsqlResult<Vec<String>> {
        // Invariant check
        if type_description.len() != raw_params.len() {
            return Err(PsqlError::Internal(anyhow!(
                "The number of params doesn't match the number of types"
            )));
        }
        if raw_params.is_empty() {
            return Ok(vec![]);
        }

        let mut params = Vec::with_capacity(raw_params.len());

        let place_hodler = Type::ANY;
        let format_iter = FormatIterator::new(param_formats, raw_params.len())
            .map_err(|err| PsqlError::Internal(anyhow!(err)))?;

        for ((type_oid, raw_param), param_format) in type_description
            .iter()
            .zip_eq_fast(raw_params.iter())
            .zip_eq_fast(format_iter)
        {
            let str = match type_oid {
                DataType::Varchar | DataType::Bytea => {
                    format!("'{}'", cstr_to_str(raw_param).unwrap().replace('\'', "''"))
                }
                DataType::Boolean => match param_format {
                    Format::Binary => bool::from_sql(&place_hodler, raw_param)
                        .unwrap()
                        .to_string(),
                    Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                },
                DataType::Int64 => match param_format {
                    Format::Binary => i64::from_sql(&place_hodler, raw_param).unwrap().to_string(),
                    Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                },
                DataType::Int16 => match param_format {
                    Format::Binary => i16::from_sql(&place_hodler, raw_param).unwrap().to_string(),
                    Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                },
                DataType::Int32 => match param_format {
                    Format::Binary => i32::from_sql(&place_hodler, raw_param).unwrap().to_string(),
                    Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                },
                DataType::Float32 => {
                    let tmp = match param_format {
                        Format::Binary => {
                            f32::from_sql(&place_hodler, raw_param).unwrap().to_string()
                        }
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::FLOAT4", tmp)
                }
                DataType::Float64 => {
                    let tmp = match param_format {
                        Format::Binary => {
                            f64::from_sql(&place_hodler, raw_param).unwrap().to_string()
                        }
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::FLOAT8", tmp)
                }
                DataType::Date => {
                    let tmp = match param_format {
                        Format::Binary => chrono::NaiveDate::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string(),
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::DATE", tmp)
                }
                DataType::Time => {
                    let tmp = match param_format {
                        Format::Binary => chrono::NaiveTime::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string(),
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::TIME", tmp)
                }
                DataType::Timestamp => {
                    let tmp = match param_format {
                        Format::Binary => chrono::NaiveDateTime::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string(),
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::TIMESTAMP", tmp)
                }
                DataType::Decimal => {
                    let tmp = match param_format {
                        Format::Binary => rust_decimal::Decimal::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string(),
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::DECIMAL", tmp)
                }
                DataType::Timestamptz => {
                    let tmp = match param_format {
                        Format::Binary => {
                            chrono::DateTime::<chrono::Utc>::from_sql(&place_hodler, raw_param)
                                .unwrap()
                                .to_string()
                        }
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::TIMESTAMPTZ", tmp)
                }
                DataType::Interval => {
                    let tmp = match param_format {
                        Format::Binary => pg_interval::Interval::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_postgres(),
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::INTERVAL", tmp)
                }
                DataType::Jsonb => {
                    let tmp = match param_format {
                        Format::Binary => {
                            use risingwave_common::types::to_text::ToText as _;
                            use risingwave_common::types::Scalar as _;
                            risingwave_common::array::JsonbVal::value_deserialize(raw_param)
                                .unwrap()
                                .as_scalar_ref()
                                .to_text_with_type(&DataType::Jsonb)
                        }
                        Format::Text => cstr_to_str(raw_param).unwrap().to_string(),
                    };
                    format!("'{}'::JSONB", tmp)
                }
                DataType::Struct(_) | DataType::List { .. } => {
                    return Err(PsqlError::Internal(anyhow!(
                        "Unsupported param type {:?}",
                        type_oid
                    )))
                }
            };
            params.push(str)
        }

        Ok(params)
    }

    /// `default_params` creates default params from type oids for
    /// [`PreparedStatement::instance_default`].
    fn default_params(type_description: &[DataType]) -> PsqlResult<Vec<String>> {
        let mut params: _ = Vec::new();
        for oid in type_description.iter() {
            match oid {
                DataType::Boolean => params.push("false".to_string()),
                DataType::Int64 => params.push("0::BIGINT".to_string()),
                DataType::Int16 => params.push("0::SMALLINT".to_string()),
                DataType::Int32 => params.push("0::INT".to_string()),
                DataType::Float32 => params.push("0::FLOAT4".to_string()),
                DataType::Float64 => params.push("0::FLOAT8".to_string()),
                DataType::Bytea => params.push("'\\x0'".to_string()),
                DataType::Varchar => params.push("'0'".to_string()),
                DataType::Date => params.push("'2021-01-01'::DATE".to_string()),
                DataType::Time => params.push("'00:00:00'::TIME".to_string()),
                DataType::Timestamp => params.push("'2021-01-01 00:00:00'::TIMESTAMP".to_string()),
                DataType::Decimal => params.push("'0'::DECIMAL".to_string()),
                DataType::Timestamptz => {
                    params.push("'2022-10-01 12:00:00+01:00'::timestamptz".to_string())
                }
                DataType::Interval => params.push("'2 months ago'::interval".to_string()),
                DataType::Jsonb => params.push("'null'::JSONB".to_string()),
                DataType::Struct(_) | DataType::List { .. } => {
                    return Err(PsqlError::Internal(anyhow!(
                        "Unsupported param type {:?}",
                        oid
                    )))
                }
            };
        }
        Ok(params)
    }

    // replace_params replaces the generic params in the raw statement with the given params.
    // Our replace algorithm:
    // param_tokens is a vec of (param_index, param_range) in the raw statement.
    // We sort the param_tokens by param_range.start to get a vec of range sorted from left to
    // right. Our purpose is to split the raw statement into several parts:
    //   [normal part1] [generic param1] [normal part2] [generic param2] [generic param3]
    // Then we create the result statement:
    //   For normal part, we just copy it from the raw statement.
    //   For generic param, we replace it with the given param.
    fn replace_params(&self, params: &[String]) -> String {
        let tmp = &self.raw_statement;

        let ranges: Vec<_> = self
            .param_tokens
            .iter()
            .sorted_by(|a, b| a.1.start.cmp(&b.1.start))
            .collect();

        let mut start_offset = 0;
        let mut res = String::new();
        for (idx, range) in ranges {
            let param = &params[*idx - 1];
            res.push_str(&tmp[start_offset..range.start]);
            res.push_str(param);
            start_offset = range.end;
        }
        res.push_str(&tmp[start_offset..]);

        res
    }

    pub fn param_type_description(&self) -> Vec<DataType> {
        self.param_types.clone()
    }

    /// `instance_default` used in parse phase.
    /// At parse phase, user still do not provide params but we need to infer the sql result.(The
    /// session can't support infer the sql with generic param now). Hence to get a sql without
    /// generic param, we used `default_params()` to generate default params according `param_types`
    /// and replace the generic param with them.
    pub fn instance_default(&self) -> PsqlResult<String> {
        let default_params = Self::default_params(&self.param_types)?;
        Ok(self.replace_params(&default_params))
    }

    pub fn instance(&self, raw_params: &[Bytes], param_formats: &[Format]) -> PsqlResult<String> {
        let params = Self::parse_params(&self.param_types, raw_params, param_formats)?;
        Ok(self.replace_params(&params))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use pg_interval::Interval;
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use postgres_types::private::BytesMut;
    use risingwave_common::types::{
        DataType, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper,
    };
    use tokio_postgres::types::{ToSql, Type};

    use crate::pg_extended::PreparedStatement;
    use crate::types::Format;

    #[test]
    fn test_prepared_statement_without_param() {
        let raw_statement = "SELECT * FROM test_table".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table" == default_sql);
        let sql = prepared_statement.instance(&[], &[]).unwrap();
        assert!("SELECT * FROM test_table" == sql);
    }

    #[test]
    fn test_prepared_statement_with_explicit_param() {
        let raw_statement = "SELECT * FROM test_table WHERE id = $1".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![DataType::INT32.to_oid()])
                .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table WHERE id = 0::INT" == default_sql);
        let sql = prepared_statement.instance(&["1".into()], &[]).unwrap();
        assert!("SELECT * FROM test_table WHERE id = 1" == sql);

        let raw_statement = "INSERT INTO test (index,data) VALUES ($1,$2)".to_string();
        let prepared_statement = PreparedStatement::parse_statement(
            raw_statement,
            vec![DataType::INT32.to_oid(), DataType::VARCHAR.to_oid()],
        )
        .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("INSERT INTO test (index,data) VALUES (0::INT,'0')" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], &[])
            .unwrap();
        assert!("INSERT INTO test (index,data) VALUES (1,'DATA')" == sql);

        let raw_statement = "UPDATE COFFEES SET SALES = $1 WHERE COF_NAME LIKE $2".to_string();
        let prepared_statement = PreparedStatement::parse_statement(
            raw_statement,
            vec![DataType::INT32.to_oid(), DataType::VARCHAR.to_oid()],
        )
        .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("UPDATE COFFEES SET SALES = 0::INT WHERE COF_NAME LIKE '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], &[])
            .unwrap();
        assert!("UPDATE COFFEES SET SALES = 1 WHERE COF_NAME LIKE 'DATA'" == sql);

        let raw_statement = "SELECT * FROM test_table WHERE id = $1 AND name = $3".to_string();
        let prepared_statement = PreparedStatement::parse_statement(
            raw_statement,
            vec![
                DataType::INT32.to_oid(),
                DataType::VARCHAR.to_oid(),
                DataType::VARCHAR.to_oid(),
            ],
        )
        .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table WHERE id = 0::INT AND name = '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into(), "NAME".into()], &[])
            .unwrap();
        assert!("SELECT * FROM test_table WHERE id = 1 AND name = 'NAME'" == sql);
    }

    #[test]
    fn test_prepared_statement_with_implicit_param() {
        let raw_statement = "SELECT * FROM test_table WHERE id = $1::INT".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table WHERE id = 0::INT" == default_sql);
        let sql = prepared_statement.instance(&["1".into()], &[]).unwrap();
        assert!("SELECT * FROM test_table WHERE id = 1" == sql);

        let raw_statement =
            "INSERT INTO test (index,data) VALUES ($1::INT4,$2::VARCHAR)".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("INSERT INTO test (index,data) VALUES (0::INT,'0')" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], &[])
            .unwrap();
        assert!("INSERT INTO test (index,data) VALUES (1,'DATA')" == sql);

        let raw_statement =
            "UPDATE COFFEES SET SALES = $1::INT WHERE COF_NAME LIKE $2::VARCHAR".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("UPDATE COFFEES SET SALES = 0::INT WHERE COF_NAME LIKE '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], &[])
            .unwrap();
        assert!("UPDATE COFFEES SET SALES = 1 WHERE COF_NAME LIKE 'DATA'" == sql);
    }

    #[test]
    fn test_prepared_statement_with_mix_param() {
        let raw_statement =
            "SELECT * FROM test_table WHERE id = $1 AND name = $2::VARCHAR".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![DataType::INT32.to_oid()])
                .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table WHERE id = 0::INT AND name = '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], &[])
            .unwrap();
        assert!("SELECT * FROM test_table WHERE id = 1 AND name = 'DATA'" == sql);

        let raw_statement = "INSERT INTO test (index,data) VALUES ($1,$2)".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![DataType::INT32.to_oid()])
                .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("INSERT INTO test (index,data) VALUES (0::INT,'0')" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], &[])
            .unwrap();
        assert!("INSERT INTO test (index,data) VALUES (1,'DATA')" == sql);

        let raw_statement =
            "UPDATE COFFEES SET SALES = $1 WHERE COF_NAME LIKE $2::VARCHAR".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![DataType::INT32.to_oid()])
                .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("UPDATE COFFEES SET SALES = 0::INT WHERE COF_NAME LIKE '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], &[])
            .unwrap();
        assert!("UPDATE COFFEES SET SALES = 1 WHERE COF_NAME LIKE 'DATA'" == sql);

        let raw_statement = "SELECT $1,$2;".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let sql = prepared_statement
            .instance(&["test$2".into(), "test$1".into()], &[])
            .unwrap();
        assert!("SELECT 'test$2','test$1';" == sql);

        let raw_statement = "SELECT $1,$1::INT,$2::VARCHAR,$2;".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![DataType::INT32.to_oid()])
                .unwrap();
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], &[])
            .unwrap();
        assert!("SELECT 1,1,'DATA','DATA';" == sql);
    }
    #[test]

    fn test_parse_params_text() {
        let raw_params = vec!["A".into(), "B".into(), "C".into()];
        let type_description = vec![DataType::Varchar; 3];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, &[]).unwrap();
        assert_eq!(params, vec!["'A'", "'B'", "'C'"]);

        let raw_params = vec!["false".into(), "true".into()];
        let type_description = vec![DataType::Boolean; 2];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, &[]).unwrap();
        assert_eq!(params, vec!["false", "true"]);

        let raw_params = vec!["1".into(), "2".into(), "3".into()];
        let type_description = vec![DataType::Int16, DataType::Int32, DataType::Int64];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, &[]).unwrap();
        assert_eq!(params, vec!["1", "2", "3"]);

        let raw_params = vec![
            "1.0".into(),
            "2.0".into(),
            rust_decimal::Decimal::from_f32_retain(3.0_f32)
                .unwrap()
                .to_string()
                .into(),
        ];
        let type_description = vec![DataType::Float32, DataType::Float64, DataType::Decimal];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, &[]).unwrap();
        assert_eq!(
            params,
            vec!["'1.0'::FLOAT4", "'2.0'::FLOAT8", "'3'::DECIMAL"]
        );

        let raw_params = vec![
            NaiveDateWrapper::from_ymd_uncheck(2021, 1, 1)
                .0
                .to_string()
                .into(),
            NaiveTimeWrapper::from_hms_uncheck(12, 0, 0)
                .0
                .to_string()
                .into(),
            NaiveDateTimeWrapper::from_timestamp_uncheck(1610000000, 0)
                .0
                .to_string()
                .into(),
        ];
        let type_description = vec![DataType::Date, DataType::Time, DataType::Timestamp];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, &[]).unwrap();
        assert_eq!(
            params,
            vec![
                "'2021-01-01'::DATE",
                "'12:00:00'::TIME",
                "'2021-01-07 06:13:20'::TIMESTAMP"
            ]
        );
    }

    #[test]
    fn test_parse_params_binary() {
        let place_hodler = Type::ANY;

        // Test VACHAR type.
        let raw_params = vec!["A".into(), "B".into(), "C".into()];
        let type_description = vec![DataType::Varchar; 3];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Binary])
                .unwrap();
        assert_eq!(params, vec!["'A'", "'B'", "'C'"]);

        // Test BOOLEAN type.
        let mut raw_params = vec![BytesMut::new(); 2];
        false.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        true.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![DataType::Boolean; 2];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Binary])
                .unwrap();
        assert_eq!(params, vec!["false", "true"]);

        // Test SMALLINT, INT, BIGINT type.
        let mut raw_params = vec![BytesMut::new(); 3];
        1_i16.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        2_i32.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        3_i64.to_sql(&place_hodler, &mut raw_params[2]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![DataType::Int16, DataType::Int32, DataType::Int64];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Binary])
                .unwrap();
        assert_eq!(params, vec!["1", "2", "3"]);

        // Test FLOAT4, FLOAT8, DECIMAL type.
        let mut raw_params = vec![BytesMut::new(); 3];
        1.0_f32.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        2.0_f64.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        rust_decimal::Decimal::from_f32_retain(3.0_f32)
            .unwrap()
            .to_sql(&place_hodler, &mut raw_params[2])
            .unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![DataType::Float32, DataType::Float64, DataType::Decimal];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Binary])
                .unwrap();
        assert_eq!(params, vec!["'1'::FLOAT4", "'2'::FLOAT8", "'3'::DECIMAL"]);

        let mut raw_params = vec![BytesMut::new(); 3];
        f32::NAN.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        f64::INFINITY
            .to_sql(&place_hodler, &mut raw_params[1])
            .unwrap();
        f64::NEG_INFINITY
            .to_sql(&place_hodler, &mut raw_params[2])
            .unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![DataType::Float32, DataType::Float64, DataType::Float64];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Binary])
                .unwrap();
        assert_eq!(
            params,
            vec!["'NaN'::FLOAT4", "'inf'::FLOAT8", "'-inf'::FLOAT8"]
        );

        // Test DATE, TIME, TIMESTAMP type.
        let mut raw_params = vec![BytesMut::new(); 3];
        NaiveDateWrapper::from_ymd_uncheck(2021, 1, 1)
            .0
            .to_sql(&place_hodler, &mut raw_params[0])
            .unwrap();
        NaiveTimeWrapper::from_hms_uncheck(12, 0, 0)
            .0
            .to_sql(&place_hodler, &mut raw_params[1])
            .unwrap();
        NaiveDateTimeWrapper::from_timestamp_uncheck(1610000000, 0)
            .0
            .to_sql(&place_hodler, &mut raw_params[2])
            .unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![DataType::Date, DataType::Time, DataType::Timestamp];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Binary])
                .unwrap();
        assert_eq!(
            params,
            vec![
                "'2021-01-01'::DATE",
                "'12:00:00'::TIME",
                "'2021-01-07 06:13:20'::TIMESTAMP"
            ]
        );

        // Test TIMESTAMPTZ, INTERVAL type.
        let mut raw_params = vec![BytesMut::new(); 2];
        DateTime::<Utc>::from_utc(NaiveDateTimeWrapper::from_timestamp_uncheck(1200, 0).0, Utc)
            .to_sql(&place_hodler, &mut raw_params[0])
            .unwrap();
        let interval = Interval::new(1, 1, 24000000);
        ToSql::to_sql(&interval, &place_hodler, &mut raw_params[1]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![DataType::Timestamptz, DataType::Interval];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Binary])
                .unwrap();
        assert_eq!(
            params,
            vec![
                "'1970-01-01 00:20:00 UTC'::TIMESTAMPTZ",
                "'1 mons 1 days 00:00:24'::INTERVAL"
            ]
        );
    }

    #[test]
    fn test_parse_params_mix_format() {
        let place_hodler = Type::ANY;

        // Test VACHAR type.
        let raw_params = vec!["A".into(), "B".into(), "C".into()];
        let type_description = vec![DataType::Varchar; 3];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Text; 3])
                .unwrap();
        assert_eq!(params, vec!["'A'", "'B'", "'C'"]);

        // Test BOOLEAN type.
        let mut raw_params = vec![BytesMut::new(); 2];
        false.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        true.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![DataType::Boolean; 2];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, &[Format::Binary; 2])
                .unwrap();
        assert_eq!(params, vec!["false", "true"]);

        // Test SMALLINT, INT, BIGINT type.
        let mut raw_params = vec![BytesMut::new(); 2];
        1_i16.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        2_i32.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        let mut raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        raw_params.push("3".into());
        let type_description = vec![DataType::Int16, DataType::Int32, DataType::Int64];
        let params = PreparedStatement::parse_params(
            &type_description,
            &raw_params,
            &[Format::Binary, Format::Binary, Format::Text],
        )
        .unwrap();
        assert_eq!(params, vec!["1", "2", "3"]);

        // Test FLOAT4, FLOAT8, DECIMAL type.
        let mut raw_params = vec![BytesMut::new(); 2];
        1.0_f32.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        2.0_f64.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        let mut raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        raw_params.push("TEST".into());
        let type_description = vec![DataType::Float32, DataType::Float64, DataType::VARCHAR];
        let params = PreparedStatement::parse_params(
            &type_description,
            &raw_params,
            &[Format::Binary, Format::Binary, Format::Text],
        )
        .unwrap();
        assert_eq!(params, vec!["'1'::FLOAT4", "'2'::FLOAT8", "'TEST'"]);
    }
}
