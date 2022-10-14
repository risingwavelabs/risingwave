// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::vec::IntoIter;

use bytes::Bytes;
use futures::stream::FusedStream;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::zip_eq;
use postgres_types::{FromSql, Type};
use regex::Regex;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{PsqlError, PsqlResult};
use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_message::{BeCommandCompleteMessage, BeMessage};
use crate::pg_protocol::{cstr_to_str, PgStream};
use crate::pg_response::{PgResponse, RowSetResult};
use crate::pg_server::{Session, SessionManager};
use crate::types::Row;

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

    pub fn type_desc(&self) -> Vec<TypeOid> {
        self.prepared_statement.type_description()
    }

    pub fn row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_description.clone()
    }

    pub fn instance<VS>(
        &self,
        portal_name: String,
        params: &[Bytes],
        result_format: bool,
        param_format: bool,
    ) -> PsqlResult<PgPortal<VS>>
    where
        VS: Stream<Item = RowSetResult> + Unpin + Send,
    {
        let instance_query_string = self.prepared_statement.instance(params, param_format)?;

        Ok(PgPortal {
            name: portal_name,
            query_string: instance_query_string,
            result_format,
            is_query: self.is_query,
            row_description: self.row_description.clone(),
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
    result_format: bool,
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
        msg_stream: &mut PgStream<S>,
    ) -> PsqlResult<()> {
        // Check if there is a result cache
        let result = if let Some(result) = &mut self.result {
            result
        } else {
            let result = session
                .run_statement(self.query_string.as_str(), self.result_format)
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

    /// Geneirc param information used for simplify replace_param().
    ///
    /// e.g.
    /// raw_statement : "select * from table where a = $1 and b = $2::INT"
    /// parama_tokens : {{1,"$1"},{2,"$2::INT"}}
    param_tokens: HashMap<usize, String>,

    param_types: Vec<TypeOid>,
}

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
        provided_param_types: Vec<TypeOid>,
    ) -> PsqlResult<Self> {
        // Match all generic param.
        // e.g.
        // raw_statement = "select * from table where a = $1 and b = $2::INT4"
        // generic_params will be {"$1","$2::INT4"}
        let parameter_pattern =
            Regex::new(r"\$[0-9][0-9]*::[a-zA-Z]+[0-9]*|\$[0-9][0-8]*").unwrap();
        let generic_params: Vec<String> = parameter_pattern
            .find_iter(raw_statement.as_str())
            .map(|m| m.as_str().to_string())
            .collect();

        if generic_params.is_empty() {
            return Ok(PreparedStatement {
                raw_statement,
                param_types: provided_param_types,
                param_tokens: HashMap::new(),
            });
        }

        let mut param_tokens = HashMap::new();
        let mut param_records: Vec<Option<TypeOid>> = vec![None; 1];

        // Parse the implicit type information.
        // e.g.
        // generic_params = {"$1::VARCHAR","$2::INT4","$3"}
        // param_record will be {Some(Type::VARCHAR),Some(Type::INT4),None}
        // None means the type information isn't provided implicitly. Such as '$3' above.
        for param in generic_params {
            let token = param.clone();
            let mut param = param.split("::");
            let param_idx = param
                .next()
                .unwrap()
                .trim_start_matches('$')
                .parse::<usize>()
                .unwrap();
            let param_type = if let Some(str) = param.next() {
                Some(TypeOid::from_str(str).map_err(|_| {
                    PsqlError::ParseError(format!("Invalid type name {}", str).into())
                })?)
            } else {
                None
            };
            if param_idx > param_records.len() {
                param_records.resize(param_idx, None);
            }
            param_records[param_idx - 1] = param_type;
            param_tokens.insert(param_idx, token);
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
                *param_record = Some(provided_param_types[idx]);
            } else {
                // If the type information isn't provided implicitly or explicitly, we just assign
                // it as VARCHAR.
                *param_record = Some(TypeOid::Varchar);
            }
        }

        let param_types = param_records.into_iter().map(|x| x.unwrap()).collect();

        Ok(PreparedStatement {
            raw_statement,
            param_tokens,
            param_types,
        })
    }

    /// parse_params is to parse raw_params:&[Bytes] into params:[String].
    /// The param produced by this function will be used in the PreparedStatement.
    ///
    /// type_description is a list of type oids.
    /// raw_params is a list of raw params.
    /// param_format is format code : false for text, true for binary.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let raw_params = vec!["A".into(), "B".into(), "C".into()];
    /// let type_description = vec![TypeOid::Varchar; 3];
    /// let params = parse_params(&type_description, &raw_params,false);
    /// assert_eq!(params, vec!["'A'", "'B'", "'C'"])
    ///
    /// let raw_params = vec!["1".into(), "2".into(), "3.1".into()];
    /// let type_description = vec![TypeOid::INT,TypeOid::INT,TypeOid::FLOAT4];
    /// let params = parse_params(&type_description, &raw_params,false);
    /// assert_eq!(params, vec!["1::INT", "2::INT", "3.1::FLOAT4"])
    /// ```
    fn parse_params(
        type_description: &[TypeOid],
        raw_params: &[Bytes],
        param_format: bool,
    ) -> PsqlResult<Vec<String>> {
        assert_eq!(type_description.len(), raw_params.len());

        let mut params = Vec::with_capacity(raw_params.len());

        // BINARY FORMAT PARAMS
        let place_hodler = Type::ANY;
        for (type_oid, raw_param) in zip_eq(type_description.iter(), raw_params.iter()) {
            let str = match type_oid {
                TypeOid::Varchar => {
                    format!("'{}'", cstr_to_str(raw_param).unwrap())
                }
                TypeOid::Boolean => {
                    if param_format {
                        bool::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    }
                }
                TypeOid::BigInt => {
                    let tmp = if param_format {
                        i64::from_sql(&place_hodler, raw_param).unwrap().to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("{}::BIGINT", tmp)
                }
                TypeOid::SmallInt => {
                    let tmp = if param_format {
                        i16::from_sql(&place_hodler, raw_param).unwrap().to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("{}::SMALLINT", tmp)
                }
                TypeOid::Int => {
                    let tmp = if param_format {
                        i32::from_sql(&place_hodler, raw_param).unwrap().to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("{}::INT", tmp)
                }
                TypeOid::Float4 => {
                    let tmp = if param_format {
                        f32::from_sql(&place_hodler, raw_param).unwrap().to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("{}::FLOAT4", tmp)
                }
                TypeOid::Float8 => {
                    let tmp = if param_format {
                        f64::from_sql(&place_hodler, raw_param).unwrap().to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("{}::FLOAT8", tmp)
                }
                TypeOid::Date => {
                    let tmp = if param_format {
                        chrono::NaiveDate::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("'{}'::DATE", tmp)
                }
                TypeOid::Time => {
                    let tmp = if param_format {
                        chrono::NaiveTime::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("'{}'::TIME", tmp)
                }
                TypeOid::Timestamp => {
                    let tmp = if param_format {
                        chrono::NaiveDateTime::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("'{}'::TIMESTAMP", tmp)
                }
                TypeOid::Decimal => {
                    let tmp = if param_format {
                        rust_decimal::Decimal::from_sql(&place_hodler, raw_param)
                            .unwrap()
                            .to_string()
                    } else {
                        cstr_to_str(raw_param).unwrap().to_string()
                    };
                    format!("{}::DECIMAL", tmp)
                }
                TypeOid::Timestampz => {
                    if param_format {
                        return Err(PsqlError::BindError(
                            "Can't support Timestampz type in binary format".into(),
                        ));
                    } else {
                        let tmp = cstr_to_str(raw_param).unwrap().to_string();
                        format!("'{}'::TIMESTAMPZ", tmp)
                    }
                }
                TypeOid::Interval => {
                    if param_format {
                        return Err(PsqlError::BindError(
                            "Can't support Interval type in binary format".into(),
                        ));
                    } else {
                        let tmp = cstr_to_str(raw_param).unwrap().to_string();
                        format!("'{}'::INTERVAL", tmp)
                    }
                }
            };
            params.push(str)
        }

        Ok(params)
    }

    /// `default_params` creates default params from type oids for
    /// [`PreparedStatement::instance_default`].
    fn default_params(type_description: &[TypeOid]) -> PsqlResult<Vec<String>> {
        let mut params: _ = Vec::new();
        for oid in type_description.iter() {
            match oid {
                TypeOid::Boolean => params.push("false".to_string()),
                TypeOid::BigInt => params.push("0::BIGINT".to_string()),
                TypeOid::SmallInt => params.push("0::SMALLINT".to_string()),
                TypeOid::Int => params.push("0::INT".to_string()),
                TypeOid::Float4 => params.push("0::FLOAT4".to_string()),
                TypeOid::Float8 => params.push("0::FLOAT8".to_string()),
                TypeOid::Varchar => params.push("'0'".to_string()),
                TypeOid::Date => params.push("'2021-01-01'::DATE".to_string()),
                TypeOid::Time => params.push("'00:00:00'::TIME".to_string()),
                TypeOid::Timestamp => params.push("'2021-01-01 00:00:00'::TIMESTAMP".to_string()),
                TypeOid::Decimal => params.push("'0'::DECIMAL".to_string()),
                TypeOid::Timestampz => {
                    return Err(PsqlError::ParseError(
                        "Can't support Timestampz type in extended query mode".into(),
                    ))
                }
                TypeOid::Interval => {
                    return Err(PsqlError::ParseError(
                        "Can't support Interval type in extended query mode".into(),
                    ))
                }
            };
        }
        Ok(params)
    }

    fn replace_params(&self, params: &[String]) -> String {
        let mut tmp = self.raw_statement.clone();

        for (idx, generic_param) in &self.param_tokens {
            let param = &params[*idx - 1];
            tmp = tmp.replace(generic_param, param);
        }

        tmp
    }

    pub fn type_description(&self) -> Vec<TypeOid> {
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

    pub fn instance(&self, raw_params: &[Bytes], param_format: bool) -> PsqlResult<String> {
        let params = Self::parse_params(&self.param_types, raw_params, param_format)?;
        Ok(self.replace_params(&params))
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use postgres_types::private::BytesMut;
    use tokio_postgres::types::{ToSql, Type};

    use crate::pg_extended::PreparedStatement;
    use crate::pg_field_descriptor::TypeOid;

    #[test]
    fn test_prepared_statement_without_param() {
        let raw_statement = "SELECT * FROM test_table".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table" == default_sql);
        let sql = prepared_statement.instance(&[], false).unwrap();
        assert!("SELECT * FROM test_table" == sql);
    }

    #[test]
    fn test_prepared_statement_with_explicit_param() {
        let raw_statement = "SELECT * FROM test_table WHERE id = $1".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![TypeOid::Int]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table WHERE id = 0::INT" == default_sql);
        let sql = prepared_statement.instance(&["1".into()], false).unwrap();
        assert!("SELECT * FROM test_table WHERE id = 1::INT" == sql);

        let raw_statement = "INSERT INTO test (index,data) VALUES ($1,$2)".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![TypeOid::Int, TypeOid::Varchar])
                .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("INSERT INTO test (index,data) VALUES (0::INT,'0')" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], false)
            .unwrap();
        assert!("INSERT INTO test (index,data) VALUES (1::INT,'DATA')" == sql);

        let raw_statement = "UPDATE COFFEES SET SALES = $1 WHERE COF_NAME LIKE $2".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![TypeOid::Int, TypeOid::Varchar])
                .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("UPDATE COFFEES SET SALES = 0::INT WHERE COF_NAME LIKE '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], false)
            .unwrap();
        assert!("UPDATE COFFEES SET SALES = 1::INT WHERE COF_NAME LIKE 'DATA'" == sql);

        let raw_statement = "SELECT * FROM test_table WHERE id = $1 AND name = $3".to_string();
        let prepared_statement = PreparedStatement::parse_statement(
            raw_statement,
            vec![TypeOid::Int, TypeOid::Varchar, TypeOid::Varchar],
        )
        .unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table WHERE id = 0::INT AND name = '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into(), "NAME".into()], false)
            .unwrap();
        assert!("SELECT * FROM test_table WHERE id = 1::INT AND name = 'NAME'" == sql);
    }

    #[test]
    fn test_prepared_statement_with_implicit_param() {
        let raw_statement = "SELECT * FROM test_table WHERE id = $1::INT".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table WHERE id = 0::INT" == default_sql);
        let sql = prepared_statement.instance(&["1".into()], false).unwrap();
        assert!("SELECT * FROM test_table WHERE id = 1::INT" == sql);

        let raw_statement =
            "INSERT INTO test (index,data) VALUES ($1::INT4,$2::VARCHAR)".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("INSERT INTO test (index,data) VALUES (0::INT,'0')" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], false)
            .unwrap();
        assert!("INSERT INTO test (index,data) VALUES (1::INT,'DATA')" == sql);

        let raw_statement =
            "UPDATE COFFEES SET SALES = $1::INT WHERE COF_NAME LIKE $2::VARCHAR".to_string();
        let prepared_statement = PreparedStatement::parse_statement(raw_statement, vec![]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("UPDATE COFFEES SET SALES = 0::INT WHERE COF_NAME LIKE '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], false)
            .unwrap();
        assert!("UPDATE COFFEES SET SALES = 1::INT WHERE COF_NAME LIKE 'DATA'" == sql);
    }

    #[test]
    fn test_prepared_statement_with_mix_param() {
        let raw_statement =
            "SELECT * FROM test_table WHERE id = $1 AND name = $2::VARCHAR".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![TypeOid::Int]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("SELECT * FROM test_table WHERE id = 0::INT AND name = '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], false)
            .unwrap();
        assert!("SELECT * FROM test_table WHERE id = 1::INT AND name = 'DATA'" == sql);

        let raw_statement = "INSERT INTO test (index,data) VALUES ($1,$2)".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![TypeOid::Int]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("INSERT INTO test (index,data) VALUES (0::INT,'0')" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], false)
            .unwrap();
        assert!("INSERT INTO test (index,data) VALUES (1::INT,'DATA')" == sql);

        let raw_statement =
            "UPDATE COFFEES SET SALES = $1 WHERE COF_NAME LIKE $2::VARCHAR".to_string();
        let prepared_statement =
            PreparedStatement::parse_statement(raw_statement, vec![TypeOid::Int]).unwrap();
        let default_sql = prepared_statement.instance_default().unwrap();
        assert!("UPDATE COFFEES SET SALES = 0::INT WHERE COF_NAME LIKE '0'" == default_sql);
        let sql = prepared_statement
            .instance(&["1".into(), "DATA".into()], false)
            .unwrap();
        assert!("UPDATE COFFEES SET SALES = 1::INT WHERE COF_NAME LIKE 'DATA'" == sql);
    }
    #[test]

    fn test_parse_params_text() {
        let raw_params = vec!["A".into(), "B".into(), "C".into()];
        let type_description = vec![TypeOid::Varchar; 3];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, false).unwrap();
        assert_eq!(params, vec!["'A'", "'B'", "'C'"]);

        let raw_params = vec!["false".into(), "true".into()];
        let type_description = vec![TypeOid::Boolean; 2];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, false).unwrap();
        assert_eq!(params, vec!["false", "true"]);

        let raw_params = vec!["1".into(), "2".into(), "3".into()];
        let type_description = vec![TypeOid::SmallInt, TypeOid::Int, TypeOid::BigInt];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, false).unwrap();
        assert_eq!(params, vec!["1::SMALLINT", "2::INT", "3::BIGINT"]);

        let raw_params = vec![
            "1.0".into(),
            "2.0".into(),
            rust_decimal::Decimal::from_f32_retain(3.0_f32)
                .unwrap()
                .to_string()
                .into(),
        ];
        let type_description = vec![TypeOid::Float4, TypeOid::Float8, TypeOid::Decimal];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, false).unwrap();
        assert_eq!(params, vec!["1.0::FLOAT4", "2.0::FLOAT8", "3::DECIMAL"]);

        let raw_params = vec![
            chrono::NaiveDate::from_ymd(2021, 1, 1).to_string().into(),
            chrono::NaiveTime::from_hms(12, 0, 0).to_string().into(),
            chrono::NaiveDateTime::from_timestamp(1610000000, 0)
                .to_string()
                .into(),
        ];
        let type_description = vec![TypeOid::Date, TypeOid::Time, TypeOid::Timestamp];
        let params =
            PreparedStatement::parse_params(&type_description, &raw_params, false).unwrap();
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

        let raw_params = vec!["A".into(), "B".into(), "C".into()];
        let type_description = vec![TypeOid::Varchar; 3];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, true).unwrap();
        assert_eq!(params, vec!["'A'", "'B'", "'C'"]);

        let mut raw_params = vec![BytesMut::new(); 2];
        false.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        true.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![TypeOid::Boolean; 2];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, true).unwrap();
        assert_eq!(params, vec!["false", "true"]);

        let mut raw_params = vec![BytesMut::new(); 3];
        1_i16.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        2_i32.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        3_i64.to_sql(&place_hodler, &mut raw_params[2]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![TypeOid::SmallInt, TypeOid::Int, TypeOid::BigInt];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, true).unwrap();
        assert_eq!(params, vec!["1::SMALLINT", "2::INT", "3::BIGINT"]);

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
        let type_description = vec![TypeOid::Float4, TypeOid::Float8, TypeOid::Decimal];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, true).unwrap();
        assert_eq!(params, vec!["1::FLOAT4", "2::FLOAT8", "3::DECIMAL"]);

        let mut raw_params = vec![BytesMut::new(); 3];
        chrono::NaiveDate::from_ymd(2021, 1, 1)
            .to_sql(&place_hodler, &mut raw_params[0])
            .unwrap();
        chrono::NaiveTime::from_hms(12, 0, 0)
            .to_sql(&place_hodler, &mut raw_params[1])
            .unwrap();
        chrono::NaiveDateTime::from_timestamp(1610000000, 0)
            .to_sql(&place_hodler, &mut raw_params[2])
            .unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![TypeOid::Date, TypeOid::Time, TypeOid::Timestamp];
        let params = PreparedStatement::parse_params(&type_description, &raw_params, true).unwrap();
        assert_eq!(
            params,
            vec![
                "'2021-01-01'::DATE",
                "'12:00:00'::TIME",
                "'2021-01-07 06:13:20'::TIMESTAMP"
            ]
        );
    }
}
