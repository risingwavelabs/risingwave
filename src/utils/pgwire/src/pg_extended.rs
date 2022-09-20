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

use std::ops::Sub;
use std::sync::Arc;
use std::vec::IntoIter;

use bytes::Bytes;
use itertools::zip_eq;
use postgres_types::{FromSql, Type};
use regex::Regex;

use crate::error::{PsqlError, PsqlResult};
use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_protocol::cstr_to_str;
use crate::pg_response::{PgResponse, StatementType};
use crate::pg_server::{BoxedError, Session, SessionManager};
use crate::types::Row;

/// default_params is to create default params:[String] from type_description:[TypeOid].
/// The params produced by this function will be used in the replace_params.
///
/// type_description is a list of type oids.
pub fn default_params(type_description: &[TypeOid]) -> PsqlResult<Vec<String>> {
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

/// parse_params is to parse raw_params:&[Bytes] into params:[String].
/// The param produced by this function will be used in the replace_params.
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
                return Err(PsqlError::BindError(
                    "Can't support Timestampz type in extended query mode".into(),
                ))
            }
            TypeOid::Interval => {
                return Err(PsqlError::BindError(
                    "Can't support Interval type in extended query mode".into(),
                ))
            }
        };
        params.push(str)
    }

    Ok(params)
}

/// repalce_params is to replace generic params in raw_query into real params.
///
/// raw_query is the query with generic params($1,$2,$3...).
/// params is the real params(e.g. ["1::INT","'A'::VARCHAR"]).
///
/// # Example
///
/// ```ignore
/// let params = vec!["'A'::VARCHAR".into(), "1::INT".into(), "2::SMALLINT".into()];
/// let res = replace_params("SELECT $3,$2,$1".to_string(), &params);
/// assert_eq!(res, "SELECT 2::SMALLINT,1::INT,'A'::VARCHAR")");
/// ```
pub fn replace_params(raw_query: String, params: &[String]) -> String {
    let mut tmp = raw_query;

    let len = params.len();
    for param_idx in 1..(len + 1) {
        let pattern =
            Regex::new(format!(r"(?P<x>\${0})(?P<y>[^\d]{{1}})|\${0}$", param_idx).as_str())
                .unwrap();
        let param = &params[param_idx.sub(1)];
        tmp = pattern
            .replace_all(&tmp, format!("{}$y", param))
            .to_string();
    }
    tmp
}

#[derive(Default)]
pub struct PgStatement {
    name: String,
    query_string: Bytes,
    type_description: Vec<TypeOid>,
    row_description: Vec<PgFieldDescriptor>,
    is_query: bool,
}

impl PgStatement {
    pub fn new(
        name: String,
        query_string: Bytes,
        type_description: Vec<TypeOid>,
        row_description: Vec<PgFieldDescriptor>,
        is_query: bool,
    ) -> Self {
        PgStatement {
            name,
            query_string,
            type_description,
            row_description,
            is_query,
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn type_desc(&self) -> Vec<TypeOid> {
        self.type_description.clone()
    }

    pub fn row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_description.clone()
    }

    async fn infer_row_description<SM: SessionManager>(
        session: Arc<SM::Session>,
        sql: &str,
    ) -> PsqlResult<Vec<PgFieldDescriptor>> {
        if sql.len() > 6 && sql[0..6].eq_ignore_ascii_case("select") {
            return session
                .infer_return_type(sql)
                .await
                .map_err(|err| PsqlError::BindError(err));
        }
        Ok(vec![])
    }

    pub async fn instance<SM: SessionManager>(
        &self,
        session: Arc<SM::Session>,
        portal_name: String,
        params: &[Bytes],
        result_format: bool,
        param_format: bool,
    ) -> PsqlResult<PgPortal> {
        let statement = cstr_to_str(&self.query_string).unwrap().to_owned();

        // params is empty(): Don't need to replace generic params($1,$2).
        if params.is_empty() {
            return Ok(PgPortal {
                name: portal_name,
                query_string: self.query_string.clone(),
                result_cache: None,
                stmt_type: None,
                row_description: self.row_desc(),
                result_format,
                is_query: self.is_query,
            });
        }

        // Convert sql with generic params into real sql.
        let params = parse_params(&self.type_description, params, param_format)?;
        let instance_query_string = replace_params(statement, &params);

        // Get row_description and return portal.
        let row_description =
            Self::infer_row_description::<SM>(session, instance_query_string.as_str()).await?;

        Ok(PgPortal {
            name: portal_name,
            query_string: Bytes::from(instance_query_string),
            result_cache: None,
            stmt_type: None,
            row_description,
            result_format,
            is_query: self.is_query,
        })
    }

    /// We define the statement start with ("select","values","show","with","describe") is query
    /// statement. Because these statement will return a result set.
    pub fn is_query(&self) -> bool {
        self.is_query
    }
}

#[derive(Default)]
pub struct PgPortal {
    name: String,
    query_string: Bytes,
    result_cache: Option<IntoIter<Row>>,
    stmt_type: Option<StatementType>,
    row_description: Vec<PgFieldDescriptor>,
    result_format: bool,
    is_query: bool,
}

impl PgPortal {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn query_string(&self) -> Bytes {
        self.query_string.clone()
    }

    pub fn row_desc(&self) -> Vec<PgFieldDescriptor> {
        self.row_description.clone()
    }

    pub async fn execute<SM: SessionManager>(
        &mut self,
        session: Arc<SM::Session>,
        row_limit: usize,
    ) -> Result<PgResponse, BoxedError> {
        if self.result_cache.is_none() {
            let process_res = session
                .run_statement(cstr_to_str(&self.query_string).unwrap(), self.result_format)
                .await;

            // Return result directly if
            // - it's not a query result.
            // - query result needn't cache. (row_limit == 0).
            if !(process_res.is_ok() && process_res.as_ref().unwrap().is_query()) || row_limit == 0
            {
                return process_res;
            }

            // Return result need to cache.
            self.stmt_type = Some(process_res.as_ref().unwrap().get_stmt_type());
            self.result_cache = Some(process_res.unwrap().values().into_iter());
        }

        // Consume row_limit row.
        let mut data_set = vec![];
        let mut row_end = false;
        for _ in 0..row_limit {
            let data = self.result_cache.as_mut().unwrap().next();
            match data {
                Some(d) => {
                    data_set.push(d);
                }
                None => {
                    row_end = true;
                    self.result_cache = None;
                    break;
                }
            }
        }

        Ok(PgResponse::new(
            self.stmt_type.unwrap(),
            data_set.len() as _,
            data_set,
            self.row_description.clone(),
            row_end,
        ))
    }

    /// We define the statement start with ("select","values","show","with","describe") is query
    /// statement. Because these statement will return a result set.
    pub fn is_query(&self) -> bool {
        self.is_query
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.

    use postgres_types::private::BytesMut;
    use tokio_postgres::types::{ToSql, Type};

    use super::{parse_params, replace_params};
    use crate::pg_field_descriptor::TypeOid;
    #[test]
    fn test_parse_params_text() {
        let raw_params = vec!["A".into(), "B".into(), "C".into()];
        let type_description = vec![TypeOid::Varchar; 3];
        let params = parse_params(&type_description, &raw_params, false).unwrap();
        assert_eq!(params, vec!["'A'", "'B'", "'C'"]);

        let raw_params = vec!["false".into(), "true".into()];
        let type_description = vec![TypeOid::Boolean; 2];
        let params = parse_params(&type_description, &raw_params, false).unwrap();
        assert_eq!(params, vec!["false", "true"]);

        let raw_params = vec!["1".into(), "2".into(), "3".into()];
        let type_description = vec![TypeOid::SmallInt, TypeOid::Int, TypeOid::BigInt];
        let params = parse_params(&type_description, &raw_params, false).unwrap();
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
        let params = parse_params(&type_description, &raw_params, false).unwrap();
        assert_eq!(params, vec!["1.0::FLOAT4", "2.0::FLOAT8", "3::DECIMAL"]);

        let raw_params = vec![
            chrono::NaiveDate::from_ymd(2021, 1, 1).to_string().into(),
            chrono::NaiveTime::from_hms(12, 0, 0).to_string().into(),
            chrono::NaiveDateTime::from_timestamp(1610000000, 0)
                .to_string()
                .into(),
        ];
        let type_description = vec![TypeOid::Date, TypeOid::Time, TypeOid::Timestamp];
        let params = parse_params(&type_description, &raw_params, false).unwrap();
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
        let params = parse_params(&type_description, &raw_params, true).unwrap();
        assert_eq!(params, vec!["'A'", "'B'", "'C'"]);

        let mut raw_params = vec![BytesMut::new(); 2];
        false.to_sql(&place_hodler, &mut raw_params[0]).unwrap();
        true.to_sql(&place_hodler, &mut raw_params[1]).unwrap();
        let raw_params = raw_params
            .into_iter()
            .map(|b| b.freeze())
            .collect::<Vec<_>>();
        let type_description = vec![TypeOid::Boolean; 2];
        let params = parse_params(&type_description, &raw_params, true).unwrap();
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
        let params = parse_params(&type_description, &raw_params, true).unwrap();
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
        let params = parse_params(&type_description, &raw_params, true).unwrap();
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
        let params = parse_params(&type_description, &raw_params, true).unwrap();
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
    fn test_replace_params() {
        let res = replace_params(
            "SELECT $3,$2,$1".to_string(),
            &["'A'".into(), "'B'".into(), "'C'".into()],
        );
        assert_eq!(res, "SELECT 'C','B','A'");

        let res = replace_params(
            "SELECT $2,$3,$1  ,$3 ,$2 ,$1     ;".to_string(),
            &["'A'".into(), "'B'".into(), "'C'".into(), "'D'".into()],
        );
        assert_eq!(res, "SELECT 'B','C','A'  ,'C' ,'B' ,'A'     ;");

        let res = replace_params(
            "SELECT $10,$2,$1;".to_string(),
            &[
                "1".into(),
                "2".into(),
                "3".into(),
                "4".into(),
                "5".into(),
                "6".into(),
                "7".into(),
                "8".into(),
                "9".into(),
                "10".into(),
            ],
        );
        assert_eq!(res, "SELECT 10,2,1;");

        let res = replace_params(
            "SELECT $2,$1,$11,$10 ,$11, $1,$12 , $2,  'He1ll2o',1;".to_string(),
            &[
                "1".into(),
                "2".into(),
                "3".into(),
                "4".into(),
                "5".into(),
                "6".into(),
                "7".into(),
                "8".into(),
                "9".into(),
                "10".into(),
                "11".into(),
                "12".into(),
            ],
        );
        assert_eq!(res, "SELECT 2,1,11,10 ,11, 1,12 , 2,  'He1ll2o',1;");

        let res = replace_params(
            "INSERT INTO nperson (name,data) VALUES ($1,$2)".to_string(),
            &["'A'".into(), "2.0::FLOAT4".into()],
        );
        assert!(res == "INSERT INTO nperson (name,data) VALUES ('A',2.0::FLOAT4)");

        let res = replace_params(
            "UPDATE COFFEES SET SALES = $1 WHERE COF_NAME LIKE $2 ".to_string(),
            &["1::INT4".into(), "2::INT8".into()],
        );
        assert!(res == "UPDATE COFFEES SET SALES = 1::INT4 WHERE COF_NAME LIKE 2::INT8 ");

        let res = replace_params(
            "SELECT * FROM TEST WHERE TIME = $1".into(),
            &["'2021-01-01'::DATE".into()],
        );
        assert!(res == "SELECT * FROM TEST WHERE TIME = '2021-01-01'::DATE");
    }
}
