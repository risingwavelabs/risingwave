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
use regex::Regex;

use crate::error::{PsqlError, PsqlResult};
use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_protocol::cstr_to_str;
use crate::pg_response::{PgResponse, StatementType};
use crate::pg_server::{BoxedError, Session, SessionManager};
use crate::types::Row;

/// Parse params according the type description.
///
/// # Example
///
/// ```ignore
/// let raw_params = vec!["A".into(), "B".into(), "C".into()];
/// let type_description = vec![TypeOid::Varchar; 3];
/// let params = parse_params(&type_description, &raw_params);
/// assert_eq!(params, vec!["'A'", "'B'", "'C'"])
/// ```
fn parse_params(type_description: &[TypeOid], raw_params: &[Bytes]) -> PsqlResult<Vec<String>> {
    assert_eq!(type_description.len(), raw_params.len());

    let mut params = Vec::with_capacity(raw_params.len());
    for (type_oid, raw_param) in zip_eq(type_description.iter(), raw_params.iter()) {
        let str = match type_oid {
            TypeOid::Varchar => format!("'{}'", cstr_to_str(raw_param).unwrap()),
            _ => {
                return Err(PsqlError::BindError(
                    "Unsupported type for parameter".into(),
                ))
            }
        };
        params.push(str)
    }
    Ok(params)
}

/// Replace generic params in query into real params.
///
/// # Example
///
/// ```ignore
/// let raw_params = vec!["A".into(), "B".into(), "C".into()];
/// let type_description = vec![TypeOid::Varchar; 3];
/// let params = parse_params(&type_description, &raw_params);
/// let res = replace_params("SELECT $3,$2,$1".to_string(), &[1, 2, 3], &params);
/// assert_eq!(res, "SELECT 'C','B','A'");
/// ```
fn replace_params(query_string: String, generic_params: &[usize], params: &[String]) -> String {
    let mut tmp = query_string;

    for &param_idx in generic_params {
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

        // 1. Identify all the $n. For example, "SELECT $3, $2, $1" -> [3, 2, 1].
        let parameter_pattern = Regex::new(r"\$[0-9][0-9]*").unwrap();
        let generic_params: Vec<usize> = parameter_pattern
            .find_iter(statement.as_str())
            .map(|x| {
                x.as_str()
                    .strip_prefix('$')
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            })
            .collect();

        // 2. Parse params bytes into string form according to type.
        let params = parse_params(&self.type_description, params)?;

        // 3. Replace generic params in statement to real value. For example, "SELECT $3, $2, $1" ->
        // "SELECT 'A', 'B', 'C'".
        let instance_query_string = replace_params(statement, &generic_params, &params);

        // 4. Get row_description and return portal.
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

    use super::{parse_params, replace_params};
    use crate::pg_field_descriptor::TypeOid;
    #[test]
    fn test_replace_params() {
        {
            let raw_params = vec!["A".into(), "B".into(), "C".into()];
            let type_description = vec![TypeOid::Varchar; 3];
            let params = parse_params(&type_description, &raw_params).unwrap();

            let res = replace_params("SELECT $3,$2,$1".to_string(), &[1, 2, 3], &params);
            assert_eq!(res, "SELECT 'C','B','A'");

            let res = replace_params(
                "SELECT $2,$3,$1  ,$3 ,$2 ,$1     ;".to_string(),
                &[1, 2, 3],
                &params,
            );
            assert_eq!(res, "SELECT 'B','C','A'  ,'C' ,'B' ,'A'     ;");
        }

        {
            let raw_params = vec![
                "A".into(),
                "B".into(),
                "C".into(),
                "D".into(),
                "E".into(),
                "F".into(),
                "G".into(),
                "H".into(),
                "I".into(),
                "J".into(),
                "K".into(),
            ];
            let type_description = vec![TypeOid::Varchar; 11];
            let params = parse_params(&type_description, &raw_params).unwrap();

            let res = replace_params(
                "SELECT $11,$2,$1;".to_string(),
                &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                &params,
            );
            assert_eq!(res, "SELECT 'K','B','A';");
        }

        {
            let raw_params = vec![
                "A".into(),
                "B".into(),
                "C".into(),
                "D".into(),
                "E".into(),
                "F".into(),
                "G".into(),
                "H".into(),
                "I".into(),
                "J".into(),
                "K".into(),
                "L".into(),
            ];
            let type_description = vec![TypeOid::Varchar; 12];
            let params = parse_params(&type_description, &raw_params).unwrap();

            let res = replace_params(
                "SELECT $2,$1,$11,$10 ,$11, $1,$12 , $2,  'He1ll2o',1;".to_string(),
                &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                &params,
            );
            assert_eq!(
                res,
                "SELECT 'B','A','K','J' ,'K', 'A','L' , 'B',  'He1ll2o',1;"
            );

            let res = replace_params(
                "SELECT $2,$1,$11,$10 ,$11, $1,$12 , $2,  'He1ll2o',1;".to_string(),
                &[2, 1, 11, 10, 12, 9, 7, 8, 5, 6, 4, 3],
                &params,
            );
            assert_eq!(
                res,
                "SELECT 'B','A','K','J' ,'K', 'A','L' , 'B',  'He1ll2o',1;"
            );
        }

        {
            let raw_params = vec!["A".into(), "B".into()];
            let type_description = vec![TypeOid::Varchar; 2];
            let params = parse_params(&type_description, &raw_params).unwrap();

            let res = replace_params(
                "INSERT INTO nperson (name,data) VALUES ($1,$2)".to_string(),
                &[1, 2],
                &params,
            );
            assert!(res == "INSERT INTO nperson (name,data) VALUES ('A','B')");
        }
    }
}
