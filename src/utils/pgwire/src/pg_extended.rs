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

use bytes::Bytes;
use regex::Regex;

use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_protocol::cstr_to_str;

/// Parse params accoring the type description.
///
/// # Example
///
/// ```ignore
/// let raw_params = vec!["A".into(), "B".into(), "C".into()];
/// let type_description = vec![TypeOid::Varchar; 3];
/// let params = parse_params(&type_description, &raw_params);
/// assert_eq!(params, vec!["'A'", "'B'", "'C'"])
/// ```
fn parse_params(type_description: &[TypeOid], raw_params: &[Bytes]) -> Vec<String> {
    assert!(type_description.len() == raw_params.len());

    raw_params
        .iter()
        .enumerate()
        .map(|(i, param)| {
            let oid = type_description[i];
            match oid {
                TypeOid::Varchar => format!("'{}'", cstr_to_str(param).unwrap()),
                TypeOid::Boolean => todo!(),
                TypeOid::BigInt => todo!(),
                TypeOid::SmallInt => todo!(),
                TypeOid::Int => todo!(),
                TypeOid::Float4 => todo!(),
                TypeOid::Float8 => todo!(),
                TypeOid::CharArray => todo!(),
                TypeOid::Date => todo!(),
                TypeOid::Time => todo!(),
                TypeOid::Timestamp => todo!(),
                TypeOid::Timestampz => todo!(),
                TypeOid::Decimal => todo!(),
            }
        })
        .collect()
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
            Regex::new(format!(r"(?P<x>\${0})(?P<y>[,;\s]+)|\${0}$", param_idx).as_str()).unwrap();
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
}

impl PgStatement {
    pub fn new(
        name: String,
        query_string: Bytes,
        type_description: Vec<TypeOid>,
        row_description: Vec<PgFieldDescriptor>,
    ) -> Self {
        PgStatement {
            name,
            query_string,
            type_description,
            row_description,
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

    pub fn instance(&self, name: String, params: &[Bytes]) -> PgPortal {
        let statement = cstr_to_str(&self.query_string).unwrap().to_owned();

        if params.is_empty() {
            return PgPortal {
                name,
                query_string: self.query_string.clone(),
            };
        }

        // 1. Identify all the $n.
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

        // 2. parse params.
        let params = parse_params(&self.type_description, params);

        // 3. replace params.
        let instance_query_string = replace_params(statement, &generic_params, &params);

        // 4. Create a new portal.
        PgPortal {
            name,
            query_string: Bytes::from(instance_query_string),
        }
    }
}

#[derive(Default)]
pub struct PgPortal {
    name: String,
    query_string: Bytes,
}

impl PgPortal {
    pub fn new(name: String, query_string: Bytes) -> Self {
        PgPortal { name, query_string }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn query_string(&self) -> Bytes {
        self.query_string.clone()
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
            let params = parse_params(&type_description, &raw_params);

            let res = replace_params("SELECT $3,$2,$1".to_string(), &[1, 2, 3], &params);
            assert!(res == "SELECT 'C','B','A'");

            let res = replace_params(
                "SELECT $2,$3,$1  ,$3 ,$2 ,$1     ;".to_string(),
                &[1, 2, 3],
                &params,
            );
            assert!(res == "SELECT 'B','C','A'  ,'C' ,'B' ,'A'     ;");
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
            let params = parse_params(&type_description, &raw_params);

            let res = replace_params(
                "SELECT $11,$2,$1;".to_string(),
                &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                &params,
            );
            assert!(res == "SELECT 'K','B','A';");
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
            let params = parse_params(&type_description, &raw_params);

            let res = replace_params(
                "SELECT $2,$1,$11,$10 ,$11, $1,$12 , $2,  'He1ll2o',1;".to_string(),
                &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                &params,
            );
            assert!(res == "SELECT 'B','A','K','J' ,'K', 'A','L' , 'B',  'He1ll2o',1;");

            let res = replace_params(
                "SELECT $2,$1,$11,$10 ,$11, $1,$12 , $2,  'He1ll2o',1;".to_string(),
                &[2, 1, 11, 10, 12, 9, 7, 8, 5, 6, 4, 3],
                &params,
            );
            assert!(res == "SELECT 'B','A','K','J' ,'K', 'A','L' , 'B',  'He1ll2o',1;");
        }
    }
}
