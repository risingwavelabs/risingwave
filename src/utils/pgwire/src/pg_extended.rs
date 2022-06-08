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

fn replace_params(query_string: String, generic_params: &[usize], params: &[Bytes]) -> String {
    let mut tmp = query_string;
    for &i in generic_params.iter() {
        let pattern = Regex::new(format!(r"(?P<x>\${})(?P<y>[,;\s]+)", i).as_str()).unwrap();
        let param = cstr_to_str(&params[i.sub(1)]).unwrap();
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

        // 2. replace params
        let instance_query_string = replace_params(statement, &generic_params, params);

        // 3. Create a new portal.
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

    use super::replace_params;
    #[test]
    fn test_replace_params() {
        let res = replace_params(
            "SELECT $3,$2,$1;".to_string(),
            &[1, 2, 3],
            &["A".into(), "B".into(), "C".into()],
        );
        assert!(res == "SELECT C,B,A;");

        let res = replace_params(
            "SELECT $2,$3,$1  ,$3 ,$2 ,$1     ;".to_string(),
            &[1, 2, 3],
            &["A".into(), "B".into(), "C".into()],
        );
        assert!(res == "SELECT B,C,A  ,C ,B ,A     ;");

        let res = replace_params(
            "SELECT $11,$2,$1;".to_string(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            &[
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
            ],
        );
        assert!(res == "SELECT K,B,A;");

        let res = replace_params(
            "SELECT $2,$1,$11,$10 ,$11, $1,$12 , $2,  \"He1ll2o\",1;".to_string(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            &[
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
            ],
        );
        assert!(res == "SELECT B,A,K,J ,K, A,L , B,  \"He1ll2o\",1;");

        let res = replace_params(
            "SELECT $2,$1,$11,$10 ,$11, $1,$12 , $2,  \"He1ll2o\",1;".to_string(),
            &[2, 1, 11, 10, 12, 9, 7, 8, 5, 6, 4, 3],
            &[
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
            ],
        );
        assert!(res == "SELECT B,A,K,J ,K, A,L , B,  \"He1ll2o\",1;");
    }
}
