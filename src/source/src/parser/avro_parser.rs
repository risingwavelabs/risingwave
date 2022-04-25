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
use std::ffi::OsStr;
use std::path::Path;

use apache_avro::Schema;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AvroParseError {
    #[error("{0}. schema location is not support.")]
    UnSupportSchemaLocation(String),
    #[error("{0}.illegal schema file format.")]
    IllegalSchemaFile(String),
}

#[derive(Debug, Clone)]
enum AvroCodec {
    Snappy,
    ZStandard,
}

#[derive(Debug)]
pub struct AvroParser {
    schema: Schema,
    codec: Option<AvroCodec>,
}

impl AvroParser {

    fn new_avro_parser(
        schema_location: &str,
        props: HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        todo!()
    }
}

pub fn read_schema_from_s3(
    path: String,
    properties: HashMap<String, String>,
) -> anyhow::Result<String> {
    todo!()
}

pub fn read_schema_from_local(path: String) -> anyhow::Result<String> {
    let content_rs = std::fs::read_to_string(path.as_str());
    if let Ok(content) = content_rs {
        Ok(content)
    } else {
        Err(content_rs.err().unwrap().into())
    }
}

pub fn load_schema<F: Fn(String, Option<HashMap<String, String>>) -> anyhow::Result<String>>(
    schema_path: String,
    properties: Option<HashMap<String, String>>,
    f: F,
) -> anyhow::Result<Schema> {
    let file_extension = Path::new(schema_path.as_str())
        .extension()
        .and_then(OsStr::to_str);
    if let Some(extension) = file_extension {
        if !extension.eq("avsc") && !extension.eq("json") {
            Err(anyhow::Error::from(AvroParseError::IllegalSchemaFile(
                schema_path.to_string(),
            )))
        } else {
            let read_schema_rs = f(schema_path, properties);
            let schema_content = if let Ok(content) = read_schema_rs {
                content
            } else {
                return Err(anyhow::Error::from(read_schema_rs.err().unwrap()));
            };
            let schema_rs = apache_avro::Schema::parse_str(schema_content.as_str());
            if let Ok(avro_schema) = schema_rs {
                Ok(avro_schema)
            } else {
                let schema_parse_err = schema_rs.err().unwrap();
                Err(anyhow::Error::from(schema_parse_err))
            }
        }
    } else {
        Err(anyhow::Error::from(AvroParseError::IllegalSchemaFile(
            schema_path.to_string(),
        )))
    }
}

#[cfg(test)]
mod test {
    use std::env;

    use crate::parser::avro_parser::{load_schema, read_schema_from_local};

    fn test_data_path() -> String {
        let curr_dir = env::current_dir().unwrap().into_os_string();
        curr_dir.into_string().unwrap() + "/src/test_data/user.avsc"
    }

    #[test]
    fn test_read_schema_from_local() {
        let schema_path = test_data_path();
        let content_rs = read_schema_from_local(schema_path);
        assert!(content_rs.is_ok());
    }

    #[test]
    fn test_load_schema() {
        let schema_path = test_data_path();
        let schema_rs = load_schema(schema_path, None, |path, props| {
            read_schema_from_local(path)
        });
        assert!(schema_rs.is_ok());
    }
}
