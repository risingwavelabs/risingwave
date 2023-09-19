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

use risingwave_expr::{function, ExprError};
use risingwave_sqlparser::parser::{Parser, ParserError};
use risingwave_sqlparser::tokenizer::{Token, Tokenizer, TokenizerError};
use thiserror::Error;

use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::root_catalog::SchemaPath;

use super::context::CATALOG_READER;

#[derive(Error, Debug)]
enum ResolveRegclassError {
    #[error("parse object name failed: {0}")]
    Parser(#[from] ParserError),
}

fn resolve_regclass(catalog: CatalogReadGuard, class_name: &str) -> Result<u32, ResolveRegclassError> {
    let obj = parse_object_name(class_name)?;

    if obj.0.len() == 1 {
        let class_name = obj.0[0].real_value();
        let schema_path = SchemaPath::Path(&self.search_path, &self.auth_context.user_name);
        Ok(catalog
            .get_id_by_class_name(&self.db_name, schema_path, &class_name)?)
    } else {
        let schema = obj.0[0].real_value();
        let class_name = obj.0[1].real_value();
        let schema_path = SchemaPath::Name(&schema);
        Ok(catalog
            .get_id_by_class_name(&self.db_name, schema_path, &class_name)?)
    }
}

fn parse_object_name(name: &str) -> Result<risingwave_sqlparser::ast::ObjectName, ParserError> {
    // We use the full parser here because this function needs to accept every legal way
    // of identifying an object in PG SQL as a valid value for the varchar
    // literal.  For example: 'foo', 'public.foo', '"my table"', and
    // '"my schema".foo' must all work as values passed pg_table_size.
    let mut tokenizer = Tokenizer::new(name);
    let tokens = tokenizer
        .tokenize_with_location()
        .map_err(ParserError::from)?;
    let mut parser = Parser::new(tokens);
    let object = parser.parse_object_name()?;
    parser.expect_token(&Token::EOF)?;
    Ok(object)
}

fn resolve_regclass(name: &str) -> 

fn cast_regclass(_s: &str) -> Result<i32, ExprError> {
    CATALOG_READER.try_with(|catalog_reader| {})?;

    Ok(1)
}
