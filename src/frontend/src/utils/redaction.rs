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

use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_sqlparser::ast::*;

use crate::utils::WithOptions;

/// Returns a redacted statement, or None if fails.
///
/// It redacts properties in create source/create table/create sink, according to `MarkRedaction::marks`.
///
/// TODO #14115: redact table name and column names.
#[allow(dead_code)]
pub fn try_redact_statement(stmt: &Statement) -> Option<Statement> {
    use mark_redaction::MarkRedaction;
    use risingwave_connector::match_source_name_str;
    use risingwave_connector::source::SourceProperties;

    let mut stmt = stmt.clone();
    let sql_options = match &mut stmt {
        Statement::CreateSource { stmt } => &mut stmt.with_properties.0,
        Statement::CreateTable { with_options, .. } => with_options,
        Statement::CreateSink { stmt } => &mut stmt.with_properties.0,
        _ => {
            return Some(stmt);
        }
    };
    let Ok(mut with_properties) =
        WithOptions::try_from(sql_options.as_slice()).map(WithOptions::into_inner)
    else {
        return None;
    };
    let Some(connector) = with_properties.remove(UPSTREAM_SOURCE_KEY) else {
        tracing::error!("Must specify 'connector' in WITH clause");
        return None;
    };
    fn redact_marked_field(options: &mut Vec<SqlOption>, marks: std::collections::HashSet<String>) {
        for option in options {
            if marks.contains(&option.name.real_value()) {
                option.value = Value::SingleQuotedString("[REDACTED]".into());
            }
        }
    }
    match_source_name_str!(
        connector.to_lowercase().as_str(),
        PropType,
        {
            let marked = PropType::marks();
            redact_marked_field(sql_options, marked);
        },
        |_| {}
    );
    Some(stmt)
}

/// Returns a redacted SQL, or None if fails.
#[allow(dead_code)]
pub fn try_redact_sql(definition: &str) -> Option<String> {
    let Ok(ast) = risingwave_sqlparser::parser::Parser::parse_sql(definition) else {
        tracing::error!("failed to parse relation definition");
        return None;
    };
    use itertools::Itertools;
    let stmt = ast
        .into_iter()
        .exactly_one()
        .expect("should contains only one statement");
    try_redact_statement(&stmt)
        .as_ref()
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use itertools::Itertools;
    use mark_redaction::MarkRedaction as MarkRedactionTrait;
    use mark_redaction_derive::MarkRedaction;

    use super::try_redact_sql;

    #[test]
    fn test_redaction_options() {
        #[derive(MarkRedaction)]
        #[expect(dead_code)]
        struct Bar<T, F> {
            #[mark_redaction]
            i_a: F,
            #[mark_redaction]
            i_b: T,
        }

        #[derive(MarkRedaction)]
        #[expect(dead_code)]
        struct Foo {
            #[mark_redaction]
            a: i32,
            b: i32,
            #[mark_redaction(rename = "c_rename", alias = "c_1", alias = "c_2")]
            c: i32,
            #[mark_redaction(alias = "d_1")]
            d: i32,
            #[mark_redaction(flatten)]
            #[rustfmt::skip]
            e: Bar::<i64,i32>,
            #[mark_redaction(rename = "fields.a", alias = "fields.b")]
            f: HashSet<String, String>,
        }

        let marks: HashSet<String> = Foo::marks();
        let expected_marks = [
            "a", "c_rename", "c_1", "c_2", "d", "d_1", "i_a", "i_b", "fields.a", "fields.b",
        ];
        assert!(marks.iter().sorted().eq(expected_marks.iter().sorted()));
    }

    #[test]
    fn test_redact_s3_source() {
        let sql = "CREATE SOURCE ad_click (bid_id BIGINT, click_timestamp TIMESTAMPTZ) WITH (connector = 's3', s3.region_name = 'us-east-1', s3.bucket_name = 'ad-click', s3.credentials.access = 'test', s3.credentials.secret = 'test', s3.endpoint_url = 'http://localstack:4566') FORMAT PLAIN ENCODE JSON";
        let expected = "CREATE SOURCE ad_click (bid_id BIGINT, click_timestamp TIMESTAMPTZ) WITH (connector = 's3', s3.region_name = 'us-east-1', s3.bucket_name = 'ad-click', s3.credentials.access = '[REDACTED]', s3.credentials.secret = '[REDACTED]', s3.endpoint_url = 'http://localstack:4566') FORMAT PLAIN ENCODE JSON";
        // s3.credentials.access and s3.credentials.secret are redacted.
        let redacted = try_redact_sql(sql).unwrap();
        assert_eq!(redacted, expected);
    }
}
