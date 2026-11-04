// Copyright 2026 RisingWave Labs
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

//! Full-text keyword matching.

use risingwave_common::fulltext::{self, Analyzer, DEFAULT_ANALYZER};
use risingwave_common::types::{JsonbRef, ScalarRefImpl};
use risingwave_expr::{ExprError, Result, function};

/// An analyzed `keyword_match` query.
#[derive(Debug)]
pub struct KeywordMatchQuery {
    analyzer: Analyzer,
    /// Sorted and de-duplicated, never empty.
    tokens: Vec<String>,
}

impl KeywordMatchQuery {
    fn new(query: &str, analyzer: &str) -> Result<Self> {
        let analyzer = Analyzer::from_name(analyzer).ok_or_else(|| ExprError::InvalidParam {
            name: "analyzer",
            reason: format!("unknown full-text analyzer \"{analyzer}\"").into(),
        })?;
        let tokens = fulltext::analyze_query(analyzer, query);
        if tokens.is_empty() {
            return Err(ExprError::InvalidParam {
                name: "query",
                reason: "keyword_match query must contain at least one token".into(),
            });
        }
        Ok(Self { analyzer, tokens })
    }

    fn with_default_analyzer(query: &str) -> Result<Self> {
        Self::new(query, DEFAULT_ANALYZER)
    }

    fn matches(&self, doc: ScalarRefImpl<'_>) -> bool {
        fulltext::document_matches(self.analyzer, doc, &self.tokens)
    }
}

/// Returns true if the document contains every token of the query. Both are tokenized with the
/// same analyzer (`simple` by default). Matching is by token, not by substring, and token order
/// does not matter.
///
/// ```slt
/// query T
/// select keyword_match('Database connection timeout', 'TIMEOUT database');
/// ----
/// t
///
/// query T
/// select keyword_match('runtime error', 'time');
/// ----
/// f
///
/// query T
/// select keyword_match('{"msg": "Database timeout", "code": 504}'::jsonb, 'database timeout', 'simple');
/// ----
/// t
/// ```
#[function(
    "keyword_match(varchar, varchar) -> boolean",
    prebuild = "KeywordMatchQuery::with_default_analyzer($1)?"
)]
#[function(
    "keyword_match(varchar, varchar, varchar) -> boolean",
    prebuild = "KeywordMatchQuery::new($1, $2)?"
)]
fn keyword_match_varchar(doc: &str, query: &KeywordMatchQuery) -> bool {
    query.matches(ScalarRefImpl::Utf8(doc))
}

#[function(
    "keyword_match(jsonb, varchar) -> boolean",
    prebuild = "KeywordMatchQuery::with_default_analyzer($1)?"
)]
#[function(
    "keyword_match(jsonb, varchar, varchar) -> boolean",
    prebuild = "KeywordMatchQuery::new($1, $2)?"
)]
fn keyword_match_jsonb(doc: JsonbRef<'_>, query: &KeywordMatchQuery) -> bool {
    query.matches(ScalarRefImpl::Jsonb(doc))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, Datum, JsonbVal, ScalarImpl};
    use risingwave_expr::expr::{
        BoxedExpression, InputRefExpression, LiteralExpression, SyncExpressionBoxExt, build_func,
    };
    use risingwave_pb::expr::expr_node::PbType;

    fn varchar(s: Option<&str>) -> Datum {
        s.map(|s| ScalarImpl::Utf8(s.into()))
    }

    fn jsonb(s: &str) -> Datum {
        Some(ScalarImpl::Jsonb(JsonbVal::from_str(s).unwrap()))
    }

    fn literal(ty: DataType, value: Datum) -> BoxedExpression {
        LiteralExpression::new(ty, value).boxed()
    }

    fn input(ty: DataType, idx: usize) -> BoxedExpression {
        InputRefExpression::new(ty, idx).boxed()
    }

    fn build(children: Vec<BoxedExpression>) -> risingwave_expr::Result<BoxedExpression> {
        build_func(PbType::KeywordMatch, DataType::Boolean, children)
    }

    /// Evaluates `keyword_match` with a constant query/analyzer (prebuilt path) and with
    /// query/analyzer read from the input row (per-row path), and checks both agree.
    async fn eval(doc_ty: DataType, doc: Datum, query: &str, analyzer: Option<&str>) -> Datum {
        let mut const_children = vec![
            input(doc_ty.clone(), 0),
            literal(DataType::Varchar, varchar(Some(query))),
        ];
        let mut row_children = vec![input(doc_ty, 0), input(DataType::Varchar, 1)];
        let mut row = vec![doc, varchar(Some(query))];
        if let Some(analyzer) = analyzer {
            const_children.push(literal(DataType::Varchar, varchar(Some(analyzer))));
            row_children.push(input(DataType::Varchar, 2));
            row.push(varchar(Some(analyzer)));
        }
        let row = OwnedRow::new(row);
        let const_result = build(const_children).unwrap().eval_row(&row).await.unwrap();
        let row_result = build(row_children).unwrap().eval_row(&row).await.unwrap();
        assert_eq!(const_result, row_result);
        const_result
    }

    async fn eval_text(doc: &str, query: &str) -> bool {
        let result = eval(DataType::Varchar, varchar(Some(doc)), query, None).await;
        result.unwrap().into_bool()
    }

    #[tokio::test]
    async fn test_keyword_match_varchar() {
        assert!(eval_text("Database connection timeout", "database timeout").await);
        assert!(
            eval_text(
                "Request timeout while connecting to database",
                "database timeout"
            )
            .await
        );
        assert!(!eval_text("Database query completed", "database timeout").await);
        assert!(!eval_text("databases timed out", "database timeout").await);
        assert!(eval_text("ERROR: Timeout", "timeout").await);
        assert!(!eval_text("runtime error", "timeout").await);
        assert!(!eval_text("runtime error", "time").await);
        assert!(eval_text("a-b c", "C, b").await);
    }

    #[tokio::test]
    async fn test_keyword_match_explicit_analyzer() {
        let result = eval(
            DataType::Varchar,
            varchar(Some("Database timeout")),
            "TIMEOUT",
            Some("simple"),
        )
        .await;
        assert_eq!(result, Some(ScalarImpl::Bool(true)));
    }

    #[tokio::test]
    async fn test_keyword_match_jsonb() {
        let doc =
            r#"{"error": {"message": "Database timeout", "code": 504}, "tags": ["Retry", 1]}"#;
        for (query, expected) in [
            ("database timeout", true),
            ("retry database", true),
            ("error", false),
            ("504", false),
            ("message", false),
        ] {
            let result = eval(DataType::Jsonb, jsonb(doc), query, None).await;
            assert_eq!(result, Some(ScalarImpl::Bool(expected)), "query: {query}");
            let result = eval(DataType::Jsonb, jsonb(doc), query, Some("simple")).await;
            assert_eq!(result, Some(ScalarImpl::Bool(expected)), "query: {query}");
        }
    }

    #[tokio::test]
    async fn test_keyword_match_null() {
        assert_eq!(eval(DataType::Varchar, None, "x", None).await, None);
        assert_eq!(eval(DataType::Jsonb, None, "x", Some("simple")).await, None);

        // NULL query or analyzer constants produce NULL.
        let row = OwnedRow::new(vec![varchar(Some("x"))]);
        let expr = build(vec![
            input(DataType::Varchar, 0),
            literal(DataType::Varchar, None),
        ])
        .unwrap();
        assert_eq!(expr.eval_row(&row).await.unwrap(), None);
        let expr = build(vec![
            input(DataType::Varchar, 0),
            literal(DataType::Varchar, varchar(Some("x"))),
            literal(DataType::Varchar, None),
        ])
        .unwrap();
        assert_eq!(expr.eval_row(&row).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_keyword_match_errors() {
        let const_err = |query: &str, analyzer: &str| {
            build(vec![
                input(DataType::Varchar, 0),
                literal(DataType::Varchar, varchar(Some(query))),
                literal(DataType::Varchar, varchar(Some(analyzer))),
            ])
            .unwrap_err()
            .to_string()
        };
        let err = const_err(" ,.! ", "simple");
        assert!(
            err.contains("keyword_match query must contain at least one token"),
            "{err}"
        );
        let err = const_err("x", "english");
        assert!(err.contains("unknown full-text analyzer"), "{err}");

        // Non-constant query: the error is raised during evaluation.
        let expr = build(vec![
            input(DataType::Varchar, 0),
            input(DataType::Varchar, 1),
        ])
        .unwrap();
        let row = OwnedRow::new(vec![varchar(Some("x")), varchar(Some("..."))]);
        let err = expr.eval_row(&row).await.unwrap_err().to_string();
        assert!(
            err.contains("keyword_match query must contain at least one token"),
            "{err}"
        );
    }
}
