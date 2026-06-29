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

use bytes::Bytes;
use pgwire::types::{Format, FormatIterator};
use risingwave_common::bail;
use risingwave_common::error::BoxedError;
use risingwave_common::types::{DataType, Datum, ScalarImpl, Timestamp, Timestamptz};

use super::BoundStatement;
use super::statement::RewriteExprsRecursive;
use crate::error::{ErrorCode, Result};
use crate::expr::{Expr, ExprImpl, ExprRewriter, Literal, default_rewrite_expr};

/// Rewrites parameter expressions to literals.
pub(crate) struct ParamRewriter {
    pub(crate) params: Vec<Option<Bytes>>,
    pub(crate) parsed_params: Vec<Datum>,
    pub(crate) param_formats: Vec<Format>,
    pub(crate) session_timezone: String,
    pub(crate) error: Option<BoxedError>,
}

impl ParamRewriter {
    pub(crate) fn new(
        param_formats: Vec<Format>,
        params: Vec<Option<Bytes>>,
        session_timezone: String,
    ) -> Self {
        Self {
            parsed_params: vec![None; params.len()],
            params,
            param_formats,
            session_timezone,
            error: None,
        }
    }
}

impl ExprRewriter for ParamRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if self.error.is_some() {
            return expr;
        }
        default_rewrite_expr(self, expr)
    }

    fn rewrite_subquery(&mut self, mut subquery: crate::expr::Subquery) -> ExprImpl {
        subquery.query.rewrite_exprs_recursive(self);
        subquery.into()
    }

    fn rewrite_parameter(&mut self, parameter: crate::expr::Parameter) -> ExprImpl {
        let data_type = parameter.return_type();

        // Postgresql parameter index is 1-based. e.g. $1,$2,$3
        // But we store it in 0-based vector. So we need to minus 1.
        let parameter_index = (parameter.index - 1) as usize;

        fn cstr_to_str(b: &[u8]) -> std::result::Result<&str, BoxedError> {
            let without_null = if b.last() == Some(&0) {
                &b[..b.len() - 1]
            } else {
                b
            };
            Ok(std::str::from_utf8(without_null)?)
        }

        let datum: Datum = if let Some(val_bytes) = self.params[parameter_index].clone() {
            let res = match self.param_formats[parameter_index] {
                Format::Text => {
                    let text_res =
                        cstr_to_str(&val_bytes).and_then(|str| ScalarImpl::from_text(str, &data_type));
                    // Timestamptz fallback: if from_text fails (no timezone in string),
                    // try parsing as naive Timestamp and apply session timezone.
                    // This matches PostgreSQL behavior where `$1::timestamptz` with a
                    // date-only string like '2026-05-01' uses the session timezone.
                    if matches!(&data_type, DataType::Timestamptz) && text_res.is_err() {
                        cstr_to_str(&val_bytes).and_then(|s| {
                            let ts: Timestamp = s.parse().map_err(|e| {
                                Box::new(e) as BoxedError
                            })?;
                            let tz = Timestamptz::lookup_time_zone(&self.session_timezone)
                                .map_err(|e| {
                                    Box::<dyn std::error::Error + Send + Sync>::from(e) as BoxedError
                                })?;
                            let instant_local = match ts.0.and_local_timezone(tz) {
                                chrono::LocalResult::Single(t) => t,
                                chrono::LocalResult::None => {
                                    // Spring-forward gap: shift back 3h, convert, shift forward
                                    (ts.0 - chrono::Duration::hours(3))
                                        .and_local_timezone(tz)
                                        .single()
                                        .ok_or_else(|| {
                                            Box::<dyn std::error::Error + Send + Sync>::from(
                                                "invalid local timestamp in DST gap",
                                            )
                                                as BoxedError
                                        })?
                                        + chrono::Duration::hours(3)
                                }
                                chrono::LocalResult::Ambiguous(_, latest) => latest,
                            };
                            Ok(ScalarImpl::from(Timestamptz::from_micros(
                                instant_local.timestamp_micros(),
                            )))
                        })
                    } else {
                        text_res
                    }
                }
                Format::Binary => ScalarImpl::from_binary(&val_bytes, &data_type),
            };
            match res {
                Ok(datum) => Some(datum),
                Err(e) => {
                    self.error = Some(e);
                    return parameter.into();
                }
            }
        } else {
            None
        };

        self.parsed_params[parameter_index].clone_from(&datum);
        Literal::new(datum, data_type).into()
    }
}

impl BoundStatement {
    pub fn bind_parameter(
        mut self,
        params: Vec<Option<Bytes>>,
        param_formats: Vec<Format>,
        session_timezone: String,
    ) -> Result<(BoundStatement, Vec<Datum>)> {
        let mut rewriter = ParamRewriter::new(
            FormatIterator::new(&param_formats, params.len())
                .map_err(ErrorCode::BindError)?
                .collect(),
            params,
            session_timezone,
        );

        self.rewrite_exprs_recursive(&mut rewriter);

        if let Some(err) = rewriter.error {
            bail!(err);
        }

        Ok((self, rewriter.parsed_params))
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use pgwire::types::Format;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_sqlparser::test_utils::parse_sql_statements;

    use crate::binder::BoundStatement;
    use crate::binder::test_utils::{mock_binder, mock_binder_with_param_types};

    fn create_expect_bound(sql: &str) -> BoundStatement {
        let mut binder = mock_binder();
        let stmt = parse_sql_statements(sql).unwrap().remove(0);
        binder.bind(stmt).unwrap()
    }

    fn create_actual_bound(
        sql: &str,
        param_types: Vec<Option<DataType>>,
        params: Vec<Option<Bytes>>,
        param_formats: Vec<Format>,
    ) -> BoundStatement {
        let mut binder = mock_binder_with_param_types(param_types);
        let stmt = parse_sql_statements(sql).unwrap().remove(0);
        let bound = binder.bind(stmt).unwrap();
        bound.bind_parameter(params, param_formats, "UTC".to_owned()).unwrap().0
    }

    fn expect_actual_eq(expect: BoundStatement, actual: BoundStatement) {
        // Use debug format to compare. May modify in future.
        assert_eq!(format!("{:?}", expect), format!("{:?}", actual));
    }

    #[tokio::test]
    async fn basic_select() {
        expect_actual_eq(
            create_expect_bound("select 1::int4"),
            create_actual_bound(
                "select $1::int4",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn basic_value() {
        expect_actual_eq(
            create_expect_bound("values(1::int4)"),
            create_actual_bound(
                "values($1::int4)",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn default_type() {
        expect_actual_eq(
            create_expect_bound("select '1'"),
            create_actual_bound(
                "select $1",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn cast_after_specific() {
        expect_actual_eq(
            create_expect_bound("select 1::varchar"),
            create_actual_bound(
                "select $1::varchar",
                vec![Some(DataType::Int32)],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn infer_case() {
        expect_actual_eq(
            create_expect_bound("select 1,1::INT4"),
            create_actual_bound(
                "select $1,$1::INT4",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    #[tokio::test]
    async fn subquery() {
        expect_actual_eq(
            create_expect_bound("select (select '1')"),
            create_actual_bound(
                "select (select $1)",
                vec![],
                vec![Some("1".into())],
                vec![Format::Text],
            ),
        );
    }

    /// Test that `$1::timestamptz` with a date-only string uses session timezone.
    /// This is the fix for <https://github.com/risingwavelabs/risingwave/issues/25563>.
    #[tokio::test]
    async fn timestamptz_date_only_bind_param() {
        use risingwave_common::types::Timestamptz;

        let mut binder = mock_binder_with_param_types(vec![Some(DataType::Timestamptz)]);
        let stmt = parse_sql_statements("select $1::timestamptz").unwrap().remove(0);
        let bound = binder.bind(stmt).unwrap();
        let (_bound, parsed_params) = bound
            .bind_parameter(
                vec![Some("2026-05-01".into())],
                vec![Format::Text],
                "UTC".to_owned(),
            )
            .unwrap();

        // "2026-05-01" with UTC timezone should be 2026-05-01 00:00:00+00:00
        let expected = Timestamptz::from_micros(
            chrono::NaiveDate::from_ymd_opt(2026, 5, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_micros(),
        );
        assert_eq!(
            parsed_params[0],
            Some(ScalarImpl::from(expected)),
            "date-only string '2026-05-01' should parse as 2026-05-01T00:00:00Z in UTC"
        );
    }

    /// Test that `cast($1 as timestamptz)` with a date-only string uses session timezone.
    #[tokio::test]
    async fn cast_timestamptz_date_only_bind_param() {
        use risingwave_common::types::Timestamptz;

        let mut binder = mock_binder_with_param_types(vec![Some(DataType::Timestamptz)]);
        let stmt = parse_sql_statements("select cast($1 as timestamptz)")
            .unwrap()
            .remove(0);
        let bound = binder.bind(stmt).unwrap();
        let (_bound, parsed_params) = bound
            .bind_parameter(
                vec![Some("2026-05-01".into())],
                vec![Format::Text],
                "UTC".to_owned(),
            )
            .unwrap();

        let expected = Timestamptz::from_micros(
            chrono::NaiveDate::from_ymd_opt(2026, 5, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_micros(),
        );
        assert_eq!(
            parsed_params[0],
            Some(ScalarImpl::from(expected)),
            "cast($1 as timestamptz) with date-only string should use session timezone"
        );
    }

    /// Test that timestamptz bind param with explicit timezone still works.
    #[tokio::test]
    async fn timestamptz_with_timezone_bind_param() {
        use risingwave_common::types::Timestamptz;

        let mut binder = mock_binder_with_param_types(vec![Some(DataType::Timestamptz)]);
        let stmt = parse_sql_statements("select $1::timestamptz").unwrap().remove(0);
        let bound = binder.bind(stmt).unwrap();
        let (_bound, parsed_params) = bound
            .bind_parameter(
                vec![Some("2026-05-01 12:00:00+08:00".into())],
                vec![Format::Text],
                "UTC".to_owned(),
            )
            .unwrap();

        // 2026-05-01 12:00:00+08:00 = 2026-05-01 04:00:00 UTC
        let expected = Timestamptz::from_micros(
            chrono::NaiveDate::from_ymd_opt(2026, 5, 1)
                .unwrap()
                .and_hms_opt(4, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_micros(),
        );
        assert_eq!(
            parsed_params[0],
            Some(ScalarImpl::from(expected)),
            "explicit timezone in bind param should still work via from_str path"
        );
    }

    /// Test that timestamptz bind param with non-UTC session timezone works.
    #[tokio::test]
    async fn timestamptz_date_only_non_utc_timezone() {
        use risingwave_common::types::Timestamptz;

        let mut binder = mock_binder_with_param_types(vec![Some(DataType::Timestamptz)]);
        let stmt = parse_sql_statements("select $1::timestamptz").unwrap().remove(0);
        let bound = binder.bind(stmt).unwrap();
        let (_bound, parsed_params) = bound
            .bind_parameter(
                vec![Some("2026-05-01".into())],
                vec![Format::Text],
                "Asia/Shanghai".to_owned(),
            )
            .unwrap();

        // "2026-05-01" in Asia/Shanghai (UTC+8) = 2026-04-30 16:00:00 UTC
        let expected = Timestamptz::from_micros(
            chrono::NaiveDate::from_ymd_opt(2026, 4, 30)
                .unwrap()
                .and_hms_opt(16, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_micros(),
        );
        assert_eq!(
            parsed_params[0],
            Some(ScalarImpl::from(expected)),
            "date-only string with Asia/Shanghai timezone should be 2026-04-30T16:00:00Z"
        );
    }
}
