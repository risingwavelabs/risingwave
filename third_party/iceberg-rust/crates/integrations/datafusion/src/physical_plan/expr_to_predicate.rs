// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::vec;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, Like, Operator};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression};
use iceberg::spec::{Datum, PrimitiveLiteral};

// A datafusion expression could be an Iceberg predicate, column, or literal.
enum TransformedResult {
    Predicate(Predicate),
    Column(Reference),
    Literal(Datum),
    NotTransformed,
}

enum OpTransformedResult {
    Operator(PredicateOperator),
    And,
    Or,
    NotTransformed,
}

/// Converts DataFusion filters ([`Expr`]) to an iceberg [`Predicate`].
/// If none of the filters could be converted, return `None` which adds no predicates to the scan operation.
/// If the conversion was successful, return the converted predicates combined with an AND operator.
pub fn convert_filters_to_predicate(filters: &[Expr]) -> Option<Predicate> {
    filters
        .iter()
        .filter_map(convert_filter_to_predicate)
        .reduce(Predicate::and)
}

fn convert_filter_to_predicate(expr: &Expr) -> Option<Predicate> {
    match to_iceberg_predicate(expr) {
        TransformedResult::Predicate(predicate) => Some(predicate),
        TransformedResult::Column(column) => {
            // A bare column in a filter context represents a boolean column check
            // Convert it to: column = true
            Some(Predicate::Binary(BinaryExpression::new(
                PredicateOperator::Eq,
                column,
                Datum::bool(true),
            )))
        }
        TransformedResult::Literal(_) => {
            // Literal values in filter context cannot be pushed down
            None
        }
        _ => None,
    }
}

fn to_iceberg_predicate(expr: &Expr) -> TransformedResult {
    match expr {
        Expr::BinaryExpr(binary) => {
            let left = to_iceberg_predicate(&binary.left);
            let right = to_iceberg_predicate(&binary.right);
            let op = to_iceberg_operation(binary.op);
            match op {
                OpTransformedResult::Operator(op) => to_iceberg_binary_predicate(left, right, op),
                OpTransformedResult::And => to_iceberg_and_predicate(left, right),
                OpTransformedResult::Or => to_iceberg_or_predicate(left, right),
                OpTransformedResult::NotTransformed => TransformedResult::NotTransformed,
            }
        }
        Expr::Not(exp) => {
            let expr = to_iceberg_predicate(exp);
            match expr {
                TransformedResult::Predicate(p) => TransformedResult::Predicate(!p),
                TransformedResult::Column(column) => {
                    // NOT of a bare boolean column: NOT col => col = false
                    TransformedResult::Predicate(Predicate::Binary(BinaryExpression::new(
                        PredicateOperator::Eq,
                        column,
                        Datum::bool(false),
                    )))
                }
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::Column(column) => TransformedResult::Column(Reference::new(column.name())),
        Expr::Literal(literal, _) => match scalar_value_to_datum(literal) {
            Some(data) => TransformedResult::Literal(data),
            None => TransformedResult::NotTransformed,
        },
        Expr::InList(inlist) => {
            let mut datums = vec![];
            for expr in &inlist.list {
                let p = to_iceberg_predicate(expr);
                match p {
                    TransformedResult::Literal(l) => datums.push(l),
                    _ => return TransformedResult::NotTransformed,
                }
            }

            let expr = to_iceberg_predicate(&inlist.expr);
            match expr {
                TransformedResult::Column(r) => match inlist.negated {
                    false => TransformedResult::Predicate(r.is_in(datums)),
                    true => TransformedResult::Predicate(r.is_not_in(datums)),
                },
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::IsNull(expr) => {
            let p = to_iceberg_predicate(expr);
            match p {
                TransformedResult::Column(r) => TransformedResult::Predicate(Predicate::Unary(
                    UnaryExpression::new(PredicateOperator::IsNull, r),
                )),
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::IsNotNull(expr) => {
            let p = to_iceberg_predicate(expr);
            match p {
                TransformedResult::Column(r) => TransformedResult::Predicate(Predicate::Unary(
                    UnaryExpression::new(PredicateOperator::NotNull, r),
                )),
                _ => TransformedResult::NotTransformed,
            }
        }
        Expr::Cast(c) => {
            if c.data_type == DataType::Date32 || c.data_type == DataType::Date64 {
                // Casts to date truncate the expression, we cannot simply extract it as it
                // can create erroneous predicates.
                return TransformedResult::NotTransformed;
            }
            to_iceberg_predicate(&c.expr)
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            // Only support simple prefix patterns (e.g., 'prefix%')
            // Note: Iceberg's StartsWith operator is case-sensitive, so we cannot
            // push down case-insensitive LIKE (ILIKE) patterns
            // Escape characters are also not supported for pushdown
            if escape_char.is_some() || *case_insensitive {
                return TransformedResult::NotTransformed;
            }

            // Extract the pattern string
            let pattern_str = match to_iceberg_predicate(pattern) {
                TransformedResult::Literal(d) => match d.literal() {
                    PrimitiveLiteral::String(s) => s.clone(),
                    _ => return TransformedResult::NotTransformed,
                },
                _ => return TransformedResult::NotTransformed,
            };

            // Check if it's a simple prefix pattern (ends with % and no other wildcards)
            if pattern_str.ends_with('%')
                && !pattern_str[..pattern_str.len() - 1].contains(['%', '_'])
            {
                // Extract the prefix (remove trailing %)
                let prefix = pattern_str[..pattern_str.len() - 1].to_string();

                // Get the column reference
                let column = match to_iceberg_predicate(expr) {
                    TransformedResult::Column(r) => r,
                    _ => return TransformedResult::NotTransformed,
                };

                // Create the appropriate predicate
                let predicate = if *negated {
                    column.not_starts_with(Datum::string(prefix))
                } else {
                    column.starts_with(Datum::string(prefix))
                };

                TransformedResult::Predicate(predicate)
            } else {
                // Complex LIKE patterns cannot be pushed down
                TransformedResult::NotTransformed
            }
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            scalar_function_to_iceberg_predicate(func.name(), args)
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_operation(op: Operator) -> OpTransformedResult {
    match op {
        Operator::Eq => OpTransformedResult::Operator(PredicateOperator::Eq),
        Operator::NotEq => OpTransformedResult::Operator(PredicateOperator::NotEq),
        Operator::Lt => OpTransformedResult::Operator(PredicateOperator::LessThan),
        Operator::LtEq => OpTransformedResult::Operator(PredicateOperator::LessThanOrEq),
        Operator::Gt => OpTransformedResult::Operator(PredicateOperator::GreaterThan),
        Operator::GtEq => OpTransformedResult::Operator(PredicateOperator::GreaterThanOrEq),
        // AND OR
        Operator::And => OpTransformedResult::And,
        Operator::Or => OpTransformedResult::Or,
        // Others not supported
        _ => OpTransformedResult::NotTransformed,
    }
}

/// Translates a DataFusion scalar function into an Iceberg predicate.
/// Unlike dedicated Expr variants (e.g. `Expr::IsNull`), scalar functions are
/// identified by name at runtime, so we need to handle them here.
fn scalar_function_to_iceberg_predicate(func_name: &str, args: &[Expr]) -> TransformedResult {
    match func_name {
        // TODO: support complex expression arguments to scalar functions
        "isnan" if args.len() == 1 => {
            let operand = to_iceberg_predicate(&args[0]);
            match operand {
                TransformedResult::Column(r) => TransformedResult::Predicate(Predicate::Unary(
                    UnaryExpression::new(PredicateOperator::IsNan, r),
                )),
                _ => TransformedResult::NotTransformed,
            }
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_and_predicate(
    left: TransformedResult,
    right: TransformedResult,
) -> TransformedResult {
    match (left, right) {
        (TransformedResult::Predicate(left), TransformedResult::Predicate(right)) => {
            TransformedResult::Predicate(left.and(right))
        }
        (TransformedResult::Predicate(left), _) => TransformedResult::Predicate(left),
        (_, TransformedResult::Predicate(right)) => TransformedResult::Predicate(right),
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_or_predicate(left: TransformedResult, right: TransformedResult) -> TransformedResult {
    match (left, right) {
        (TransformedResult::Predicate(left), TransformedResult::Predicate(right)) => {
            TransformedResult::Predicate(left.or(right))
        }
        _ => TransformedResult::NotTransformed,
    }
}

fn to_iceberg_binary_predicate(
    left: TransformedResult,
    right: TransformedResult,
    op: PredicateOperator,
) -> TransformedResult {
    let (r, d, op) = match (left, right) {
        (TransformedResult::NotTransformed, _) => return TransformedResult::NotTransformed,
        (_, TransformedResult::NotTransformed) => return TransformedResult::NotTransformed,
        (TransformedResult::Column(r), TransformedResult::Literal(d)) => (r, d, op),
        (TransformedResult::Literal(d), TransformedResult::Column(r)) => {
            (r, d, reverse_predicate_operator(op))
        }
        _ => return TransformedResult::NotTransformed,
    };
    TransformedResult::Predicate(Predicate::Binary(BinaryExpression::new(op, r, d)))
}

fn reverse_predicate_operator(op: PredicateOperator) -> PredicateOperator {
    match op {
        PredicateOperator::Eq => PredicateOperator::Eq,
        PredicateOperator::NotEq => PredicateOperator::NotEq,
        PredicateOperator::GreaterThan => PredicateOperator::LessThan,
        PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
        PredicateOperator::LessThan => PredicateOperator::GreaterThan,
        PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
        _ => unreachable!("Reverse {}", op),
    }
}

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

/// Convert a scalar value to an iceberg datum.
fn scalar_value_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some(Datum::bool(*v)),
        ScalarValue::Int8(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int16(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Float32(Some(v)) => Some(Datum::double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::Binary(Some(v)) => Some(Datum::binary(v.clone())),
        ScalarValue::LargeBinary(Some(v)) => Some(Datum::binary(v.clone())),
        ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
        ScalarValue::Date64(Some(v)) => Some(Datum::date((*v / MILLIS_PER_DAY) as i32)),
        // Timestamp conversions
        // Note: TimestampSecond and TimestampMillisecond are not handled here because
        // DataFusion's type coercion always converts them to match the column type
        // (either TimestampMicrosecond or TimestampNanosecond) before predicate pushdown.
        // See unit tests for how those conversions would work if needed.
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(Datum::timestamp_micros(*v)),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(Datum::timestamp_nanos(*v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::utils::split_conjunction;
    use datafusion::prelude::{Expr, SessionContext};
    use iceberg::expr::{Predicate, Reference};
    use iceberg::spec::Datum;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::convert_filters_to_predicate;

    fn create_test_schema() -> DFSchema {
        let arrow_schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("bar", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string())]),
            ),
            Field::new("qux", DataType::Float64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )])),
        ]);
        DFSchema::try_from_qualified_schema("my_table", &arrow_schema).unwrap()
    }

    fn convert_to_iceberg_predicate(sql: &str) -> Option<Predicate> {
        let df_schema = create_test_schema();
        let expr = SessionContext::new()
            .parse_sql_expr(sql, &df_schema)
            .unwrap();
        let exprs: Vec<Expr> = split_conjunction(&expr).into_iter().cloned().collect();
        convert_filters_to_predicate(&exprs[..])
    }

    #[test]
    fn test_predicate_conversion_with_single_condition() {
        let predicate = convert_to_iceberg_predicate("foo = 1").unwrap();
        assert_eq!(predicate, Reference::new("foo").equal_to(Datum::long(1)));

        let predicate = convert_to_iceberg_predicate("foo != 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").not_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo > 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo >= 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than_or_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo < 1").unwrap();
        assert_eq!(predicate, Reference::new("foo").less_than(Datum::long(1)));

        let predicate = convert_to_iceberg_predicate("foo <= 1").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").less_than_or_equal_to(Datum::long(1))
        );

        let predicate = convert_to_iceberg_predicate("foo is null").unwrap();
        assert_eq!(predicate, Reference::new("foo").is_null());

        let predicate = convert_to_iceberg_predicate("foo is not null").unwrap();
        assert_eq!(predicate, Reference::new("foo").is_not_null());

        let predicate = convert_to_iceberg_predicate("foo in (5, 6)").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").is_in([Datum::long(5), Datum::long(6)])
        );

        let predicate = convert_to_iceberg_predicate("foo not in (5, 6)").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").is_not_in([Datum::long(5), Datum::long(6)])
        );

        let predicate = convert_to_iceberg_predicate("not foo = 1").unwrap();
        assert_eq!(predicate, !Reference::new("foo").equal_to(Datum::long(1)));
    }

    #[test]
    fn test_predicate_conversion_with_single_unsupported_condition() {
        let predicate = convert_to_iceberg_predicate("foo + 1 = 1");
        assert_eq!(predicate, None);

        let predicate = convert_to_iceberg_predicate("length(bar) = 1");
        assert_eq!(predicate, None);

        let predicate = convert_to_iceberg_predicate("foo in (1, 2, foo)");
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_single_condition_rev() {
        let predicate = convert_to_iceberg_predicate("1 < foo").unwrap();
        assert_eq!(
            predicate,
            Reference::new("foo").greater_than(Datum::long(1))
        );
    }

    #[test]
    fn test_predicate_conversion_with_and_condition() {
        let sql = "foo > 1 and bar = 'test'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition_unsupported() {
        let sql = "foo > 1 and length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Reference::new("foo").greater_than(Datum::long(1));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_and_condition_both_unsupported() {
        let sql = "foo in (1, 2, foo) and length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_or_condition_unsupported() {
        let sql = "foo > 1 or length(bar) = 1";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_or_condition_supported() {
        let sql = "foo > 1 or bar = 'test'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::or(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr() {
        let sql = "(foo > 1 and bar = 'test') or foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();

        let inner_predicate = Predicate::and(
            Reference::new("foo").greater_than(Datum::long(1)),
            Reference::new("bar").equal_to(Datum::string("test")),
        );
        let expected_predicate = Predicate::or(
            inner_predicate,
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_one_and_expr_supported() {
        let sql = "(foo > 1 and length(bar) = 1 ) or foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();

        let inner_predicate = Reference::new("foo").greater_than(Datum::long(1));
        let expected_predicate = Predicate::or(
            inner_predicate,
            Reference::new("foo").less_than(Datum::long(0)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_complex_binary_expr_unsupported() {
        let sql = "(foo > 1 or length(bar) = 1 ) and foo < 0 ";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Reference::new("foo").less_than(Datum::long(0));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_cast() {
        let sql = "ts >= timestamp '2023-01-05T00:00:00'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate =
            Reference::new("ts").greater_than_or_equal_to(Datum::string("2023-01-05T00:00:00"));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_date_cast() {
        let sql = "ts >= date '2023-01-05T11:00:00'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_scalar_value_to_datum_timestamp() {
        use datafusion::common::ScalarValue;

        // Test TimestampMicrosecond - maps directly to Datum::timestamp_micros
        let ts_micros = 1672876800000000i64; // 2023-01-05 00:00:00 UTC in microseconds
        let datum =
            super::scalar_value_to_datum(&ScalarValue::TimestampMicrosecond(Some(ts_micros), None));
        assert_eq!(datum, Some(Datum::timestamp_micros(ts_micros)));

        // Test TimestampNanosecond - maps to Datum::timestamp_nanos to preserve precision
        let ts_nanos = 1672876800000000500i64; // 2023-01-05 00:00:00.000000500 UTC in nanoseconds
        let datum =
            super::scalar_value_to_datum(&ScalarValue::TimestampNanosecond(Some(ts_nanos), None));
        assert_eq!(datum, Some(Datum::timestamp_nanos(ts_nanos)));

        // Test None timestamp
        let datum = super::scalar_value_to_datum(&ScalarValue::TimestampMicrosecond(None, None));
        assert_eq!(datum, None);

        // Note: TimestampSecond and TimestampMillisecond are not supported because
        // DataFusion's type coercion converts them to TimestampMicrosecond or TimestampNanosecond
        // before they reach scalar_value_to_datum in SQL queries.
        //
        // These return None (not pushed down):
        let ts_seconds = 1672876800i64; // 2023-01-05 00:00:00 UTC in seconds
        let datum =
            super::scalar_value_to_datum(&ScalarValue::TimestampSecond(Some(ts_seconds), None));
        assert_eq!(datum, None);

        let ts_millis = 1672876800000i64; // 2023-01-05 00:00:00 UTC in milliseconds
        let datum =
            super::scalar_value_to_datum(&ScalarValue::TimestampMillisecond(Some(ts_millis), None));
        assert_eq!(datum, None);
    }

    #[test]
    fn test_scalar_value_to_datum_binary() {
        use datafusion::common::ScalarValue;

        let bytes = vec![1u8, 2u8, 3u8];
        let datum = super::scalar_value_to_datum(&ScalarValue::Binary(Some(bytes.clone())));
        assert_eq!(datum, Some(Datum::binary(bytes.clone())));

        let datum = super::scalar_value_to_datum(&ScalarValue::LargeBinary(Some(bytes.clone())));
        assert_eq!(datum, Some(Datum::binary(bytes)));

        let datum = super::scalar_value_to_datum(&ScalarValue::Binary(None));
        assert_eq!(datum, None);
    }

    #[test]
    fn test_predicate_conversion_with_binary() {
        let sql = "foo = 1 and bar = X'0102'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        // Binary literals are converted to Datum::binary
        // Note: SQL literal 1 is converted to Long by DataFusion
        let expected_predicate = Reference::new("foo")
            .equal_to(Datum::long(1))
            .and(Reference::new("bar").equal_to(Datum::binary(vec![1u8, 2u8])));
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_scalar_value_to_datum_boolean() {
        use datafusion::common::ScalarValue;

        // Test boolean true
        let datum = super::scalar_value_to_datum(&ScalarValue::Boolean(Some(true)));
        assert_eq!(datum, Some(Datum::bool(true)));

        // Test boolean false
        let datum = super::scalar_value_to_datum(&ScalarValue::Boolean(Some(false)));
        assert_eq!(datum, Some(Datum::bool(false)));

        // Test None boolean
        let datum = super::scalar_value_to_datum(&ScalarValue::Boolean(None));
        assert_eq!(datum, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_starts_with() {
        let sql = "bar LIKE 'test%'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        assert_eq!(
            predicate,
            Reference::new("bar").starts_with(Datum::string("test"))
        );
    }

    #[test]
    fn test_predicate_conversion_with_not_like_starts_with() {
        let sql = "bar NOT LIKE 'test%'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        assert_eq!(
            predicate,
            Reference::new("bar").not_starts_with(Datum::string("test"))
        );
    }

    #[test]
    fn test_predicate_conversion_with_like_empty_prefix() {
        let sql = "bar LIKE '%'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        assert_eq!(
            predicate,
            Reference::new("bar").starts_with(Datum::string(""))
        );
    }

    #[test]
    fn test_predicate_conversion_with_like_complex_pattern() {
        // Patterns with wildcards in the middle cannot be pushed down
        let sql = "bar LIKE 'te%st'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_underscore_wildcard() {
        // Patterns with underscore wildcard cannot be pushed down
        let sql = "bar LIKE 'test_'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_no_wildcard() {
        // Patterns without trailing % cannot be pushed down as StartsWith
        let sql = "bar LIKE 'test'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_ilike() {
        // Case-insensitive LIKE (ILIKE) is not supported
        let sql = "bar ILIKE 'test%'";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_and_other_conditions() {
        let sql = "bar LIKE 'test%' AND foo > 1";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("bar").starts_with(Datum::string("test")),
            Reference::new("foo").greater_than(Datum::long(1)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_like_special_characters() {
        // Test LIKE with special characters in prefix
        let sql = "bar LIKE 'test-abc_123%'";
        let predicate = convert_to_iceberg_predicate(sql);
        // This should not be pushed down because it contains underscore
        assert_eq!(predicate, None);
    }

    #[test]
    fn test_predicate_conversion_with_like_unicode() {
        // Test LIKE with unicode characters in prefix
        let sql = "bar LIKE '测试%'";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        assert_eq!(
            predicate,
            Reference::new("bar").starts_with(Datum::string("测试"))
        );
    }

    #[test]
    fn test_predicate_conversion_with_isnan() {
        let predicate = convert_to_iceberg_predicate("isnan(qux)").unwrap();
        assert_eq!(predicate, Reference::new("qux").is_nan());
    }

    #[test]
    fn test_predicate_conversion_with_not_isnan() {
        let predicate = convert_to_iceberg_predicate("NOT isnan(qux)").unwrap();
        assert_eq!(predicate, !Reference::new("qux").is_nan());
    }

    #[test]
    fn test_predicate_conversion_with_isnan_and_other_condition() {
        let sql = "isnan(qux) AND foo > 1";
        let predicate = convert_to_iceberg_predicate(sql).unwrap();
        let expected_predicate = Predicate::and(
            Reference::new("qux").is_nan(),
            Reference::new("foo").greater_than(Datum::long(1)),
        );
        assert_eq!(predicate, expected_predicate);
    }

    #[test]
    fn test_predicate_conversion_with_isnan_unsupported_arg() {
        // isnan on a complex expression (not a bare column) cannot be pushed down
        let sql = "isnan(qux + 1)";
        let predicate = convert_to_iceberg_predicate(sql);
        assert_eq!(predicate, None);
    }
}
