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

#![feature(used_with_arg)]

use risingwave_common::types::DataType;
use risingwave_expr::expr::{
    BoxedExpression, LiteralExpression, SyncExpressionBoxExt, build_from_pretty,
};
use risingwave_expr::{ExprError, Result, build_function};

risingwave_expr_impl::enable!();

// These test-only overloads exercise the macro-generated descriptor, rather than calling the
// builder directly. The real pg_sleep(float8) supplies an async child without performing I/O.
#[build_function("pg_sleep(int4) -> int4", async)]
fn marked_async(_: DataType, mut children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    children.pop().ok_or(ExprError::DivisionByZero)
}

#[build_function("pg_sleep_for(int4) -> int4")]
fn unmarked(_: DataType, mut children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    children.pop().ok_or(ExprError::DivisionByZero)
}

#[test]
fn marked_async_builder_rejects_sync_result() {
    let descriptor = pg_sleep_int4_int4();
    assert!(descriptor.is_async);
    let expr = LiteralExpression::new(DataType::Int32, Some(1.into())).boxed();
    let err = descriptor
        .build_scalar(DataType::Int32, vec![expr])
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "async expression builder `marked_async` returned a synchronous expression"
    );
}

#[test]
fn marked_async_builder_accepts_async_result() {
    let expr = build_from_pretty("(pg_sleep:int4 0.0:float8)");
    let built = pg_sleep_int4_int4()
        .build_scalar(DataType::Int32, vec![expr])
        .unwrap();
    assert!(matches!(built, BoxedExpression::Async(_)));
}

#[test]
fn marked_async_builder_preserves_build_error() {
    let err = pg_sleep_int4_int4()
        .build_scalar(DataType::Int32, vec![])
        .unwrap_err();
    assert!(matches!(err, ExprError::DivisionByZero));
}

#[test]
fn unmarked_builder_accepts_async_child() {
    let descriptor = pg_sleep_for_int4_int4();
    assert!(!descriptor.is_async);
    let expr = build_from_pretty("(pg_sleep:int4 0.0:float8)");
    let built = descriptor
        .build_scalar(DataType::Int32, vec![expr])
        .unwrap();
    assert!(matches!(built, BoxedExpression::Async(_)));
}
