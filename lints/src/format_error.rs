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

use clippy_utils::diagnostics::span_lint_and_help;
use clippy_utils::macros::{
    find_format_arg_expr, find_format_args, is_format_macro, macro_backtrace,
};
use clippy_utils::ty::implements_trait;
use clippy_utils::{is_in_cfg_test, is_in_test_function, is_trait_method, match_function_call};
use rustc_ast::FormatArgsPiece;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_session::{declare_tool_lint, impl_lint_pass};
use rustc_span::{sym, Span};

declare_tool_lint! {
    /// ### What it does
    /// Checks for the use of `format!` directly on an error.
    ///
    /// ### Why is this bad?
    /// By convention, errors only ensure that they will format their **own**
    /// messages, but not always along with the messages of their sources.
    /// Directly formatting an error may result in the loss of information.
    ///
    /// ### Known problems
    /// Ignore this lint if you're intentionally formatting the message of an
    /// error.
    ///
    /// ### Example
    /// ```no_run
    /// let err = "foo".parse::<i32>().unwrap_err();
    /// println!("error: {}", err);
    /// ```
    /// Use instead:
    /// ```no_run
    /// use thiserror_ext::AsReport;
    /// let err = "foo".parse::<i32>().unwrap_err();
    /// println!("error: {}", err.as_report());
    /// ```
    pub rw::FORMAT_ERROR,
    Warn,
    "formatting an error directly"
}

#[derive(Default)]
pub struct FormatError;

impl_lint_pass!(FormatError => [FORMAT_ERROR]);

const TRACING_FIELD_DEBUG: [&str; 3] = ["tracing_core", "field", "debug"];
const TRACING_FIELD_DISPLAY: [&str; 3] = ["tracing_core", "field", "display"];

impl<'tcx> LateLintPass<'tcx> for FormatError {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'_>) {
        // Ignore if in test code.
        if is_in_cfg_test(cx.tcx, expr.hir_id) || is_in_test_function(cx.tcx, expr.hir_id) {
            return;
        }

        // `%err`, `?err` in tracing events and spans.
        if let Some(args) = match_function_call(cx, expr, &TRACING_FIELD_DEBUG)
            .or_else(|| match_function_call(cx, expr, &TRACING_FIELD_DISPLAY))
            && let [arg_expr, ..] = args
        {
            check_fmt_arg(cx, arg_expr);
        }

        // `{}`, `{:?}` in format macros.
        for macro_call in macro_backtrace(expr.span) {
            if is_format_macro(cx, macro_call.def_id)
                && let Some(format_args) = find_format_args(cx, expr, macro_call.expn) {
                for piece in &format_args.template {
                    if let FormatArgsPiece::Placeholder(placeholder) = piece
                        && let Ok(index) = placeholder.argument.index
                        && let Some(arg) = format_args.arguments.all_args().get(index)
                        && let Ok(arg_expr) = find_format_arg_expr(expr, arg)
                    {
                        check_fmt_arg(cx, arg_expr);
                    }
                }
            }
        }

        // `err.to_string()`
        if let ExprKind::MethodCall(_, receiver, [], to_string_span) = expr.kind
            && is_trait_method(cx, expr, sym::ToString)
        {
            check_to_string_call(cx, receiver, to_string_span);
        }
    }
}

fn check_fmt_arg(cx: &LateContext<'_>, arg_expr: &Expr<'_>) {
    check_arg(
        cx,
        arg_expr,
        arg_expr.span,
        "consider importing `thiserror_ext::AsReport` and using `.as_report()` instead",
    );
}

fn check_to_string_call(cx: &LateContext<'_>, receiver: &Expr<'_>, to_string_span: Span) {
    check_arg(
        cx,
        receiver,
        to_string_span,
        "consider importing `thiserror_ext::AsReport` and using `.to_report_string()` instead",
    );
}

fn check_arg(cx: &LateContext<'_>, arg_expr: &Expr<'_>, span: Span, help: &str) {
    let Some(error_trait_id) = cx.tcx.get_diagnostic_item(sym::Error) else {
        return;
    };

    let ty = cx.typeck_results().expr_ty(arg_expr).peel_refs();

    if implements_trait(cx, ty, error_trait_id, &[]) {
        if let Some(span) = core::iter::successors(Some(span), |s| s.parent_callsite())
            .find(|s| s.can_be_used_for_suggestions())
        {
            // TODO: applicable suggestions
            span_lint_and_help(
                cx,
                FORMAT_ERROR,
                span,
                "should not format error directly",
                None,
                help,
            );
        }
    }
}

#[test]
fn ui() {
    dylint_testing::ui_test_example(env!("CARGO_PKG_NAME"), "format_error");
}
