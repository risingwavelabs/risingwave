// Copyright 2025 RisingWave Labs
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
    FormatArgsStorage, find_format_arg_expr, is_format_macro, macro_backtrace,
};
use clippy_utils::ty::{implements_trait, match_type};
use clippy_utils::{is_in_cfg_test, is_in_test_function, is_trait_method, match_def_path};
use rustc_ast::FormatArgsPiece;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_session::{declare_tool_lint, impl_lint_pass};
use rustc_span::{Span, sym};

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
pub struct FormatError {
    format_args: FormatArgsStorage,
}

impl FormatError {
    pub fn new(format_args: FormatArgsStorage) -> Self {
        Self { format_args }
    }
}

impl_lint_pass!(FormatError => [FORMAT_ERROR]);

const TRACING_FIELD_DEBUG: [&str; 3] = ["tracing_core", "field", "debug"];
const TRACING_FIELD_DISPLAY: [&str; 3] = ["tracing_core", "field", "display"];
const TRACING_MACROS_EVENT: [&str; 3] = ["tracing", "macros", "event"];
const ANYHOW_MACROS_ANYHOW: [&str; 3] = ["anyhow", "macros", "anyhow"];
const ANYHOW_ERROR: [&str; 2] = ["anyhow", "Error"];
const THISERROR_EXT_REPORT_REPORT: [&str; 3] = ["thiserror_ext", "report", "Report"];

fn match_function_call<'tcx>(
    cx: &LateContext<'tcx>,
    expr: &'tcx Expr<'_>,
    path: &[&str],
) -> Option<&'tcx [Expr<'tcx>]> {
    if let ExprKind::Call(path_expr, args) = expr.kind {
        if let ExprKind::Path(qpath) = path_expr.kind {
            if let Some(def_id) = cx.qpath_res(&qpath, path_expr.hir_id).opt_def_id() {
                if match_def_path(cx, def_id, path) {
                    return Some(args);
                }
            }
        }
    }
    None
}

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
            check_fmt_arg_in_tracing_event(cx, arg_expr);
        }

        // Indirect `{}`, `{:?}` from other macros.
        let in_tracing_event_macro = macro_backtrace(expr.span)
            .any(|macro_call| match_def_path(cx, macro_call.def_id, &TRACING_MACROS_EVENT));
        let in_anyhow_macro = macro_backtrace(expr.span)
            .any(|macro_call| match_def_path(cx, macro_call.def_id, &ANYHOW_MACROS_ANYHOW));

        for macro_call in macro_backtrace(expr.span) {
            if is_format_macro(cx, macro_call.def_id)
                && let Some(format_args) = self.format_args.get(cx, expr, macro_call.expn)
            {
                for piece in &format_args.template {
                    if let FormatArgsPiece::Placeholder(placeholder) = piece
                        && let Ok(index) = placeholder.argument.index
                        && let Some(arg) = format_args.arguments.all_args().get(index)
                        && let Some(arg_expr) = find_format_arg_expr(expr, arg)
                    {
                        if in_tracing_event_macro {
                            check_fmt_arg_in_tracing_event(cx, arg_expr);
                        } else if in_anyhow_macro {
                            if format_args.template.len() == 1 {
                                check_fmt_arg_in_anyhow_error(cx, arg_expr);
                            } else {
                                check_fmt_arg_in_anyhow_context(cx, arg_expr);
                            }
                        } else {
                            check_fmt_arg(cx, arg_expr);
                        }
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
    check_fmt_arg_with_help(
        cx,
        arg_expr,
        "consider importing `thiserror_ext::AsReport` and using `.as_report()` instead",
    )
}

fn check_fmt_arg_in_tracing_event(cx: &LateContext<'_>, arg_expr: &Expr<'_>) {
    // TODO: replace `<error>` with the actual code snippet.
    check_fmt_arg_with_help(
        cx,
        arg_expr,
        "consider importing `thiserror_ext::AsReport` and recording the error as a field \
        with `error = %<error>.as_report()` instead",
    );
}

fn check_fmt_arg_in_anyhow_error(cx: &LateContext<'_>, arg_expr: &Expr<'_>) {
    check_fmt_arg_with_help(
        cx,
        arg_expr,
        (
            "consider directly wrapping the error with `anyhow::anyhow!(..)` instead of formatting it",
            "consider removing the redundant wrapping of `anyhow::anyhow!(..)`",
            "consider directly wrapping the error with `anyhow::anyhow!(..)` instead of formatting its report",
        ),
    );
}

fn check_fmt_arg_in_anyhow_context(cx: &LateContext<'_>, arg_expr: &Expr<'_>) {
    check_fmt_arg_with_help(
        cx,
        arg_expr,
        (
            "consider using `anyhow::Context::(with_)context` to \
        attach additional message to the error and make it an error source instead",
            "consider using `.context(..)` to \
        attach additional message to the error and make it an error source instead",
            "consider using `anyhow::Context::(with_)context` to \
        attach additional message to the error and make it an error source instead",
        ),
    );
}

fn check_fmt_arg_with_help(cx: &LateContext<'_>, arg_expr: &Expr<'_>, help: impl Help + 'static) {
    check_arg(cx, arg_expr, arg_expr.span, help);
}

fn check_to_string_call(cx: &LateContext<'_>, receiver: &Expr<'_>, to_string_span: Span) {
    check_arg(
        cx,
        receiver,
        to_string_span,
        "consider importing `thiserror_ext::AsReport` and using `.to_report_string()` instead",
    );
}

fn check_arg(cx: &LateContext<'_>, arg_expr: &Expr<'_>, span: Span, help: impl Help + 'static) {
    let Some(error_trait_id) = cx.tcx.get_diagnostic_item(sym::Error) else {
        return;
    };

    let ty = cx.typeck_results().expr_ty(arg_expr).peel_refs();

    let help = if implements_trait(cx, ty, error_trait_id, &[]) {
        help.normal_help()
    } else if match_type(cx, ty, &ANYHOW_ERROR) {
        help.anyhow_help()
    } else if match_type(cx, ty, &THISERROR_EXT_REPORT_REPORT) {
        if let Some(help) = help.report_help() {
            help
        } else {
            return;
        }
    } else {
        return;
    };

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

trait Help {
    fn normal_help(&self) -> &'static str;
    fn anyhow_help(&self) -> &'static str {
        self.normal_help()
    }
    fn report_help(&self) -> Option<&'static str> {
        None
    }
}

impl Help for &'static str {
    fn normal_help(&self) -> &'static str {
        self
    }
}

impl Help for (&'static str, &'static str, &'static str) {
    fn normal_help(&self) -> &'static str {
        self.0
    }

    fn anyhow_help(&self) -> &'static str {
        self.1
    }

    fn report_help(&self) -> Option<&'static str> {
        Some(self.2)
    }
}

#[test]
fn ui() {
    dylint_testing::ui_test_example(env!("CARGO_PKG_NAME"), "format_error");
}
