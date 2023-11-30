use clippy_utils::diagnostics::span_lint;
use clippy_utils::macros::{
    find_format_arg_expr, find_format_args, is_format_macro, macro_backtrace,
};
use clippy_utils::match_function_call;
use clippy_utils::ty::implements_trait;
use rustc_ast::FormatArgsPiece;
use rustc_hir::Expr;
use rustc_lint::{LateContext, LateLintPass};
use rustc_session::{declare_tool_lint, impl_lint_pass};
use rustc_span::sym;

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
        // `%err`, `?err` in tracing events and spans.
        if let Some(args) = match_function_call(cx, expr, &TRACING_FIELD_DEBUG)
            .or_else(|| match_function_call(cx, expr, &TRACING_FIELD_DISPLAY))
            && let [arg_expr, ..] = args
        {
            check_arg(cx, arg_expr);
        }

        for macro_call in macro_backtrace(expr.span) {
            if is_format_macro(cx, macro_call.def_id)
            && let Some(format_args) = find_format_args(cx, expr, macro_call.expn) {
                for piece in &format_args.template {
                    if let FormatArgsPiece::Placeholder(placeholder) = piece
                        && let Ok(index) = placeholder.argument.index
                        && let Some(arg) = format_args.arguments.all_args().get(index)
                        && let Ok(arg_expr) = find_format_arg_expr(expr, arg)
                    {
                        check_arg(cx, arg_expr);
                    }
                }
            }
        }
    }
}

fn check_arg(cx: &LateContext<'_>, arg_expr: &Expr<'_>) {
    let Some(error_trait_id) = cx.tcx.get_diagnostic_item(sym::Error) else {
        return;
    };

    let ty = cx.typeck_results().expr_ty(arg_expr).peel_refs();

    if implements_trait(cx, ty, error_trait_id, &[]) {
        if let Some(span) = core::iter::successors(Some(arg_expr.span), |s| s.parent_callsite())
            .find(|s| s.can_be_used_for_suggestions())
        {
            // TODO: suggest `err.as_report()`
            span_lint(cx, FORMAT_ERROR, span, "should not format error directly");
        }
    }
}

#[test]
fn ui() {
    dylint_testing::ui_test_example(env!("CARGO_PKG_NAME"), "format_error");
}
