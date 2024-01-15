#[allow(unused_imports)]
use clippy_utils::diagnostics::span_lint; // useful for debug
use clippy_utils::diagnostics::span_lint_and_help;
use clippy_utils::higher::WhileLet;
use clippy_utils::{is_in_cfg_test, is_in_test_function, is_lint_allowed, match_def_path};
use rustc_hir::intravisit::{walk_expr, Visitor};
use rustc_hir::{Expr, ExprKind, LoopSource, MatchSource};
use rustc_lint::{LateContext, LateLintPass};
use rustc_middle::lint::is_from_async_await;
use rustc_session::{declare_tool_lint, impl_lint_pass};
use rustc_span::Span;

declare_tool_lint! {
    /// ### What it does
    /// Checks .await call in loop
    ///
    /// ### Why is this bad?
    /// This can't be concurrent
    ///
    /// ### Known problems
    ///
    /// Can cause false positives.
    ///
    /// It is possible that the loop body must be executed sequentially.
    ///
    /// Also it'll make your code a little bit more complicated, and may introduce
    /// some redundant clones due to the limitation of rust compiler. If your code
    /// is not in the critical path, feel free to ignore that.
    ///
    /// ### Example
    /// ```no_run
    /// for v in 0..10 {
    ///     f().await;
    /// }
    /// ```
    /// Use instead:
    /// ```no_run
    /// futures::future::join_all((0..10).map(|_| f())).await;
    /// ```
    pub rw::AWAIT_IN_LOOP,
    Warn,
    ".await in loop"
}

#[derive(Default)]
pub struct AwaitInLoop;

impl_lint_pass!(AwaitInLoop => [AWAIT_IN_LOOP]);

impl<'tcx> LateLintPass<'tcx> for AwaitInLoop {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'_>) {
        // Ignore if in test code.
        if is_in_cfg_test(cx.tcx, expr.hir_id) || is_in_test_function(cx.tcx, expr.hir_id) {
            return;
        }

        if is_lint_allowed(cx, AWAIT_IN_LOOP, expr.hir_id) {
            return;
        }

        let mut visitor = AwaitInLoopVisitor {
            cx,
            in_loop: None,
            found_await: None,
        };

        visitor.visit_expr(expr);

        if let Some((await_span, in_loop_span)) = visitor.found_await {
            span_lint_and_help(
                cx,
                AWAIT_IN_LOOP,
                vec![await_span, in_loop_span],
                ".await in loop",
                None,
                "consider make it concurrent using futures::future::join_all, or ignore the lint if the loop body must be executed sequentially",
            )
        }
    }
}

struct AwaitInLoopVisitor<'hir, 'tcx> {
    // Useful for debug.
    cx: &'hir LateContext<'tcx>,
    in_loop: Option<Span>,
    // The first span is the `.await` and the second span is the loop.
    found_await: Option<(Span, Span)>,
}

impl<'hir, 'tcx> Visitor<'hir> for AwaitInLoopVisitor<'hir, 'tcx> {
    fn visit_expr(&mut self, ex: &'hir Expr<'_>) {
        match &ex.kind {
            ExprKind::Loop(_block, _label, source, _span)
                // In most cases, a raw `loop` can't be concurrent.
                if !matches!(source, LoopSource::Loop) =>
            {
                // `.await` will be expanded as a match-until-yield loop in HIR.
                // We need to ignore the expanded loop itself.
                if !is_from_async_await(ex.span) {
                    self.in_loop = Some(ex.span);
                    if let Some(WhileLet {
                        let_pat: _,
                        // Don't visit `let_expr` to avoid such case:
                        // ```rust
                        // while let Some(_) = s.next().await {}
                        // ```
                        let_expr: _,
                        if_then,
                    }) = WhileLet::hir(ex)
                    {
                        self.visit_expr(if_then);
                    } else {
                        walk_expr(self, ex);
                    }
                    self.in_loop = None;
                } else {
                    walk_expr(self, ex);
                }
            }
            ExprKind::Match(sub, _, source) if matches!(source, MatchSource::AwaitDesugar) => 'm: {
                if let Some(in_loop_span) = self.in_loop {
                    let typeck_results = self.cx.typeck_results();
                    let ty = typeck_results.expr_ty(sub);
                    if let rustc_middle::ty::Adt(adt, _) = ty.kind() {
                        for path in [
                            &["tokio", "time", "sleep", "Sleep"] as &[&str],
                            &["tokio", "runtime", "task", "join", "JoinHandle"],
                        ] {
                            if match_def_path(self.cx, adt.did(), path) {
                                break 'm;
                            }
                        }
                    }
                    let _ = self.found_await.get_or_insert((ex.span, in_loop_span));
                }
            }
            _ => walk_expr(self, ex),
        }
    }
}

#[test]
fn ui() {
    dylint_testing::ui_test_example(env!("CARGO_PKG_NAME"), "await_in_loop");
}
