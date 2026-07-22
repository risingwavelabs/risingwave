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

use clippy_utils::diagnostics::span_lint_and_help;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_session::{declare_tool_lint, impl_lint_pass};
use rustc_span::Symbol;

declare_tool_lint! {
    /// ### What it does
    /// Checks for updating a metric directly on a temporary returned from
    /// `with_guarded_label_values`.
    ///
    /// ### Why is this bad?
    /// `with_guarded_label_values` returns a guarded metric. The guard keeps
    /// the label values registered while the returned value is alive. Updating
    /// the metric through a temporary, such as
    /// `metrics.with_guarded_label_values(...).set(value)`, immediately drops
    /// the guard and may remove/reset the label series before Prometheus can
    /// scrape it.
    ///
    /// ### Known problems
    /// This lint intentionally only checks common metric update methods. It
    /// does not reject conversions such as `.local()` when the converted metric
    /// is stored.
    ///
    /// ### Example
    /// ```no_run
    /// metrics.with_guarded_label_values(&labels).set(value);
    /// ```
    /// Use instead:
    /// ```no_run
    /// let metric = metrics.with_guarded_label_values(&labels);
    /// metric.set(value);
    /// ```
    pub rw::TEMPORARY_GUARDED_METRIC,
    Allow,
    "updating a temporary guarded metric"
}

#[derive(Default)]
pub struct TemporaryGuardedMetric;

impl_lint_pass!(TemporaryGuardedMetric => [TEMPORARY_GUARDED_METRIC]);

impl<'tcx> LateLintPass<'tcx> for TemporaryGuardedMetric {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'_>) {
        let ExprKind::MethodCall(update_method, receiver, _, update_span) = expr.kind else {
            return;
        };

        if !is_metric_update_method(update_method.ident.name) {
            return;
        }

        if let ExprKind::MethodCall(guarded_method, _, _, _) = receiver.kind
            && guarded_method.ident.name.as_str() == "with_guarded_label_values"
        {
            span_lint_and_help(
                cx,
                TEMPORARY_GUARDED_METRIC,
                expr.span,
                "should not update a temporary guarded metric",
                Some(update_span),
                "store the value returned by `with_guarded_label_values` so the label guard lives long enough",
            );
        }
    }
}

fn is_metric_update_method(method: Symbol) -> bool {
    matches!(
        method.as_str(),
        "add" | "dec" | "dec_by" | "inc" | "inc_by" | "observe" | "set" | "start_timer" | "sub"
    )
}

#[test]
fn ui() {
    dylint_testing::ui_test_example(env!("CARGO_PKG_NAME"), "temporary_guarded_metric");
}
