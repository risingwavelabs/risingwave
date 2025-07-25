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

use super::*;

/// Vistis expressions in a `PlanRef`.
/// To visit recursively, call `visit_exprs_recursive` on [`VisitExprsRecursive`].
pub trait ExprVisitable {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {}
}

impl<C: ConventionMarker> ExprVisitable for PlanRef<C> {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.deref().visit_exprs(v);
    }
}
