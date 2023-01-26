// Copyright 2023 Singularity Data
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

use std::ops::Deref;

use paste::paste;

use super::*;
use crate::expr::ExprRewriter;
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};

pub trait ExprRewritable {
    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef;
}

impl ExprRewritable for PlanRef {
    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let inputs: Vec<PlanRef> = self
            .inputs()
            .iter()
            .map(|plan_ref| plan_ref.rewrite_exprs(r))
            .collect();
        let new = self.clone_with_inputs(&inputs[..]);
        let dyn_t = new.deref();
        dyn_t.rewrite_exprs(r)
    }
}

macro_rules! ban_expr_rewritable {
    ($( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ExprRewritable for [<$convention $name>] {
                fn rewrite_exprs(&self, _r: &mut dyn ExprRewriter) -> PlanRef {
                    unimplemented!()
                }
            })*
        }
    }
}
for_batch_plan_nodes! {ban_expr_rewritable}
for_stream_plan_nodes! {ban_expr_rewritable}
