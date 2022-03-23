// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use paste::paste;

use super::super::plan_node::*;
use crate::optimizer::plan_node::PlanNodeId;
use crate::session::QueryContextRef;
use crate::{for_batch_plan_nodes, for_logical_plan_nodes, for_stream_plan_nodes};

pub trait WithContext {
    fn ctx(&self) -> QueryContextRef;
}

macro_rules! impl_with_ctx {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithContext for [<$convention $name>] {
                fn ctx(&self) -> QueryContextRef {
                    self.base.ctx.clone()
                }
            }
        })*
    }
}
for_batch_plan_nodes! { impl_with_ctx }
for_logical_plan_nodes! { impl_with_ctx }
for_stream_plan_nodes! { impl_with_ctx }

pub trait WithId {
    fn id(&self) -> PlanNodeId;
}

macro_rules! impl_with_id {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithId for [<$convention $name>] {
                fn id(&self) -> PlanNodeId {
                    self.base.id
                }
            }
        })*
    }
}
for_batch_plan_nodes! { impl_with_id }
for_logical_plan_nodes! { impl_with_id }
for_stream_plan_nodes! { impl_with_id }
