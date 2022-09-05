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
use risingwave_pb::batch_plan::plan_node as pb_batch_node;
use risingwave_pb::stream_plan::stream_node as pb_stream_node;

use super::*;
use crate::{
    for_all_plan_nodes, for_batch_plan_nodes, for_logical_plan_nodes, for_stream_plan_nodes,
};

pub trait ToProst: ToBatchProst + StreamNode {}

pub trait ToBatchProst {
    fn to_batch_prost_body(&self) -> pb_batch_node::NodeBody {
        unimplemented!()
    }
}

pub trait StreamNode {
    fn to_stream_prost_body(
        &self,
        _state: &mut BuildFragmentGraphState,
    ) -> pb_stream_node::NodeBody {
        unimplemented!()
    }
}

/// impl `ToProst` nodes which have impl `ToBatchProst` and `ToStreamProst`.
macro_rules! impl_to_prost {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToProst for [<$convention $name>] { })*
        }
    }
}
for_all_plan_nodes! { impl_to_prost }
/// impl a panic `ToBatchProst` for logical and stream node.
macro_rules! ban_to_batch_prost {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToBatchProst for [<$convention $name>] {
                fn to_batch_prost_body(&self) -> pb_batch_node::NodeBody {
                    panic!("convert into distributed is only allowed on batch plan")
                }
            })*
        }
    }
}
for_logical_plan_nodes! { ban_to_batch_prost }
for_stream_plan_nodes! { ban_to_batch_prost }
/// impl a panic `ToStreamProst` for logical and batch node.
macro_rules! ban_to_stream_prost {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl StreamNode for [<$convention $name>] {
                fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> pb_stream_node::NodeBody {
                    panic!("convert into distributed is only allowed on stream plan")
                }
            })*
        }
    }
}
for_logical_plan_nodes! { ban_to_stream_prost }
for_batch_plan_nodes! { ban_to_stream_prost }
