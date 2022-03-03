use paste::paste;
use risingwave_pb::plan::plan_node as pb_batch_node;
use risingwave_pb::stream_plan::stream_node as pb_stream_node;

use super::*;
use crate::{
    for_all_plan_nodes, for_batch_plan_nodes, for_logical_plan_nodes, for_stream_plan_nodes,
};

pub trait ToProst: ToBatchProst + ToStreamProst {}

pub trait ToBatchProst {
    fn to_batch_prost_body(&self) -> pb_batch_node::NodeBody {
        unimplemented!()
    }
}

pub trait ToStreamProst {
    fn to_stream_prost_body(&self) -> pb_stream_node::Node {
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
for_all_plan_nodes! {impl_to_prost}
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
        $(impl ToStreamProst for [<$convention $name>] {
            fn to_stream_prost_body(&self) -> pb_stream_node::Node {
                panic!("convert into distributed is only allowed on stream plan")
            }
       })*
      }
    }
  }
for_logical_plan_nodes! { ban_to_stream_prost }
for_batch_plan_nodes! { ban_to_stream_prost }
