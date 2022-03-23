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
//
use paste::paste;

use super::*;
use crate::optimizer::property::{Distribution, Order};
use crate::utils::ColIndexMapping;
use crate::{for_batch_plan_nodes, for_logical_plan_nodes, for_stream_plan_nodes};

/// `ToStream` converts a logical plan node to streaming physical node
/// with an optional required distribution.
///
/// when implement this trait you can choose the two ways
/// - Implement `to_stream` and use the default implementation of `to_stream_with_dist_required`
/// - Or, if the required distribution is given, there will be a better plan. For example a hash
///   join with hash-key(a,b) and the plan is required hash-distributed by (a,b,c). you can
///   implement `to_stream_with_dist_required`, and implement `to_stream` with
///   `to_stream_with_dist_required(Distribution::any())`. you can see
///   (`LogicalProject`)[LogicalProject] as an example.
pub trait ToStream {
    /// `logical_rewrite_for_stream` will rewrite the logical node, and return (new_plan_node,
    /// col_mapping), the col_mapping is for original columns have been changed into some other
    /// position.
    /// now it is used to 1. ensure every plan node's output having pk column
    /// 2. (todo) add row_count() in every Agg
    fn logical_rewrite_for_stream(&self) -> (PlanRef, ColIndexMapping);
    /// `to_stream` is equivalent to `to_stream_with_dist_required(Distribution::any())`
    fn to_stream(&self) -> PlanRef;
    /// convert the plan to streaming physical plan and satisfy the required distribution
    fn to_stream_with_dist_required(&self, required_dist: &Distribution) -> PlanRef {
        let ret = self.to_stream();
        required_dist.enforce_if_not_satisfies(ret, Order::any())
    }
}

/// `ToBatch` allows to convert a logical plan node to batch physical node
/// with an optional required order.
///
/// when implement this trait you can choose the two ways
/// - Implement `to_batch` and use the default implementation of `to_batch_with_order_required`
/// - Or, if the required order is given, there will be a better plan. For example a join with
///   join-key(a,b) and the plan is required sorted by (a,b,c), a sort merge join is better. you can
///   implement `to_batch_with_order_required`, and implement `to_batch` with
///   `to_batch_with_order_required(Order::any())`. you can see (`LogicalJoin`)[LogicalJoin] as an
///   example.
pub trait ToBatch {
    /// `to_batch` is equivalent to `to_batch_with_order_required(Order::any())`
    fn to_batch(&self) -> PlanRef;
    /// convert the plan to batch physical plan and satisfy the required Order
    fn to_batch_with_order_required(&self, required_order: &Order) -> PlanRef {
        let ret = self.to_batch();
        required_order.enforce_if_not_satisfies(ret)
    }
}

/// `ToDistributedBatch` allows to convert a batch physical plan to distributed batch plan, by
/// insert exchange node, with an optional required order and distributed.
///
/// when implement this trait you can choose the two ways
/// - Implement `to_distributed` and use the default implementation of
///   `to_distributed_with_required`
/// - Or, if the required order and distribution is given, there will be a better plan. For example
///   a hash join with hash-key(a,b) and the plan is required hash-distributed by (a,b,c). you can
///   implement `to_distributed_with_required`, and implement `to_distributed` with
///   `to_distributed_with_required(Order::any())`.
pub trait ToDistributedBatch {
    /// `to_distributed` is equivalent to `to_distributed_with_required(Distribution::any(),
    /// Order::any())`
    fn to_distributed(&self) -> PlanRef;
    /// insert the exchange in batch physical plan to satisfy the required Distribution and Order.
    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &Distribution,
    ) -> PlanRef {
        let ret = self.to_distributed();
        let ret = required_order.enforce_if_not_satisfies(ret);
        required_dist.enforce_if_not_satisfies(ret, required_order)
    }
}

/// Implement ToBatch for batch and streaming node.
macro_rules! impl_to_batch {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToBatch for [<$convention $name>] {
                fn to_batch(&self) -> PlanRef {
                    panic!("convert into batch is only allowed on logical plan")
                }
            })*
        }
    }
}
for_batch_plan_nodes! { impl_to_batch }
for_stream_plan_nodes! { impl_to_batch }

/// Implement ToStream for batch and streaming node.
macro_rules! impl_to_stream {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToStream for [<$convention $name>] {
                fn to_stream(&self) -> PlanRef {
                    panic!("convert into stream is only allowed on logical plan")
                }
            })*
        }
    }
}
for_batch_plan_nodes! { impl_to_stream }
for_stream_plan_nodes! { impl_to_stream }

/// impl ToDistributedBatch  for logical and streaming node.
macro_rules! ban_to_distributed {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ToDistributedBatch for [<$convention $name>] {
                fn to_distributed(&self) -> PlanRef {
                    panic!("convert into distributed is only allowed on batch plan")
                }
            })*
        }
    }
}
for_logical_plan_nodes! { ban_to_distributed }
for_stream_plan_nodes! { ban_to_distributed }
