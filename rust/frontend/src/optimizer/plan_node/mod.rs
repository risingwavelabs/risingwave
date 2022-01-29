#![allow(rustdoc::private_intra_doc_links)]
//! Defines all kinds of node in the plan tree, each node represent a relational expression.
//!
//! We use a immutable style tree structure, every Node are immutable and cannot be modified after
//! it has been created. If you want to modify the node, such as rewriting the expression in a
//! ProjectNode or changing a node's input node, you need to create a new node. We use Rc as the
//! node's reference, and a node just storage its inputs' reference, so change a node just need
//! create one new node but not the entire sub-tree.
//!
//! So when you want to add a new node, make sure:
//! - each field in the node struct are private
//! - recommend to implement the construction of Node in a unified `new()` function, if have multi
//!   methods to construct, make they have a consistent behavior
//! - all field should be valued in construction, so the porperties' derivation should be finished
//!   in the `new()` function.

use std::fmt::{Debug, Display};
use std::rc::Rc;

use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{self, DynClone};
use paste::paste;

use super::property::{WithConvention, WithDistribution, WithOrder, WithSchema};
/// The common trait over all plan nodes. Used by optimizer framework which will treate all node as
/// `dyn PlanNode`
///
/// We split the trait into lots of sub-trait so that we can easily use macro to impl them.
pub trait PlanNode:
    PlanTreeNode
    + DynClone
    + Debug
    + Display
    + Downcast
    + WithConvention
    + WithOrder
    + WithDistribution
    + WithSchema
    + ColPrunable
    + IntoPlanRef
    + ToBatch
    + ToStream
    + ToDistributedBatch
{
    fn node_type(&self) -> PlanNodeType;
}

impl_downcast!(PlanNode);
pub type PlanRef = Rc<dyn PlanNode>;

impl dyn PlanNode {
    /// Write explain the whole plan tree.
    pub fn explain(&self, level: usize, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(f, "{}{}", " ".repeat(level * 2), self)?;
        for input in self.inputs() {
            input.explain(level + 1, f)?;
        }
        Ok(())
    }
}

#[macro_use]
mod plan_tree_node;
pub use plan_tree_node::*;
mod col_pruning;
pub use col_pruning::*;
mod convert;
pub use convert::*;
mod join_predicate;
pub use join_predicate::*;

// SOME Intellisense DONT UNDERSTAND THIS.
//
// /// Define module for each plan node.
// macro_rules! def_mod_and_use {
//   ([], $( { $convention:ident, $name:ident }),*) => {
//       $(paste! {
//           mod [<$convention:snake _ $name:snake>];
//           pub use [<$convention:snake _ $name:snake>]::[<$convention $name>];
//       })*
//   }
// }
// for_all_plan_nodes! {def_mod_and_use }

mod batch_exchange;
mod batch_hash_join;
mod batch_project;
mod batch_seq_scan;
mod batch_sort;
mod batch_sort_merge_join;
mod logical_agg;
mod logical_filter;
mod logical_join;
mod logical_project;
mod logical_scan;
mod logical_values;
mod stream_exchange;
mod stream_hash_join;
mod stream_project;
mod stream_table_source;
pub use batch_exchange::BatchExchange;
pub use batch_hash_join::BatchHashJoin;
pub use batch_project::BatchProject;
pub use batch_seq_scan::BatchSeqScan;
pub use batch_sort::BatchSort;
pub use batch_sort_merge_join::BatchSortMergeJoin;
pub use logical_agg::LogicalAgg;
pub use logical_filter::LogicalFilter;
pub use logical_join::LogicalJoin;
pub use logical_project::LogicalProject;
pub use logical_scan::LogicalScan;
pub use logical_values::LogicalValues;
pub use stream_exchange::StreamExchange;
pub use stream_hash_join::StreamHashJoin;
pub use stream_project::StreamProject;
pub use stream_table_source::StreamTableSource;

/// [`for_all_plan_nodes`] includes all plan nodes. If you added a new plan node
/// inside the project, be sure to add here and in its conventions like [`for_logical_plan_nodes`]
///
/// Every tuple has two elements, where `{ convention, name }`
/// You can use it as follows
/// ```rust
/// macro_rules! use_plan {
///     ([], $({ $convention:ident, $name:ident }),*) => {};
/// }
/// frontend::for_all_plan_nodes! { use_plan }
/// ```
/// See the following implementations for example.
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
      $macro! {
          [$($x),*]
          ,{ Logical, Agg}
          ,{ Logical, Filter}
          ,{ Logical, Project}
          ,{ Logical, Scan}
          ,{ Logical, Join}
          ,{ Logical, Values}
          // ,{ Logical, Sort} we don't need a LogicalSort, just require the Order
          ,{ Batch, Project}
          ,{ Batch, SeqScan}
          ,{ Batch, HashJoin}
          ,{ Batch, SortMergeJoin}
          ,{ Batch, Sort}
          ,{ Batch, Exchange}
          ,{ Stream, Project}
          ,{ Stream, TableSource}
          ,{ Stream, HashJoin}
          ,{ Stream, Exchange}
      }
  };
}
/// `for_logical_plan_nodes` includes all plan nodes with logical convention.
#[macro_export]
macro_rules! for_logical_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ Logical, Agg}
            ,{ Logical, Filter}
            ,{ Logical, Project}
            ,{ Logical, Scan}
            ,{ Logical, Join}
            ,{ Logical, Values}
            // ,{ Logical, Sort} not sure if we will support Order by clause in subquery/view/MV
            // if we dont support thatk, we don't need LogicalSort, just require the Order at the top of query
        }
    };
}

/// `for_batch_plan_nodes` includes all plan nodes with batch convention.
#[macro_export]
macro_rules! for_batch_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ Batch, Project}
            ,{ Batch, SeqScan}
            ,{ Batch, HashJoin}
            ,{ Batch, SortMergeJoin}
            ,{ Batch, Sort}
            ,{ Batch, Exchange}
        }
    };
}

/// `for_stream_plan_nodes` includes all plan nodes with stream convention.
#[macro_export]
macro_rules! for_stream_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ Stream, Project}
            ,{ Stream, TableSource}
            ,{ Stream, HashJoin}
            ,{ Stream, Exchange}
        }
    };
}

/// impl PlanNodeType fn for each node.
macro_rules! enum_plan_node_type {
  ([], $( { $convention:ident, $name:ident }),*) => {
    paste!{
      /// each enum value represent a PlanNode struct type, help us to dispatch and downcast
      pub enum PlanNodeType{
        $(  [<$convention $name>] ),*
      }

      $(impl PlanNode for [<$convention $name>] {
          fn node_type(&self) -> PlanNodeType{
            PlanNodeType::[<$convention $name>]
          }
        })*
    }
  }
}
for_all_plan_nodes! {enum_plan_node_type }

pub trait IntoPlanRef {
    fn into_plan_ref(self) -> PlanRef;
    fn clone_as_plan_ref(&self) -> PlanRef;
}
/// impl fn plan_ref for each node.
macro_rules! impl_plan_ref {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl IntoPlanRef for [<$convention $name>] {
                fn into_plan_ref(self) -> PlanRef {
                    std::rc::Rc::new(self)
                }
                fn clone_as_plan_ref(&self) -> PlanRef{
                    self.clone().into_plan_ref()
                }
            })*
        }
    }
}
for_all_plan_nodes! {impl_plan_ref }

/// impl plan node downcast fn for each node.
macro_rules! impl_down_cast_fn {
    ([], $( { $convention:ident, $name:ident }),*) => {
        paste!{
            #[allow(unused)]
            impl dyn PlanNode {
                $( pub fn [< as_$convention:snake _ $name:snake>](&self) -> Option<&[<$convention $name>]> {
                    self.downcast_ref::<[<$convention $name>]>()
                } )*
            }
        }
    }
}
for_all_plan_nodes! {impl_down_cast_fn }
