use paste::paste;

use super::super::plan_node::*;
use crate::for_all_plan_nodes;
#[derive(Debug, PartialEq)]
pub enum Convention {
    Logical,
    Batch,
    Stream,
}

pub trait WithConvention {
    fn convention(&self) -> Convention;
}

/// Define module for each node.
macro_rules! impl_convention_for_plan_node {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithConvention for [<$convention $name>] {
                fn convention(&self) -> Convention {
                    Convention::$convention
                }
            }
        })*
    }
}
for_all_plan_nodes! {impl_convention_for_plan_node }
