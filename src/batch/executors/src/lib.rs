#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(exact_size_is_empty)]
#![feature(type_alias_impl_trait)]
#![cfg_attr(coverage, feature(coverage_attribute))]
#![feature(coroutines)]
#![feature(proc_macro_hygiene, stmt_expr_attributes)]
#![feature(iterator_try_collect)]
#![recursion_limit = "256"]
#![feature(let_chains)]
#![feature(int_roundings)]
#![feature(allocator_api)]
#![feature(impl_trait_in_assoc_type)]
#![feature(assert_matches)]
#![feature(error_generic_member_access)]
#![feature(map_try_insert)]
#![feature(iter_from_coroutine)]
#![feature(used_with_arg)]

pub mod executor;
pub use risingwave_batch::{error, task};

#[macro_use]
extern crate tracing;
#[macro_use]
extern crate risingwave_common;

#[cfg(test)]
risingwave_expr_impl::enable!();
