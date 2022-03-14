#![allow(dead_code)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![deny(unused_must_use)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(is_sorted)]
#![feature(backtrace)]
#![feature(fn_traits)]

#[macro_use]
pub mod error;
#[macro_use]
pub mod array;
#[macro_use]
pub mod util;
mod alloc;
pub mod buffer;
pub mod catalog;
pub mod collection;
pub mod config;
pub mod expr;
pub mod hash;
#[cfg(test)]
pub mod test_utils;
pub mod types;
pub mod vector_op;
