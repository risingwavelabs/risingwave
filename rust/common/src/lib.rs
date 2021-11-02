#![allow(dead_code)]
#![warn(clippy::map_flatten)]
#![warn(clippy::doc_markdown)]
#![deny(unused_must_use)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]

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
pub mod expr;
#[cfg(test)]
pub mod test_utils;
pub mod types;
pub mod vector_op;
