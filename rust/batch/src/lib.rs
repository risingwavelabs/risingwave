#![allow(dead_code)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(test)]
#![feature(map_first_last)]

pub mod execution;
pub mod executor;
pub mod rpc;
pub mod task;

#[macro_use]
extern crate log;
#[macro_use]
extern crate risingwave_common;
extern crate test;
