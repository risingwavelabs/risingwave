#![allow(dead_code)]
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

// This is a bug of rustc which warn me to remove macro_use, I have add this.
#[allow(unused_imports)]
#[macro_use]
extern crate risingwave_common;
#[macro_use]
extern crate log;
extern crate test;

pub mod rpc;
