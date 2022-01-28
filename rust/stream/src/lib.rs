#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(more_qualified_paths)]
#![feature(binary_heap_drain_sorted)]
#![feature(test)]
#![feature(map_first_last)]

#[macro_use]
extern crate log;
extern crate test;

pub mod executor;
pub mod task;
