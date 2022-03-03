#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(const_fn_trait_bound)]
#![feature(option_result_contains)]
#![feature(let_chains)]
#![feature(type_alias_impl_trait)]
#![feature(map_first_last)]

mod barrier;
pub mod cluster;
mod dashboard;
mod hummock;
pub mod manager;
mod model;
pub mod rpc;
pub mod storage;
mod stream;
pub mod test_utils;
