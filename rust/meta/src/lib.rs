#![allow(dead_code)]
#![warn(clippy::map_flatten)]
#![warn(clippy::doc_markdown)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]

mod catalog;
mod cluster;
mod manager;
pub mod rpc;
mod storage;
mod stream;
