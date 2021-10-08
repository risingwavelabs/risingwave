#![allow(dead_code)]
#![warn(clippy::map_flatten)]
#![warn(clippy::doc_markdown)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]
#![feature(binary_heap_drain_sorted)]

extern crate anyhow;
extern crate backtrace;
extern crate futures;
extern crate grpcio;
#[macro_use]
extern crate log;
extern crate either;
extern crate log4rs;
extern crate pb_convert;
extern crate protobuf;
extern crate rayon;
extern crate risingwave_proto;
extern crate thiserror;
extern crate tokio;
extern crate typed_builder;

#[macro_use]
mod error;
#[macro_use]
pub mod util;
mod alloc;
#[macro_use]
mod array2;
mod buffer;
mod catalog;
mod execution;
mod executor;
mod expr;
pub mod service;
mod source;
mod storage;
mod stream;
mod stream_op;
mod task;
#[cfg(test)]
mod test_utils;
mod types;
mod vector_op;

pub mod server;
