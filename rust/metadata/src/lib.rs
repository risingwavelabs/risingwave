#![allow(dead_code)]
#![warn(clippy::map_flatten)]
#![warn(clippy::doc_markdown)]
// Enable this rule if https://github.com/brendanzab/approx/issues/73 resolved.
#![allow(clippy::if_then_panic)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]

extern crate anyhow;
extern crate backtrace;
extern crate either;
extern crate futures;
extern crate log;
extern crate log4rs;
extern crate pb_convert;
extern crate protobuf;
extern crate risingwave_common;
extern crate risingwave_proto;
extern crate thiserror;
extern crate tokio;
extern crate typed_builder;

mod metadata;
pub mod rpc;
