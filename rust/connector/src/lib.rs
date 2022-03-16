#![allow(dead_code)]
#![warn(clippy::dbg_macro)]
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

pub mod base;
mod filesystem;
mod kafka;
pub mod kinesis;
mod pulsar;
pub mod state;

use crate::kinesis::config::AwsConfigInfo;

#[derive(Clone, Debug)]
pub enum ConnectorConfig {
    Kinesis(AwsConfigInfo),
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
