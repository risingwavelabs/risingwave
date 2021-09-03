#![allow(clippy::module_inception)]
mod array;
pub use array::*;
mod array_data;
mod data_chunk;
pub use data_chunk::*;
mod primitive;
pub use primitive::*;
mod decimal;
pub use decimal::*;
mod array_builder;
mod bool_array;
pub mod interval_array;
mod utf8_array;

pub use array_builder::*;
pub use bool_array::*;
pub use utf8_array::*;
