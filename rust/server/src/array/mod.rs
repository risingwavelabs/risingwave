#![allow(clippy::module_inception)]
mod array;
pub(crate) use array::*;
mod array_data;
mod data_chunk;
pub(crate) use data_chunk::*;
mod primitive;
pub(crate) use primitive::*;
mod array_builder;
pub(crate) use array_builder::*;
