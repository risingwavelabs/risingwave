#![allow(clippy::module_inception)]
mod array;
pub(crate) use array::*;
mod array_data;
mod data_chunk;
pub(crate) use data_chunk::*;
mod primitive;
pub(crate) use primitive::*;
mod decimal;
pub(crate) use decimal::*;
mod array_builder;
mod bool_array;
mod utf8_array;

pub(crate) use array_builder::*;
pub(crate) use bool_array::*;
pub(crate) use utf8_array::*;
