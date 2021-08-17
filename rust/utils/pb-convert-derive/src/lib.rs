//! This crate provides procedure macros to automatically derive implementation of `FromProtobuf`
//! and `IntoProtobuf`.
//!
//! # Design
//!
//! `pb-convert` is designed to help converting protobuf messages from/to rust structs.
//!
//! # Attributes
//!
//! This crate provides attributes in [`pb_convert`] to help customize behavior of derived
//! implementation.
//!
//! ## Struct/Enum Attributes
//!
//! - `pb_type`: *Mandatory* This attribute specifies what protobuf message your struct/enum can be
//! converted from/to.
//!
//! ## Struct Field Attributes
//!
//! - `pb_field`: *Optional* This attribute specifies corresponding field name in protobuf
//! message, by default it's same as struct field name.
//!
//! - `skip`: *Optional* This attribute specifies whether field should be ignored when converting
//! from/to protobuf message.
//!
//! ## Enum Variant Attributes
//!
//! - `pb_variant`: *Optional* This attribute specifies corresponding enum variant defined in
//! protobuf message, by default it's same as enum variant name.
//!
//! # Example
//!
//! ## Setup dependency
//!
//! To use `pb-convert`, add following to your Cargo.toml's dependency section:
//! ```toml
//! [dependencies]
//! pb-convert-derive = {path = "../utils/pb-convert-derive" }
//! pb-convert = {path = "../utils/pb-convert" }
//! ```
//!
//! ## Derive Conversion
//!
//! ```ignore
//! use pb_convert::{IntoProtobuf, FromProtobuf};
//!
//! #[derive(FromProtobuf, IntoProtobuf)]
//! #[pb_convert(pb_type = "proto::DatabaseRefId")]
//! pub struct DatabaseId {
//!   database_id: i32,
//! }
//!
//! #[derive(IntoProtobuf, FromProtobuf)]
//! #[pb_convert(pb_type = "proto::SchemaRefId")]
//! pub struct SchemaId {
//!   database_ref_id: DatabaseId,
//!   // Use `s_id` in protobuf message.
//!   #[pb_convert(pb_field = "s_id")]
//!   schema_id: i32,
//!   // `other_field` will be ignored when covnerting from/to protobuf message.
//!   #[pb_convert(skip=true)]
//!   other_field: i32
//! }
//!
//! #[derive(IntoProtobuf, FromProtobuf)]
//! #[pb_convert(pb_type = "proto::DataTypeKindProto")]
//! pub enum DataTypeKind {
//!   INT32,
//!   #[pb_convert(pb_variant = "DOUBLE")]
//!   FLOAT64,
//! }
//!
//! ```
//!
//! And protobuf message definition:
//! ```ignore
//! message DatabaseRefId {
//!     int32 database_id = 1;
//! }
//!
//! message SchemaRefId {
//!     DatabaseRefId database_ref_id = 1;
//!     int32 s_id = 2;
//! }
//!
//! enum DataTypeKindProto {
//!   INT32,
//!   DOUBLE
//! }
//! ```
//!
//! For a complete example, please refer to [`tests`].
//!
mod internal;

extern crate anyhow;
extern crate darling;
extern crate phf;
extern crate proc_macro;
extern crate quote;
extern crate syn;

use crate::internal::Container;
use proc_macro::TokenStream;
use syn::parse_macro_input;
use syn::DeriveInput;

#[proc_macro_derive(FromProtobuf, attributes(pb_convert))]
pub fn derive_from_pb(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    Container::from_ast(&ast)
        .and_then(|c| c.expend(true))
        .map(TokenStream::from)
        .expect("Failed to derive FromPb")
}

#[proc_macro_derive(IntoProtobuf, attributes(pb_convert))]
pub fn derive_into_pb(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    Container::from_ast(&ast)
        .and_then(|c| c.expend(false))
        .map(TokenStream::from)
        .expect("Failed to derive IntoPb")
}
