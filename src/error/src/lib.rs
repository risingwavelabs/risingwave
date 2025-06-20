// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Error handling utilities.
//!
//! Made into a separate crate so that it can be used in places where
//! `risingwave_common` is not available or not desired.
//!
//! The crate is also re-exported in `risingwave_common::error` for easy
//! access if `risingwave_common` is already a dependency.

#![feature(error_generic_member_access)]
#![feature(register_tool)]
#![register_tool(rw)]
#![feature(trait_alias)]

pub mod anyhow;
pub mod code;
pub mod common;
pub mod macros;
pub mod tonic;
pub mod wrappers;

// Re-export the `thiserror-ext` crate.
pub use thiserror_ext;
pub use thiserror_ext::*;

/// An error type that is [`Send`], [`Sync`], and `'static`.
pub trait Error = std::error::Error + Send + Sync + 'static;

/// A boxed error type that is [`Send`], [`Sync`], and `'static`.
pub type BoxedError = Box<dyn Error>;
