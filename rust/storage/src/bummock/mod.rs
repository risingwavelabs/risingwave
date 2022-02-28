//! The implementation for Bummock indexing storage.

#![allow(dead_code)]

use std::sync::atomic::AtomicU64;

pub use index::*;
pub use iter::*;

mod index;
mod iter;

pub type AtomicDocumentId = AtomicU64;
pub type DocumentId = u64;
pub const PK_SIZE: usize = std::mem::size_of::<DocumentId>();
