//! Memcomparable is an encoding format which converts the data into a form that can be directly
//! compared with memcmp.

#![deny(missing_docs)]

mod de;
mod error;
mod ser;

pub use de::{from_slice, Deserializer};
pub use error::{Error, Result};
pub use ser::{to_vec, Serializer};
