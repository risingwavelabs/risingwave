// Copyright 2024 RisingWave Labs
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

pub mod avro;
pub mod utils;

use risingwave_common::types::{DataType, Datum, DatumCow, ToOwnedDatum};
use thiserror::Error;
use thiserror_ext::Macro;

#[derive(Error, Debug, Macro)]
#[thiserror_ext(macro(mangle, path = "crate::decoder"))]
pub enum AccessError {
    #[error("Undefined field `{name}` at `{path}`")]
    Undefined { name: String, path: String },
    #[error("Cannot parse value `{value}` with type `{got}` into expected type `{expected}`")]
    TypeError {
        expected: String,
        got: String,
        value: String,
    },
    #[error("Unsupported data type `{ty}`")]
    UnsupportedType { ty: String },

    #[error("Unsupported additional column `{name}`")]
    UnsupportedAdditionalColumn { name: String },

    /// Errors that are not categorized into variants above.
    #[error("{message}")]
    Uncategorized { message: String },
}

pub type AccessResult<T = Datum> = std::result::Result<T, AccessError>;

/// Access to a field in the data structure.
pub trait Access {
    /// Accesses `path` in the data structure (*parsed* Avro/JSON/Protobuf data),
    /// and then converts it to RisingWave `Datum`.
    /// `type_expected` might or might not be used during the conversion depending on the implementation.
    ///
    /// # Path
    ///
    /// We usually expect the data is a record (struct), and `path` represents field path.
    /// The data (or part of the data) represents the whole row (`Vec<Datum>`),
    /// and we use different `path` to access one column at a time.
    ///
    /// e.g., for Avro, we access `["col_name"]`; for Debezium Avro, we access `["before", "col_name"]`.
    ///
    /// # Returns
    ///
    /// The implementation should prefer to return a borrowed [`DatumRef`](risingwave_common::types::DatumRef)
    /// through [`DatumCow::Borrowed`] to avoid unnecessary allocation if possible, especially for fields
    /// with string or bytes data. If that's not the case, it may return an owned [`Datum`] through
    /// [`DatumCow::Owned`].
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>>;
}

// Note: made an extension trait to disallow implementing or overriding `access_owned`.
#[easy_ext::ext(AccessExt)]
impl<A: Access> A {
    /// Similar to `access`, but always returns an owned [`Datum`]. See [`Access::access`] for more details.
    ///
    /// Always prefer calling `access` directly if possible to avoid unnecessary allocation.
    pub fn access_owned(&self, path: &[&str], type_expected: &DataType) -> AccessResult<Datum> {
        self.access(path, type_expected)
            .map(ToOwnedDatum::to_owned_datum)
    }
}
