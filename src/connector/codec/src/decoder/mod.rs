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

pub mod avro;
pub mod json;
pub mod protobuf;
pub mod utils;

use risingwave_common::error::NotImplemented;
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

    /// CDC auto schema change specific error that may include table context
    #[error("CDC auto schema change error: unsupported data type `{ty}` in table `{table_name}`")]
    CdcAutoSchemaChangeError { ty: String, table_name: String },

    #[error("Unsupported additional column `{name}`")]
    UnsupportedAdditionalColumn { name: String },

    #[error("Fail to convert protobuf Any into jsonb: {0}")]
    ProtobufAnyToJson(#[source] serde_json::Error),

    /// Parquet parser specific errors
    #[error("Parquet parser error: {message}")]
    ParquetParser { message: String },

    /// Errors that are not categorized into variants above.
    #[error("{message}")]
    Uncategorized { message: String },

    #[error(transparent)]
    NotImplemented(#[from] NotImplemented),
    // NOTE: We intentionally don't embed `anyhow::Error` in `AccessError` since it happens
    // in record-level and it might be too heavy to capture the backtrace
    // when creating a new `anyhow::Error`.
}

pub type AccessResult<T = Datum> = std::result::Result<T, AccessError>;

/// Access to a field in the data structure. Created by `AccessBuilder`.
///
/// It's the `ENCODE ...` part in `FORMAT ... ENCODE ...`
pub trait Access {
    /// Accesses `path` in the data structure (*parsed* Avro/JSON/Protobuf data),
    /// and then converts it to RisingWave `Datum`.
    ///
    /// `type_expected` might or might not be used during the conversion depending on the implementation.
    ///
    /// # Path
    ///
    /// We usually expect the data (`Access` instance) is a record (struct), and `path` represents field path.
    /// The data (or part of the data) represents the whole row (`Vec<Datum>`),
    /// and we use different `path` to access one column at a time.
    ///
    /// TODO: the meaning of `path` is a little confusing and maybe over-abstracted.
    /// `access` does not need to serve arbitrarily deep `path` access, but just "top-level" access.
    /// The API creates an illusion that arbitrary access is supported, but it's not.
    /// Perhaps we should separate out another trait like `ToDatum`,
    /// which only does type mapping, without caring about the path. And `path` itself is only an `enum` instead of `&[&str]`.
    ///
    /// What `path` to access is decided by the CDC layer, i.e., the `FORMAT ...` part (`ChangeEvent`).
    /// e.g.,
    /// - `DebeziumChangeEvent` accesses `["before", "col_name"]` for value,
    ///   `["source", "db"]`, `["source", "table"]` etc. for additional columns' values,
    ///   `["op"]` for op type.
    /// - `MaxwellChangeEvent` accesses `["data", "col_name"]` for value, `["type"]` for op type.
    /// - In the simplest case, for `FORMAT PLAIN/UPSERT` (`KvEvent`), they just access `["col_name"]` for value, and op type is derived.
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
