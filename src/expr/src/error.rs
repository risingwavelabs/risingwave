// Copyright 2023 RisingWave Labs
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

use risingwave_common::array::ArrayError;
use risingwave_common::cast::{ByteaCastError, DateTimeCastError};
use risingwave_common::error::{BoxedError, ErrorCode, RwError};
use risingwave_common::types::{DataType, JsonbError};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_pb::PbFieldNotFound;
use snafu::Snafu;

/// A specialized Result type for expression operations.
pub type Result<T, E = ExprError> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum ExprError {
    #[snafu(display("unsupported function `{name}`"))]
    UnsupportedFunction { name: Box<str> },

    #[snafu(display("cannot cast `{from}` to `{to}`"))]
    UnsupportedCast { from: DataType, to: DataType },

    #[snafu(display("out of range when casting to {to}"))]
    CastOutOfRange { to: &'static str },

    #[snafu(display("numeric out of range"))]
    NumericOutOfRange,

    #[snafu(display("numeric out of range (underflow)"))]
    NumericUnderflow,

    #[snafu(display("numeric out of range (overflow)"))]
    NumericOverflow,

    #[snafu(display("division by zero"))]
    DivisionByZero,

    #[snafu(display("cannot cast to bytea"), context(false))]
    ByteaCast {
        #[snafu(source(from(ByteaCastError, Box::new)))]
        source: Box<ByteaCastError>,
    },

    #[snafu(display("cannot cast to date/time"), context(false))]
    DateTimeCast { source: DateTimeCastError },

    #[snafu(display("failed to manipulate jsonb"), context(false))]
    Jsonb {
        #[snafu(source(from(JsonbError, Box::new)))]
        source: Box<JsonbError>,
    },

    #[snafu(display("parse error"))] // other parse errors
    Parse { source: BoxedError },

    #[snafu(
        display("failed to serialize/deserailize ordered value"),
        context(false)
    )]
    MemcmpEncoding { source: memcomparable::Error },

    #[snafu(display("failed to serialize/deserailize value"), context(false))]
    ValueEncoding {
        #[snafu(source(from(ValueEncodingError, Box::new)))]
        source: Box<ValueEncodingError>,
    },

    #[snafu(display("invalid parameter `{name}`: {reason}"))]
    InvalidParam {
        name: &'static str,
        reason: Box<str>,
    },

    #[snafu(display("array error"), context(false))]
    Array { source: ArrayError },

    #[snafu(display("udf error"), context(false))]
    Udf { source: risingwave_udf::Error },

    #[snafu(display("more than one row returned by {name} used as an expression"))]
    MaxOneRow { name: &'static str },

    #[snafu(display("not a constant"))]
    NotConstant,

    #[snafu(display("context not found"))]
    Context,

    #[snafu(display("field name must not be null"))]
    FieldNameNull,

    #[snafu(display("failed to build expression"), context(false))]
    BuildFromProto { source: PbFieldNotFound },

    // TODO: remove this variant and make errors categorized
    // TODO(snafu): may use `whatever`
    #[snafu(display("uncategorized error"), context(false))]
    Internal { source: anyhow::Error },
}

static_assertions::const_assert_eq!(std::mem::size_of::<ExprError>(), 40);

impl From<ExprError> for RwError {
    fn from(s: ExprError) -> Self {
        ErrorCode::ExprError(Box::new(s)).into()
    }
}

impl From<regex::Error> for ExprError {
    fn from(re: regex::Error) -> Self {
        Self::InvalidParam {
            name: "pattern",
            reason: re.to_string().into(),
        }
    }
}

impl From<chrono::ParseError> for ExprError {
    fn from(e: chrono::ParseError) -> Self {
        Self::Parse { source: e.into() }
    }
}
