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

use std::fmt::Display;

use risingwave_pb::stream_plan::stream_node::PbStreamKind;
use static_assertions::const_assert_eq;

/// The kind of the changelog stream output by a stream operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// The stream contains `Insert`, `Delete`, `UpdateDelete`, and `UpdateInsert` operations.
    ///
    /// When a row is going to be updated or deleted, a `Delete` or `UpdateDelete` record
    /// containing the complete old value will be emitted first, before the new value is emitted
    /// as an `Insert` or `UpdateInsert` record.
    Retract,

    /// The stream contains only `Insert` operations.
    AppendOnly,

    /// The stream contains `Insert` and `Delete` operations.
    /// When a row is going to be updated, only the new value is emitted as an `Insert` record.
    /// When a row is going to be deleted, an incomplete `Delete` record may be emitted, where
    /// only the primary key columns are guaranteed to be set.
    ///
    /// Stateful operators typically can not process such streams correctly. It must be converted
    /// to `Retract` before being sent to stateful operators in this case.
    Upsert,
}

impl Display for StreamKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Retract => "retract",
                Self::AppendOnly => "append-only",
                Self::Upsert => "upsert",
            }
        )
    }
}

// Although there's a `to_protobuf` method, we have no way to avoid calling `as i32` to fit
// `StreamKind` to the protobuf enum. We ensure their values are the same here for safety.
const_assert_eq!(StreamKind::Retract as i32, PbStreamKind::Retract as i32);
const_assert_eq!(
    StreamKind::AppendOnly as i32,
    PbStreamKind::AppendOnly as i32
);
const_assert_eq!(StreamKind::Upsert as i32, PbStreamKind::Upsert as i32);

impl StreamKind {
    /// Returns `true` if it's [`StreamKind::AppendOnly`].
    pub fn is_append_only(self) -> bool {
        matches!(self, Self::AppendOnly)
    }

    /// Returns the stream kind representing the merge (union) of the two.
    ///
    /// Note that there should be no conflict on the stream key between the two streams,
    /// otherwise it will result in an "inconsistent" stream.
    pub fn merge(self, other: Self) -> Self {
        let any = |kind| self == kind || other == kind;

        if any(Self::Upsert) {
            Self::Upsert
        } else if any(Self::Retract) {
            Self::Retract
        } else {
            Self::AppendOnly
        }
    }

    /// Converts the stream kind to the protobuf representation.
    pub fn to_protobuf(self) -> PbStreamKind {
        match self {
            Self::Retract => PbStreamKind::Retract,
            Self::AppendOnly => PbStreamKind::AppendOnly,
            Self::Upsert => PbStreamKind::Upsert,
        }
    }
}

/// Reject upsert stream as input.
macro_rules! reject_upsert_input {
    ($input:expr) => {
        reject_upsert_input!(
            $input,
            std::any::type_name::<Self>().split("::").last().unwrap()
        )
    };

    ($input:expr, $curr:expr) => {{
        use crate::optimizer::plan_node::Explain;
        use crate::optimizer::property::StreamKind;

        let kind = $input.stream_kind();
        if let StreamKind::Upsert = kind {
            risingwave_common::bail!(
                "upsert stream is not supported as input of {}, plan:\n{}",
                $curr,
                $input.explain_to_string()
            );
        }
        kind
    }};
}
pub(crate) use reject_upsert_input;
