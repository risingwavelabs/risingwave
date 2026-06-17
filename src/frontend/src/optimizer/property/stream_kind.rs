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

    /// The stream carries a hidden `_row_id` column, but the column has not been filled yet.
    ///
    /// This is a frontend-only intermediate state before `StreamRowIdGen`. When the stream is
    /// serialized to protobuf before row id generation, it is represented by its semantic
    /// append-only property. If `append_only` is true, all visible operations must be insert-like
    /// operations (`Insert` or `UpdateInsert`) and some `_row_id` values may still be unfilled, but
    /// they must be unique after being filled later. If it is false, the stream should be treated
    /// like an upsert stream until row id is filled.
    RowIdNotFilled { append_only: bool },
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
                Self::RowIdNotFilled { append_only: true } => {
                    "row-id-not-filled-append-only"
                }
                Self::RowIdNotFilled { append_only: false } => "row-id-not-filled-upsert",
            }
        )
    }
}

impl StreamKind {
    /// Returns `true` if it's [`StreamKind::Retract`].
    pub fn is_retract(self) -> bool {
        matches!(self, Self::Retract)
    }

    /// Returns `true` if it's [`StreamKind::AppendOnly`].
    pub fn is_append_only(self) -> bool {
        matches!(self, Self::AppendOnly)
    }

    /// Returns `true` if it's [`StreamKind::Upsert`].
    pub fn is_upsert(self) -> bool {
        matches!(self, Self::Upsert)
    }

    /// Returns `true` if it's [`StreamKind::RowIdNotFilled`].
    pub fn is_row_id_not_filled(self) -> bool {
        matches!(self, Self::RowIdNotFilled { .. })
    }

    /// Returns whether the stream is semantically append-only.
    ///
    /// This differs from [`Self::is_append_only`] for `RowIdNotFilled`, which is still an
    /// unresolved frontend-only stream kind and should not be accepted by stateful operators.
    pub fn is_semantic_append_only(self) -> bool {
        match self {
            Self::AppendOnly => true,
            Self::RowIdNotFilled { append_only } => append_only,
            Self::Retract | Self::Upsert => false,
        }
    }

    /// Returns the stream kind representing the merge (union) of the two.
    ///
    /// Note that there should be no conflict on the stream key between the two streams,
    /// otherwise it will result in an "inconsistent" stream.
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (
                Self::RowIdNotFilled { append_only: left },
                Self::RowIdNotFilled { append_only: right },
            ) => Self::RowIdNotFilled {
                append_only: left && right,
            },
            (Self::RowIdNotFilled { append_only }, Self::AppendOnly)
            | (Self::AppendOnly, Self::RowIdNotFilled { append_only }) => {
                Self::RowIdNotFilled { append_only }
            }
            (Self::RowIdNotFilled { .. }, _) | (_, Self::RowIdNotFilled { .. }) => {
                Self::RowIdNotFilled { append_only: false }
            }
            (Self::Upsert, _) | (_, Self::Upsert) => Self::Upsert,
            (Self::Retract, _) | (_, Self::Retract) => Self::Retract,
            (Self::AppendOnly, Self::AppendOnly) => Self::AppendOnly,
        }
    }

    /// Converts the stream kind to the protobuf representation.
    pub fn to_protobuf(self) -> PbStreamKind {
        match self {
            Self::Retract => PbStreamKind::Retract,
            Self::AppendOnly => PbStreamKind::AppendOnly,
            Self::Upsert => PbStreamKind::Upsert,
            Self::RowIdNotFilled { append_only: true } => PbStreamKind::AppendOnly,
            Self::RowIdNotFilled { append_only: false } => PbStreamKind::Upsert,
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
        if matches!(kind, StreamKind::Upsert | StreamKind::RowIdNotFilled { .. }) {
            risingwave_common::bail!(
                "{} stream is not supported as input of {}, plan:\n{}",
                kind,
                $curr,
                $input.explain_to_string()
            );
        }
        kind
    }};
}
pub(crate) use reject_upsert_input;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_protobuf() {
        assert_eq!(StreamKind::Retract.to_protobuf(), PbStreamKind::Retract);
        assert_eq!(
            StreamKind::AppendOnly.to_protobuf(),
            PbStreamKind::AppendOnly
        );
        assert_eq!(StreamKind::Upsert.to_protobuf(), PbStreamKind::Upsert);
        assert_eq!(
            StreamKind::RowIdNotFilled { append_only: true }.to_protobuf(),
            PbStreamKind::AppendOnly
        );
        assert_eq!(
            StreamKind::RowIdNotFilled { append_only: false }.to_protobuf(),
            PbStreamKind::Upsert
        );
    }

    #[test]
    fn test_merge_row_id_not_filled() {
        assert_eq!(
            StreamKind::RowIdNotFilled { append_only: true }.merge(StreamKind::AppendOnly),
            StreamKind::RowIdNotFilled { append_only: true }
        );
        assert_eq!(
            StreamKind::RowIdNotFilled { append_only: true }.merge(StreamKind::Upsert),
            StreamKind::RowIdNotFilled { append_only: false }
        );
        assert_eq!(
            StreamKind::RowIdNotFilled { append_only: false }
                .merge(StreamKind::RowIdNotFilled { append_only: true }),
            StreamKind::RowIdNotFilled { append_only: false }
        );
    }
}
