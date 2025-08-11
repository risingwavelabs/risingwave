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

/// The kind of the changelog stream output by a stream operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// The stream contains only `Insert` operations.
    AppendOnly,

    /// The stream contains `Insert`, `Delete`, `UpdateDelete`, and `UpdateInsert` operations.
    ///
    /// When a row is going to be updated or deleted, a `Delete` or `UpdateDelete` record
    /// containing the complete old value will be emitted first, before the new value is emitted
    /// as an `Insert` or `UpdateInsert` record.
    Retract,
    // /// The stream contains `Insert` and `Delete` operations.
    // /// When a row is going to be updated, only the new value is emitted as an `Insert` record.
    // /// When a row is going to be deleted, an incomplete `Delete` record may be emitted, where
    // /// only the primary key columns are guaranteed to be set.
    // ///
    // /// Stateful operators typically can not process such streams correctly. It will be converted
    // /// to `Retract` before being sent to stateful operators in this case.
    // Upsert,
}

impl StreamKind {
    /// Returns `true` if it's [`StreamKind::AppendOnly`].
    pub fn is_append_only(self) -> bool {
        matches!(self, Self::AppendOnly)
    }
}
