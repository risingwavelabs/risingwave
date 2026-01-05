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

use chrono::Duration;

use super::{Date, ScalarImpl, Timestamp};

/// A successor is a term that comes right after a particular value. Suppose n is a number (where n
/// belongs to any whole number), then the successor of n is 'n+1'. The other terminologies used for
/// a successor are just after, immediately after, and next value.
pub trait Successor {
    /// Returns the successor of the current value if it exists, otherwise returns None.
    fn successor(&self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

impl Successor for i16 {
    fn successor(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Successor for i32 {
    fn successor(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Successor for i64 {
    fn successor(&self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Successor for Timestamp {
    fn successor(&self) -> Option<Self> {
        self.0
            .checked_add_signed(Duration::nanoseconds(1))
            .map(Timestamp)
    }
}

impl Successor for Date {
    fn successor(&self) -> Option<Self> {
        self.0.checked_add_signed(Duration::days(1)).map(Date)
    }
}

impl ScalarImpl {
    /// Returns the successor of the current value if it exists.
    ///
    /// See also [`Successor`].
    ///
    /// The function may return None when:
    /// 1. The current value is the maximum value of the type.
    /// 2. The successor value of the type is not well-defined.
    pub fn successor(&self) -> Option<Self> {
        match self {
            ScalarImpl::Int16(v) => v.successor().map(ScalarImpl::Int16),
            ScalarImpl::Int32(v) => v.successor().map(ScalarImpl::Int32),
            ScalarImpl::Int64(v) => v.successor().map(ScalarImpl::Int64),
            ScalarImpl::Timestamp(v) => v.successor().map(ScalarImpl::Timestamp),
            ScalarImpl::Date(v) => v.successor().map(ScalarImpl::Date),
            _ => None,
        }
    }
}
