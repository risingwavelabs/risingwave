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

use parse_display::Display;
use risingwave_common::bail;
use risingwave_pb::expr::window_frame::{
    PbBound, PbBoundType, PbRowsFrameBound, PbRowsFrameBounds,
};

use super::FrameBound::{
    self, CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding,
};
use super::FrameBoundsImpl;
use crate::Result;

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash)]
#[display("ROWS BETWEEN {start} AND {end}")]
pub struct RowsFrameBounds {
    pub start: RowsFrameBound,
    pub end: RowsFrameBound,
}

impl RowsFrameBounds {
    pub(super) fn from_protobuf(bounds: &PbRowsFrameBounds) -> Result<Self> {
        let start = FrameBound::<usize>::from_protobuf(bounds.get_start()?)?;
        let end = FrameBound::<usize>::from_protobuf(bounds.get_end()?)?;
        Ok(Self { start, end })
    }

    pub(super) fn to_protobuf(&self) -> PbRowsFrameBounds {
        PbRowsFrameBounds {
            start: Some(self.start.to_protobuf()),
            end: Some(self.end.to_protobuf()),
        }
    }
}

impl RowsFrameBounds {
    /// Check if the `ROWS` frame is canonical.
    ///
    /// A canonical `ROWS` frame is defined as:
    ///
    /// - Its bounds are valid (see [`Self::validate`]).
    /// - It contains the current row.
    pub fn is_canonical(&self) -> bool {
        self.validate().is_ok() && {
            let start = self.start.to_offset();
            let end = self.end.to_offset();
            start.unwrap_or(0) <= 0 && end.unwrap_or(0) >= 0
        }
    }

    /// Get the number of preceding rows.
    pub fn n_preceding_rows(&self) -> Option<usize> {
        match (&self.start, &self.end) {
            (UnboundedPreceding, _) => None,
            (Preceding(n1), Preceding(n2)) => Some(*n1.max(n2)),
            (Preceding(n), _) => Some(*n),
            (CurrentRow | Following(_) | UnboundedFollowing, _) => Some(0),
        }
    }

    /// Get the number of following rows.
    pub fn n_following_rows(&self) -> Option<usize> {
        match (&self.start, &self.end) {
            (_, UnboundedFollowing) => None,
            (Following(n1), Following(n2)) => Some(*n1.max(n2)),
            (_, Following(n)) => Some(*n),
            (_, CurrentRow | Preceding(_) | UnboundedPreceding) => Some(0),
        }
    }
}

impl FrameBoundsImpl for RowsFrameBounds {
    fn validate(&self) -> Result<()> {
        FrameBound::validate_bounds(&self.start, &self.end, |_| Ok(()))
    }
}

pub type RowsFrameBound = FrameBound<usize>;

impl RowsFrameBound {
    pub(super) fn from_protobuf_legacy(bound: &PbBound) -> Result<Self> {
        use risingwave_pb::expr::window_frame::bound::PbOffset;

        let offset = bound.get_offset()?;
        let bound = match offset {
            PbOffset::Integer(offset) => Self::from_protobuf(&PbRowsFrameBound {
                r#type: bound.get_type()? as _,
                offset: Some(*offset),
            })?,
            PbOffset::Datum(_) => bail!("offset of `RowsFrameBound` must be `Integer`"),
        };
        Ok(bound)
    }

    fn from_protobuf(bound: &PbRowsFrameBound) -> Result<Self> {
        let bound = match bound.get_type()? {
            PbBoundType::Unspecified => bail!("unspecified type of `RowsFrameBound`"),
            PbBoundType::UnboundedPreceding => Self::UnboundedPreceding,
            PbBoundType::Preceding => Self::Preceding(*bound.get_offset()? as usize),
            PbBoundType::CurrentRow => Self::CurrentRow,
            PbBoundType::Following => Self::Following(*bound.get_offset()? as usize),
            PbBoundType::UnboundedFollowing => Self::UnboundedFollowing,
        };
        Ok(bound)
    }

    fn to_protobuf(&self) -> PbRowsFrameBound {
        let (r#type, offset) = match self {
            Self::UnboundedPreceding => (PbBoundType::UnboundedPreceding, None),
            Self::Preceding(offset) => (PbBoundType::Preceding, Some(*offset as _)),
            Self::CurrentRow => (PbBoundType::CurrentRow, None),
            Self::Following(offset) => (PbBoundType::Following, Some(*offset as _)),
            Self::UnboundedFollowing => (PbBoundType::UnboundedFollowing, None),
        };
        PbRowsFrameBound {
            r#type: r#type as _,
            offset,
        }
    }
}

impl RowsFrameBound {
    /// Convert the bound to sized offset from current row. `None` if the bound is unbounded.
    pub fn to_offset(&self) -> Option<isize> {
        match self {
            UnboundedPreceding | UnboundedFollowing => None,
            CurrentRow => Some(0),
            Preceding(n) => Some(-(*n as isize)),
            Following(n) => Some(*n as isize),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_rows_frame_bounds() {
        let bounds = RowsFrameBounds {
            start: Preceding(1),
            end: CurrentRow,
        };
        assert!(bounds.validate().is_ok());
        assert!(bounds.is_canonical());
        assert_eq!(bounds.start.to_offset(), Some(-1));
        assert_eq!(bounds.end.to_offset(), Some(0));
        assert_eq!(bounds.n_preceding_rows(), Some(1));
        assert_eq!(bounds.n_following_rows(), Some(0));

        let bounds = RowsFrameBounds {
            start: CurrentRow,
            end: Following(1),
        };
        assert!(bounds.validate().is_ok());
        assert!(bounds.is_canonical());
        assert_eq!(bounds.start.to_offset(), Some(0));
        assert_eq!(bounds.end.to_offset(), Some(1));
        assert_eq!(bounds.n_preceding_rows(), Some(0));
        assert_eq!(bounds.n_following_rows(), Some(1));

        let bounds = RowsFrameBounds {
            start: UnboundedPreceding,
            end: Following(10),
        };
        assert!(bounds.validate().is_ok());
        assert!(bounds.is_canonical());
        assert_eq!(bounds.start.to_offset(), None);
        assert_eq!(bounds.end.to_offset(), Some(10));
        assert_eq!(bounds.n_preceding_rows(), None);
        assert_eq!(bounds.n_following_rows(), Some(10));

        let bounds = RowsFrameBounds {
            start: Preceding(10),
            end: UnboundedFollowing,
        };
        assert!(bounds.validate().is_ok());
        assert!(bounds.is_canonical());
        assert_eq!(bounds.start.to_offset(), Some(-10));
        assert_eq!(bounds.end.to_offset(), None);
        assert_eq!(bounds.n_preceding_rows(), Some(10));
        assert_eq!(bounds.n_following_rows(), None);

        let bounds = RowsFrameBounds {
            start: Preceding(1),
            end: Preceding(10),
        };
        assert!(bounds.validate().is_ok());
        assert!(!bounds.is_canonical());
        assert_eq!(bounds.start.to_offset(), Some(-1));
        assert_eq!(bounds.end.to_offset(), Some(-10));
        assert_eq!(bounds.n_preceding_rows(), Some(10));
        assert_eq!(bounds.n_following_rows(), Some(0));

        let bounds = RowsFrameBounds {
            start: Following(10),
            end: Following(1),
        };
        assert!(bounds.validate().is_ok());
        assert!(!bounds.is_canonical());
        assert_eq!(bounds.start.to_offset(), Some(10));
        assert_eq!(bounds.end.to_offset(), Some(1));
        assert_eq!(bounds.n_preceding_rows(), Some(0));
        assert_eq!(bounds.n_following_rows(), Some(10));

        let bounds = RowsFrameBounds {
            start: UnboundedFollowing,
            end: Following(10),
        };
        assert!(bounds.validate().is_err());
        assert!(!bounds.is_canonical());

        let bounds = RowsFrameBounds {
            start: Preceding(10),
            end: UnboundedPreceding,
        };
        assert!(bounds.validate().is_err());
        assert!(!bounds.is_canonical());
    }
}
