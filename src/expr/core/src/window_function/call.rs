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

use std::fmt::Display;

use enum_as_inner::EnumAsInner;
use parse_display::Display;
use risingwave_common::bail;
use risingwave_common::types::{DataType, ScalarImpl, ScalarRefImpl, ToText};
use risingwave_pb::expr::window_frame::{PbBound, PbExclusion};
use risingwave_pb::expr::{PbWindowFrame, PbWindowFunction};
use FrameBound::{CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding};

use super::WindowFuncKind;
use crate::aggregate::AggArgs;
use crate::Result;

#[derive(Debug, Clone)]
pub struct WindowFuncCall {
    pub kind: WindowFuncKind,
    pub args: AggArgs,
    pub return_type: DataType,
    pub frame: Frame,
}

impl WindowFuncCall {
    pub fn from_protobuf(call: &PbWindowFunction) -> Result<Self> {
        let call = WindowFuncCall {
            kind: WindowFuncKind::from_protobuf(call.get_type()?)?,
            args: AggArgs::from_protobuf(call.get_args())?,
            return_type: DataType::from(call.get_return_type()?),
            frame: Frame::from_protobuf(call.get_frame()?)?,
        };
        Ok(call)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Frame {
    pub bounds: FrameBounds,
    pub exclusion: FrameExclusion,
}

impl Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.bounds)?;
        if self.exclusion != FrameExclusion::default() {
            write!(f, " {}", self.exclusion)?;
        }
        Ok(())
    }
}

impl Frame {
    pub fn rows(start: FrameBound<usize>, end: FrameBound<usize>) -> Self {
        Self {
            bounds: FrameBounds::Rows(RowsFrameBounds { start, end }),
            exclusion: FrameExclusion::default(),
        }
    }

    pub fn rows_with_exclusion(
        start: FrameBound<usize>,
        end: FrameBound<usize>,
        exclusion: FrameExclusion,
    ) -> Self {
        Self {
            bounds: FrameBounds::Rows(RowsFrameBounds { start, end }),
            exclusion,
        }
    }
}

impl Frame {
    pub fn from_protobuf(frame: &PbWindowFrame) -> Result<Self> {
        use risingwave_pb::expr::window_frame::PbType;
        let bounds = match frame.get_type()? {
            PbType::Unspecified => bail!("unspecified type of `WindowFrame`"),
            PbType::Rows => {
                let start = FrameBound::from_protobuf(frame.get_start()?)?;
                let end = FrameBound::from_protobuf(frame.get_end()?)?;
                FrameBounds::Rows(RowsFrameBounds { start, end })
            }
        };
        let exclusion = FrameExclusion::from_protobuf(frame.get_exclusion()?)?;
        Ok(Self { bounds, exclusion })
    }

    pub fn to_protobuf(&self) -> PbWindowFrame {
        use risingwave_pb::expr::window_frame::PbType;
        let exclusion = self.exclusion.to_protobuf() as _;
        match &self.bounds {
            FrameBounds::Rows(RowsFrameBounds { start, end }) => PbWindowFrame {
                r#type: PbType::Rows as _,
                start: Some(start.to_protobuf()),
                end: Some(end.to_protobuf()),
                exclusion,
            },
            FrameBounds::Range(_) => {
                todo!("`RANGE` frame should be temporarily banned in `LogicalOverWindow`")
            }
        }
    }
}

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash, EnumAsInner)]
#[display("{0}")]
pub enum FrameBounds {
    Rows(RowsFrameBounds),
    // Groups(GroupsFrameBounds),
    Range(RangeFrameBounds),
}

impl FrameBounds {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Rows(bounds) => bounds.validate(),
            Self::Range(bounds) => bounds.validate(),
        }
    }

    pub fn start_is_unbounded(&self) -> bool {
        match self {
            Self::Rows(RowsFrameBounds { start, .. }) => start.is_unbounded_preceding(),
            Self::Range(RangeFrameBounds { start, .. }) => start.is_unbounded_preceding(),
        }
    }

    pub fn end_is_unbounded(&self) -> bool {
        match self {
            Self::Rows(RowsFrameBounds { end, .. }) => end.is_unbounded_following(),
            Self::Range(RangeFrameBounds { end, .. }) => end.is_unbounded_following(),
        }
    }

    pub fn is_unbounded(&self) -> bool {
        self.start_is_unbounded() || self.end_is_unbounded()
    }
}

pub trait FrameBoundsImpl {
    fn validate(&self) -> Result<()>;
}

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash)]
#[display("ROWS BETWEEN {start} AND {end}")]
pub struct RowsFrameBounds {
    pub start: FrameBound<usize>,
    pub end: FrameBound<usize>,
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
        FrameBound::validate_bounds(&self.start, &self.end)
    }
}

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash, EnumAsInner)]
#[display(style = "TITLE CASE")]
pub enum FrameBound<T> {
    UnboundedPreceding,
    #[display("{0} PRECEDING")]
    Preceding(T),
    CurrentRow,
    #[display("{0} FOLLOWING")]
    Following(T),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RangeFrameBounds {
    pub start: FrameBound<ScalarImpl>,
    pub end: FrameBound<ScalarImpl>,
}

impl Display for RangeFrameBounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RANGE BETWEEN {} AND {}",
            self.start.for_display(),
            self.end.for_display()
        )?;
        Ok(())
    }
}

impl FrameBoundsImpl for RangeFrameBounds {
    fn validate(&self) -> Result<()> {
        FrameBound::validate_bounds(&self.start, &self.end, |offset| {
            match offset.as_scalar_ref_impl() {
                // TODO(): use decl macro to merge with the following
                ScalarRefImpl::Int16(val) => {
                    if val < 0 {
                        bail!("frame bound offset should be non-negative, but {} is given", val);
                    }
                }
                ScalarRefImpl::Int32(val) => {
                    if val < 0 {
                        bail!("frame bound offset should be non-negative, but {} is given", val);
                    }
                }
                ScalarRefImpl::Int64(val) => {
                    if val < 0 {
                        bail!("frame bound offset should be non-negative, but {} is given", val);
                    }
                }
                // TODO(): datetime types
                _ => unreachable!("other order column data types are not supported and should be banned in frontend"),
            }
            Ok(())
        })
    }
}

impl<T> FrameBound<T> {
    fn offset_value(&self) -> Option<&T> {
        match self {
            UnboundedPreceding | UnboundedFollowing | CurrentRow => None,
            Preceding(offset) | Following(offset) => Some(offset),
        }
    }

    fn validate_bounds(
        start: &Self,
        end: &Self,
        offset_checker: impl Fn(&T) -> Result<()>,
    ) -> Result<()> {
        match (start, end) {
            (_, UnboundedPreceding) => bail!("frame end cannot be UNBOUNDED PRECEDING"),
            (UnboundedFollowing, _) => {
                bail!("frame start cannot be UNBOUNDED FOLLOWING")
            }
            (Following(_), CurrentRow) | (Following(_), Preceding(_)) => {
                bail!("frame starting from following row cannot have preceding rows")
            }
            (CurrentRow, Preceding(_)) => {
                bail!("frame starting from current row cannot have preceding rows")
            }
            _ => {}
        }

        for bound in [start, end] {
            if let Some(offset) = bound.offset_value() {
                offset_checker(offset)?;
            }
        }

        Ok(())
    }
}

impl FrameBound<usize> {
    pub fn from_protobuf(bound: &PbBound) -> Result<Self> {
        use risingwave_pb::expr::window_frame::bound::PbOffset;
        use risingwave_pb::expr::window_frame::PbBoundType;

        let offset = bound.get_offset()?;
        let bound = match offset {
            PbOffset::Integer(offset) => match bound.get_type()? {
                PbBoundType::Unspecified => bail!("unspecified type of `FrameBound<usize>`"),
                PbBoundType::UnboundedPreceding => Self::UnboundedPreceding,
                PbBoundType::Preceding => Self::Preceding(*offset as usize),
                PbBoundType::CurrentRow => Self::CurrentRow,
                PbBoundType::Following => Self::Following(*offset as usize),
                PbBoundType::UnboundedFollowing => Self::UnboundedFollowing,
            },
            PbOffset::Datum(_) => bail!("offset of `FrameBound<usize>` must be `Integer`"),
        };
        Ok(bound)
    }

    pub fn to_protobuf(&self) -> PbBound {
        use risingwave_pb::expr::window_frame::bound::PbOffset;
        use risingwave_pb::expr::window_frame::PbBoundType;

        let (r#type, offset) = match self {
            Self::UnboundedPreceding => (PbBoundType::UnboundedPreceding, PbOffset::Integer(0)),
            Self::Preceding(offset) => (PbBoundType::Preceding, PbOffset::Integer(*offset as _)),
            Self::CurrentRow => (PbBoundType::CurrentRow, PbOffset::Integer(0)),
            Self::Following(offset) => (PbBoundType::Following, PbOffset::Integer(*offset as _)),
            Self::UnboundedFollowing => (PbBoundType::UnboundedFollowing, PbOffset::Integer(0)),
        };
        PbBound {
            r#type: r#type as _,
            offset: Some(offset),
        }
    }
}

impl FrameBound<usize> {
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

impl FrameBound<ScalarImpl> {
    fn for_display(&self) -> FrameBound<String> {
        match self {
            UnboundedPreceding => UnboundedPreceding,
            Preceding(offset) => Preceding(offset.as_scalar_ref_impl().to_text()),
            CurrentRow => CurrentRow,
            Following(offset) => Following(offset.as_scalar_ref_impl().to_text()),
            UnboundedFollowing => UnboundedFollowing,
        }
    }
}

#[derive(Display, Debug, Copy, Clone, Eq, PartialEq, Hash, Default, EnumAsInner)]
#[display("EXCLUDE {}", style = "TITLE CASE")]
pub enum FrameExclusion {
    CurrentRow,
    // Group,
    // Ties,
    #[default]
    NoOthers,
}

impl FrameExclusion {
    pub fn from_protobuf(exclusion: PbExclusion) -> Result<Self> {
        let excl = match exclusion {
            PbExclusion::Unspecified => bail!("unspecified type of `FrameExclusion`"),
            PbExclusion::CurrentRow => Self::CurrentRow,
            PbExclusion::NoOthers => Self::NoOthers,
        };
        Ok(excl)
    }

    pub fn to_protobuf(self) -> PbExclusion {
        match self {
            Self::CurrentRow => PbExclusion::CurrentRow,
            Self::NoOthers => PbExclusion::NoOthers,
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
