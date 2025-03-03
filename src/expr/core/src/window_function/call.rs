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

use FrameBound::{CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding};
use enum_as_inner::EnumAsInner;
use parse_display::Display;
use risingwave_common::types::DataType;
use risingwave_common::{bail, must_match};
use risingwave_pb::expr::window_frame::{PbBounds, PbExclusion};
use risingwave_pb::expr::{PbWindowFrame, PbWindowFunction};

use super::{
    RangeFrameBounds, RowsFrameBound, RowsFrameBounds, SessionFrameBounds, WindowFuncKind,
};
use crate::Result;
use crate::aggregate::AggArgs;

#[derive(Debug, Clone)]
pub struct WindowFuncCall {
    pub kind: WindowFuncKind,
    pub return_type: DataType,
    pub args: AggArgs,
    pub ignore_nulls: bool,
    pub frame: Frame,
}

impl WindowFuncCall {
    pub fn from_protobuf(call: &PbWindowFunction) -> Result<Self> {
        let call = WindowFuncCall {
            kind: WindowFuncKind::from_protobuf(call.get_type()?)?,
            return_type: DataType::from(call.get_return_type()?),
            args: AggArgs::from_protobuf(call.get_args())?,
            ignore_nulls: call.get_ignore_nulls(),
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
    pub fn rows(start: RowsFrameBound, end: RowsFrameBound) -> Self {
        Self {
            bounds: FrameBounds::Rows(RowsFrameBounds { start, end }),
            exclusion: FrameExclusion::default(),
        }
    }

    pub fn rows_with_exclusion(
        start: RowsFrameBound,
        end: RowsFrameBound,
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
            PbType::RowsLegacy => {
                #[expect(deprecated)]
                {
                    let start = FrameBound::<usize>::from_protobuf_legacy(frame.get_start()?)?;
                    let end = FrameBound::<usize>::from_protobuf_legacy(frame.get_end()?)?;
                    FrameBounds::Rows(RowsFrameBounds { start, end })
                }
            }
            PbType::Rows => {
                let bounds = must_match!(frame.get_bounds()?, PbBounds::Rows(bounds) => bounds);
                FrameBounds::Rows(RowsFrameBounds::from_protobuf(bounds)?)
            }
            PbType::Range => {
                let bounds = must_match!(frame.get_bounds()?, PbBounds::Range(bounds) => bounds);
                FrameBounds::Range(RangeFrameBounds::from_protobuf(bounds)?)
            }
            PbType::Session => {
                let bounds = must_match!(frame.get_bounds()?, PbBounds::Session(bounds) => bounds);
                FrameBounds::Session(SessionFrameBounds::from_protobuf(bounds)?)
            }
        };
        let exclusion = FrameExclusion::from_protobuf(frame.get_exclusion()?)?;
        Ok(Self { bounds, exclusion })
    }

    pub fn to_protobuf(&self) -> PbWindowFrame {
        use risingwave_pb::expr::window_frame::PbType;
        let exclusion = self.exclusion.to_protobuf() as _;
        #[expect(deprecated)] // because of `start` and `end` fields
        match &self.bounds {
            FrameBounds::Rows(bounds) => PbWindowFrame {
                r#type: PbType::Rows as _,
                start: None, // deprecated
                end: None,   // deprecated
                exclusion,
                bounds: Some(PbBounds::Rows(bounds.to_protobuf())),
            },
            FrameBounds::Range(bounds) => PbWindowFrame {
                r#type: PbType::Range as _,
                start: None, // deprecated
                end: None,   // deprecated
                exclusion,
                bounds: Some(PbBounds::Range(bounds.to_protobuf())),
            },
            FrameBounds::Session(bounds) => PbWindowFrame {
                r#type: PbType::Session as _,
                start: None, // deprecated
                end: None,   // deprecated
                exclusion,
                bounds: Some(PbBounds::Session(bounds.to_protobuf())),
            },
        }
    }
}

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash, EnumAsInner)]
#[display("{0}")]
pub enum FrameBounds {
    Rows(RowsFrameBounds),
    // Groups(GroupsFrameBounds),
    Range(RangeFrameBounds),
    Session(SessionFrameBounds),
}

impl FrameBounds {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Rows(bounds) => bounds.validate(),
            Self::Range(bounds) => bounds.validate(),
            Self::Session(bounds) => bounds.validate(),
        }
    }

    pub fn start_is_unbounded(&self) -> bool {
        match self {
            Self::Rows(RowsFrameBounds { start, .. }) => start.is_unbounded_preceding(),
            Self::Range(RangeFrameBounds { start, .. }) => start.is_unbounded_preceding(),
            Self::Session(_) => false,
        }
    }

    pub fn end_is_unbounded(&self) -> bool {
        match self {
            Self::Rows(RowsFrameBounds { end, .. }) => end.is_unbounded_following(),
            Self::Range(RangeFrameBounds { end, .. }) => end.is_unbounded_following(),
            Self::Session(_) => false,
        }
    }

    pub fn is_unbounded(&self) -> bool {
        self.start_is_unbounded() || self.end_is_unbounded()
    }
}

pub trait FrameBoundsImpl {
    fn validate(&self) -> Result<()>;
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

impl<T> FrameBound<T> {
    fn offset_value(&self) -> Option<&T> {
        match self {
            UnboundedPreceding | UnboundedFollowing | CurrentRow => None,
            Preceding(offset) | Following(offset) => Some(offset),
        }
    }

    pub(super) fn validate_bounds(
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

    pub fn map<U>(self, f: impl Fn(T) -> U) -> FrameBound<U> {
        match self {
            UnboundedPreceding => UnboundedPreceding,
            Preceding(offset) => Preceding(f(offset)),
            CurrentRow => CurrentRow,
            Following(offset) => Following(f(offset)),
            UnboundedFollowing => UnboundedFollowing,
        }
    }
}

impl<T> FrameBound<T>
where
    T: Copy,
{
    pub(super) fn reverse(self) -> FrameBound<T> {
        match self {
            UnboundedPreceding => UnboundedFollowing,
            Preceding(offset) => Following(offset),
            CurrentRow => CurrentRow,
            Following(offset) => Preceding(offset),
            UnboundedFollowing => UnboundedPreceding,
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
    fn from_protobuf(exclusion: PbExclusion) -> Result<Self> {
        let excl = match exclusion {
            PbExclusion::Unspecified => bail!("unspecified type of `FrameExclusion`"),
            PbExclusion::CurrentRow => Self::CurrentRow,
            PbExclusion::NoOthers => Self::NoOthers,
        };
        Ok(excl)
    }

    fn to_protobuf(self) -> PbExclusion {
        match self {
            Self::CurrentRow => PbExclusion::CurrentRow,
            Self::NoOthers => PbExclusion::NoOthers,
        }
    }
}
