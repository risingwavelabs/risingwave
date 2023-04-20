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

use std::cmp::Ordering;
use std::fmt::Display;

use risingwave_common::types::DataType;

use super::WindowFuncKind;
use crate::function::aggregate::AggArgs;

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum Frame {
    Rows(FrameBound<usize>, FrameBound<usize>),
    // Groups(FrameBound<usize>, FrameBound<usize>),
    // Range(FrameBound<ScalarImpl>, FrameBound<ScalarImpl>),
}

impl Frame {
    pub fn is_valid(&self) -> bool {
        match self {
            Frame::Rows(start, end) => start.partial_cmp(end).map(|o| o.is_le()).unwrap_or(false),
        }
    }

    pub fn start_is_unbounded(&self) -> bool {
        match self {
            Frame::Rows(start, _) => matches!(start, FrameBound::UnboundedPreceding),
        }
    }

    pub fn end_is_unbounded(&self) -> bool {
        match self {
            Frame::Rows(_, end) => matches!(end, FrameBound::UnboundedFollowing),
        }
    }
}

impl Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Rows(start, end) => {
                write!(f, "ROWS BETWEEN {} AND {}", start, end)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub enum FrameBound<T> {
    UnboundedPreceding,
    Preceding(T),
    CurrentRow,
    Following(T),
    UnboundedFollowing,
}

impl<T: Ord> PartialOrd for FrameBound<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use FrameBound::*;
        match (self, other) {
            (UnboundedPreceding, UnboundedPreceding) => None,
            (UnboundedPreceding, _) => Some(Ordering::Less),
            (_, UnboundedPreceding) => Some(Ordering::Greater),

            (UnboundedFollowing, UnboundedFollowing) => None,
            (UnboundedFollowing, _) => Some(Ordering::Greater),
            (_, UnboundedFollowing) => Some(Ordering::Less),

            (CurrentRow, CurrentRow) => Some(Ordering::Equal),

            // it's ok to think preceding(0) < current row here
            (Preceding(_), CurrentRow) => Some(Ordering::Less),
            (CurrentRow, Preceding(_)) => Some(Ordering::Greater),

            // it's ok to think following(0) > current row here
            (Following(_), CurrentRow) => Some(Ordering::Greater),
            (CurrentRow, Following(_)) => Some(Ordering::Less),

            (Preceding(n1), Preceding(n2)) => n2.partial_cmp(n1),
            (Following(n1), Following(n2)) => n1.partial_cmp(n2),
            (Preceding(_), Following(_)) => Some(Ordering::Less),
            (Following(_), Preceding(_)) => Some(Ordering::Greater),
        }
    }
}

impl Display for FrameBound<usize> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameBound::UnboundedPreceding => write!(f, "UNBOUNDED PRECEDING")?,
            FrameBound::Preceding(n) => write!(f, "{} PRECEDING", n)?,
            FrameBound::CurrentRow => write!(f, "CURRENT ROW")?,
            FrameBound::Following(n) => write!(f, "{} FOLLOWING", n)?,
            FrameBound::UnboundedFollowing => write!(f, "UNBOUNDED FOLLOWING")?,
        }
        Ok(())
    }
}

impl FrameBound<usize> {
    pub fn to_offset(&self) -> Option<isize> {
        match self {
            FrameBound::UnboundedPreceding | FrameBound::UnboundedFollowing => None,
            FrameBound::CurrentRow => Some(0),
            FrameBound::Preceding(n) => Some(-(*n as isize)),
            FrameBound::Following(n) => Some(*n as isize),
        }
    }
}

#[derive(Clone)]
pub struct WindowFuncCall {
    pub kind: WindowFuncKind,
    pub args: AggArgs,
    pub return_type: DataType,
    pub frame: Frame,
}
