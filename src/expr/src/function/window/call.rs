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

use risingwave_common::types::DataType;

use super::WindowFuncKind;
use crate::function::aggregate::AggArgs;

#[derive(Clone)]
pub enum Frame {
    Rows(FrameBound<usize>, FrameBound<usize>),
    // Groups(FrameBound<usize>, FrameBound<usize>),
    // Range(FrameBound<ScalarImpl>, FrameBound<ScalarImpl>),
}

#[derive(Clone)]
pub enum FrameBound<T> {
    Unbounded,
    CurrentRow,
    Preceding(T),
    Following(T),
}

impl FrameBound<usize> {
    pub fn to_offset(&self) -> Option<isize> {
        match self {
            FrameBound::Unbounded => None,
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
