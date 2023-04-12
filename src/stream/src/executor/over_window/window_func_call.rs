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

use std::ops::Bound;

use risingwave_common::types::ScalarImpl;
use risingwave_expr::expr::WindowFuncKind;

use super::WindowFuncState;
use crate::executor::aggregation::AggArgs;
use crate::executor::over_window::state::{LagState, LeadState};

#[derive(Clone)]
pub(super) enum Frame {
    Offset(isize), // for `LAG` and `LEAD`
    Rows(Bound<usize>, Bound<usize>),
    Groups(Bound<usize>, Bound<usize>),
    Range(Bound<ScalarImpl>, Bound<ScalarImpl>),
}

#[derive(Clone)]
pub(super) struct WindowFuncCall {
    pub kind: WindowFuncKind,
    pub args: AggArgs, // TODO(): give `AggArgs` a more general name
    pub frame: Frame,
}

impl WindowFuncCall {
    pub(super) fn new_state(&self) -> Box<dyn WindowFuncState> {
        use WindowFuncKind::*;
        match self.kind {
            Lag => Box::new(LagState::new(&self.frame)),
            Lead => Box::new(LeadState::new(&self.frame)),
            FirstValue => todo!(),
            LastValue => todo!(),
            NthValue => todo!(),
            Aggregate(_) => todo!(),
        }
    }
}
