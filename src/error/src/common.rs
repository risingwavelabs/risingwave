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

//! Commonly used error types.

use std::fmt::{Display, Formatter};

use thiserror::Error;
use thiserror_ext::Macro;

#[derive(Debug, Clone, Copy, Default)]
pub struct TrackingIssue(Option<u32>);

impl TrackingIssue {
    pub fn new(id: u32) -> Self {
        TrackingIssue(Some(id))
    }

    pub fn none() -> Self {
        TrackingIssue(None)
    }
}

impl From<u32> for TrackingIssue {
    fn from(id: u32) -> Self {
        TrackingIssue(Some(id))
    }
}

impl From<Option<u32>> for TrackingIssue {
    fn from(id: Option<u32>) -> Self {
        TrackingIssue(id)
    }
}

impl Display for TrackingIssue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(id) => write!(
                f,
                "Tracking issue: https://github.com/risingwavelabs/risingwave/issues/{id}"
            ),
            None => write!(
                f,
                "No tracking issue yet. Feel free to submit a feature request at https://github.com/risingwavelabs/risingwave/issues/new?labels=type%2Ffeature&template=feature_request.yml"
            ),
        }
    }
}

#[derive(Error, Debug, Macro)]
#[error("Feature is not yet implemented: {feature}\n{issue}")]
#[thiserror_ext(macro(path = "crate::common"))]
pub struct NotImplemented {
    #[message]
    pub feature: String,
    pub issue: TrackingIssue,
}

#[derive(Error, Debug, Macro)]
#[thiserror_ext(macro(path = "crate::common"))]
pub struct NoFunction {
    #[message]
    pub sig: String,
    pub candidates: Option<String>,
}

impl Display for NoFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "function {} does not exist", self.sig)?;
        if let Some(candidates) = &self.candidates {
            write!(f, ", do you mean {}", candidates)?;
        }
        Ok(())
    }
}
