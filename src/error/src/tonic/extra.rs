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

use serde::{Deserialize, Serialize};

use crate::error_request_copy;

/// The score of the error.
///
/// Currently, it's used to identify the root cause of streaming pipeline failures, i.e., which actor
/// led to the failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Score(pub i32);

/// A error with a score, used to find the root cause of multiple failures.
#[derive(Debug, Clone)]
pub struct ScoredError<E> {
    pub error: E,
    pub score: Score,
}

impl<E: std::fmt::Display> std::fmt::Display for ScoredError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<E: std::error::Error> std::error::Error for ScoredError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }

    fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
        self.error.provide(request);
        // HIGHLIGHT: Provide the score to make it retrievable from meta service.
        request.provide_value(self.score);
    }
}

/// Extra fields in errors that can be passed through the gRPC boundary.
///
/// - Field being set to `None` means it is not available.
/// - To add a new field, also update the `provide` method.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct Extra {
    pub score: Option<Score>,
}

impl Extra {
    /// Create a new [`Extra`] by [requesting](std::error::request_ref) each field from the given error.
    pub fn new<T>(error: &T) -> Self
    where
        T: ?Sized + std::error::Error,
    {
        Self {
            score: error_request_copy(error),
        }
    }

    /// Provide each field to the given [request](std::error::Request).
    pub fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
        if let Some(score) = self.score {
            request.provide_value(score);
        }
    }
}
