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

use std::backtrace::Backtrace;
use std::fmt::Display;

use datafusion_common::DataFusionError;
use risingwave_common::error::BoxedError;

/// Wraps a boxed error and optionally a captured backtrace for integration with DataFusion.
///
/// This struct is used to ensure that backtraces are displayed correctly in DataFusion error
/// messages, since DataFusion does not use `provide` to print or provide backtraces. The
/// backtrace is either extracted from the error if available, or captured at the point of
/// wrapping, and is included in the `Display` implementation.
#[derive(Debug)]
struct ToDataFusionErrorWrapper(BoxedError, Option<Backtrace>);

impl Display for ToDataFusionErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        if let Some(bt) = std::error::request_ref::<Backtrace>(self) {
            write!(f, "{}{}", DataFusionError::BACK_TRACE_SEP, bt)?;
        }
        Ok(())
    }
}

impl std::error::Error for ToDataFusionErrorWrapper {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }

    fn provide<'a>(&'a self, request: &mut std::error::Request<'a>) {
        if let Some(bt) = &self.1 {
            request.provide_ref(bt);
        }
        if let Some(source) = self.source() {
            source.provide(request);
        }
    }
}

/// Converts a boxed error into a `DataFusionError`, capturing a backtrace if one is not already
/// present in the error.
pub fn to_datafusion_error(error: impl Into<BoxedError>) -> DataFusionError {
    let error = error.into();
    let mut bt = None;
    if std::error::request_ref::<Backtrace>(error.as_ref()).is_none() {
        bt = Some(Backtrace::capture());
    }
    DataFusionError::External(Box::new(ToDataFusionErrorWrapper(error, bt)))
}
