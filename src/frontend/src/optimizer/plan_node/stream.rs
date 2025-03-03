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

use super::generic::PhysicalPlanRef;
use crate::optimizer::property::{MonotonicityMap, WatermarkColumns};

/// A subtrait of [`PhysicalPlanRef`] for stream plans.
///
/// Due to the lack of refactoring, all plan nodes currently implement this trait
/// through [`super::PlanBase`]. One may still use this trait as a bound for
/// accessing a stream plan, in contrast to [`GenericPlanRef`] or
/// [`PhysicalPlanRef`].
///
/// [`GenericPlanRef`]: super::generic::GenericPlanRef
#[auto_impl::auto_impl(&)]
pub trait StreamPlanRef: PhysicalPlanRef {
    fn append_only(&self) -> bool;
    fn emit_on_window_close(&self) -> bool;
    fn watermark_columns(&self) -> &WatermarkColumns;
    fn columns_monotonicity(&self) -> &MonotonicityMap;
}

/// Prelude for stream plan nodes.
pub mod prelude {
    pub use super::super::Stream;
    pub use super::super::generic::{GenericPlanRef, PhysicalPlanRef};
    pub use super::StreamPlanRef;
}
