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

use fixedbitset::FixedBitSet;

use super::generic::PhysicalPlanRef;

#[auto_impl::auto_impl(&)]
pub trait StreamSpecific {
    fn append_only(&self) -> bool;
    fn emit_on_window_close(&self) -> bool;
    fn watermark_columns(&self) -> &FixedBitSet;
}


/// A subtrait of [`PhysicalPlanRef`] for stream plans.
///
/// Due to the lack of refactoring, all plan nodes currently implement this trait
/// through [`super::PlanBase`]. One may still use this trait as a bound for
/// accessing a stream plan, in contrast to [`GenericPlanRef`] or
/// [`PhysicalPlanRef`].
///
/// [`GenericPlanRef`]: super::generic::GenericPlanRef
pub trait StreamPlanRef = PhysicalPlanRef + StreamSpecific;
