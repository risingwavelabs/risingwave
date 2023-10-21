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

use super::generic::GenericPlanRef;
use crate::optimizer::property::Order;

/// A subtrait of [`GenericPlanRef`] for batch plans.
///
/// Due to the lack of refactoring, all plan nodes currently implement this trait
/// through [`super::PlanBase`]. One may still use this trait as a bound for
/// expecting a batch plan, in contrast to [`GenericPlanRef`].
pub trait BatchPlanRef: GenericPlanRef {
    fn order(&self) -> &Order;
}
