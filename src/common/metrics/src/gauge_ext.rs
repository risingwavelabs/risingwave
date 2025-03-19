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

use prometheus::IntGauge;
use prometheus::core::{AtomicU64, GenericGauge};

/// The integer version of [`prometheus::Gauge`]. Provides better performance if metric values are
/// all unsigned integers.
pub type UintGauge = GenericGauge<AtomicU64>;

#[easy_ext::ext(IntGaugeExt)]
impl IntGauge {
    /// Increment the gauge, and return a guard that will decrement the gauge when dropped.
    pub fn inc_guard(&self) -> impl Drop + '_ {
        struct Guard<'a> {
            gauge: &'a IntGauge,
        }

        impl<'a> Guard<'a> {
            fn create(gauge: &'a IntGauge) -> Self {
                gauge.inc();
                Self { gauge }
            }
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.gauge.dec();
            }
        }

        Guard::create(self)
    }
}

#[easy_ext::ext(UintGaugeExt)]
impl UintGauge {
    /// Increment the gauge, and return a guard that will decrement the gauge when dropped.
    pub fn inc_guard(&self) -> impl Drop + '_ {
        struct Guard<'a> {
            gauge: &'a UintGauge,
        }

        impl<'a> Guard<'a> {
            fn create(gauge: &'a UintGauge) -> Self {
                gauge.inc();
                Self { gauge }
            }
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.gauge.dec();
            }
        }

        Guard::create(self)
    }
}
