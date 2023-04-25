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

use hytra::TrAdder;
use prometheus::core::{Atomic, GenericGauge};

pub struct TrAdderAtomic(TrAdder<i64>);

impl Atomic for TrAdderAtomic {
    type T = i64;

    fn new(val: i64) -> Self {
        let v = TrAdderAtomic(TrAdder::new());
        v.0.inc(val);
        v
    }

    fn set(&self, _val: i64) {
        panic!("TrAdderAtomic doesn't support set operation.")
    }

    fn get(&self) -> i64 {
        self.0.get()
    }

    fn inc_by(&self, delta: i64) {
        self.0.inc(delta)
    }

    fn dec_by(&self, delta: i64) {
        self.0.inc(-delta)
    }
}

pub type TrAdderGauge = GenericGauge<TrAdderAtomic>;
