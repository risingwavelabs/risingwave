// Copyright 2024 RisingWave Labs
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

#[cfg(loom)]
mod inner {
    pub(crate) use loom::sync::*;

    pub(crate) struct Mutex<T> {
        inner: loom::sync::Mutex<T>,
    }

    impl<T> Mutex<T> {
        pub(crate) fn new(value: T) -> Self {
            Self {
                inner: loom::sync::Mutex::new(value),
            }
        }

        pub(crate) fn lock(&self) -> loom::sync::MutexGuard<'_, T> {
            self.inner.lock().expect("should not fail")
        }
    }
}

#[cfg(not(loom))]
mod inner {
    pub(crate) use std::sync::*;

    pub(crate) use parking_lot::Mutex;
}

pub(crate) use inner::*;
