// Copyright 2026 RisingWave Labs
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

/// A utility struct that runs a closure when dropped, unless `disarm` is called.
/// This is useful for ensuring that certain cleanup or error handling code is executed
/// when a scope is exited, unless explicitly marked as successful.
pub struct DropGuard<F: FnOnce()> {
    on_drop: Option<F>,
}

impl<F: FnOnce()> Drop for DropGuard<F> {
    fn drop(&mut self) {
        if let Some(on_drop) = self.on_drop.take() {
            on_drop();
        }
    }
}

impl<F: FnOnce()> DropGuard<F> {
    /// Creates a new `DropGuard` that will execute the given closure when dropped.
    pub fn new(on_drop: F) -> Self {
        Self {
            on_drop: Some(on_drop),
        }
    }

    /// Prevents the `DropGuard` from executing the closure when dropped.
    pub fn disarm(mut self) {
        self.on_drop = None;
    }
}
