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

use std::sync::OnceLock;

static TOGGLE_OTEL_LAYER: OnceLock<Box<dyn Fn(bool) + Sync + Send>> = OnceLock::new();

/// Set the function to toggle the opentelemetry tracing layer. Panics if called twice.
pub fn set_toggle_otel_layer_fn(f: impl Fn(bool) + Sync + Send + 'static) {
    TOGGLE_OTEL_LAYER
        .set(Box::new(f))
        .ok()
        .expect("toggle otel layer fn set twice");
}

/// Toggle the opentelemetry tracing layer.
pub fn toggle_otel_layer(enabled: bool) {
    if let Some(f) = TOGGLE_OTEL_LAYER.get() {
        f(enabled);
    }
}
