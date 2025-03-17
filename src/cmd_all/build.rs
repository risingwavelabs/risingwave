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

use thiserror_ext::AsReport;
use vergen::EmitBuilder;

fn main() {
    if let Err(e) = EmitBuilder::builder().git_sha(true).fail_on_error().emit() {
        // Leave the environment variable unset if error occurs.
        println!("cargo:warning={}", e.as_report())
    }
}
