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

//! Error handling utilities.
//!
//! This will eventually replace the `RwError` in `risingwave_common`.

#![feature(error_generic_member_access)]
#![feature(lint_reasons)]
#![feature(register_tool)]
#![register_tool(rw)]

pub mod tonic;
