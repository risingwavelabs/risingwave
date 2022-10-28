// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(exit_status_error)]
#![feature(let_chains)]
#![feature(lint_reasons)]

mod config;
pub use config::*;
mod config_gen;
pub use config_gen::*;
mod preflight_check;
pub use preflight_check::*;
pub mod service_config;
pub use service_config::*;
mod compose;
pub use compose::*;
mod compose_deploy;
pub use compose_deploy::*;
mod risectl_env;
pub use risectl_env::*;

mod task;
pub mod util;
mod wait;
pub use task::*;
