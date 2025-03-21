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

#![feature(trait_alias)]
#![feature(let_chains)]
#![feature(try_blocks)]
#![feature(register_tool)]
#![feature(if_let_guard)]
#![register_tool(rw)]
#![allow(rw::format_error)] // test code

pub mod client;
pub mod cluster;
pub mod ctl_ext;
pub mod kafka;
pub mod nexmark;
pub mod slt;
pub mod utils;

risingwave_batch_executors::enable!();
risingwave_expr_impl::enable!();
