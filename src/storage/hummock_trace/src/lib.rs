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

// FIXME: open the assert when 1.8.2 is stable that can use #![feature(cursor_split)] instead of #![feature(cursor_remaining)]
// see: https://github.com/rust-lang/rust/pull/109174 
// #![feature(cursor_remaining)]

#![feature(trait_alias)]
#![feature(coroutines)]

mod collector;
mod error;
mod opts;
mod read;
mod record;
mod replay;
mod write;
use std::future::Future;

pub use collector::*;
pub use error::*;
pub use opts::*;
pub use read::*;
pub use record::*;
pub use replay::*;
pub(crate) use write::*;

pub fn hummock_trace_scope<F: Future>(f: F) -> impl Future<Output = F::Output> {
    let id = CONCURRENT_ID.next();
    LOCAL_ID.scope(id, f)
}
