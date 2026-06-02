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

//! Streaming `MATCH_RECOGNIZE` (row pattern recognition) executor.
//!
//! Contains the pure row-pattern NFA ([`nfa`]) and the streaming executor built on top of it.

// TODO: remove once `from_proto` wires `MatchRecognizeExecutor` (held back pending e2e/SLT verification
// so deployed MATCH_RECOGNIZE keeps the explicit not-implemented error rather than running unverified).
#![allow(dead_code, unused_imports)]

mod executor;
mod nfa;
mod parse;

pub use executor::{MatchRecognizeExecutor, MatchRecognizeExecutorArgs};
