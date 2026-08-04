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

//! Streaming `MATCH_RECOGNIZE` (SQL:2016 row pattern recognition).
//!
//! This module currently carries the pattern-matching core — the Thompson-construction NFA, its
//! preference-order walkers, the catastrophic-backtracking defenses (per-start failure memo and
//! the per-visit scan budget) — shared by the executor that lands next in the series.

pub mod nfa;
