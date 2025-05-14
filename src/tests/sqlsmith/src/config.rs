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

#[derive(clap::Args, Clone, Debug, Default)]
pub struct SqlWeightOptions {
    /// Probability (0-100) of generating a WHERE clause.
    #[clap(long, default_value = "50")]
    pub where_clause_prob: u8,

    /// Probability (0-100) of generating a GROUP BY clause.
    #[clap(long, default_value = "50")]
    pub group_by_prob: u8,

    /// Probability (0-100) of using GROUPING SETS (only if GROUP BY is enabled).
    #[clap(long, default_value = "10")]
    pub grouping_sets_prob: u8,

    /// Probability (0-100) of generating a HAVING clause (requires GROUP BY).
    #[clap(long, default_value = "30")]
    pub having_clause_prob: u8,

    /// Probability (0-100) of generating SELECT DISTINCT instead of SELECT ALL.
    #[clap(long, default_value = "10")]
    pub distinct_prob: u8,

    /// Probability (0-100) of using aggregate expressions (e.g., SUM, COUNT).
    #[clap(long, default_value = "30")]
    pub agg_func_prob: u8,
}

impl SqlWeightOptions {
    /// Decide whether to generate something based on a probability (0-100).
    pub fn should_generate<R: rand::Rng>(rng: &mut R, prob: u8) -> bool {
        rng.random_range(0..100) < prob
    }
}
