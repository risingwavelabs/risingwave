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

use std::fmt::{Debug, Write};
use std::sync::LazyLock;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use chrono::format::StrftimeItems;
use risingwave_common::types::{Timestamp, Timestamptz};
use risingwave_expr::{function, ExprError, Result};

use super::timestamptz::time_zone_err;

type Pattern<'a> = Vec<chrono::format::Item<'a>>;

self_cell::self_cell! {
    pub struct ChronoPattern {
        owner: String,
        #[covariant]
        dependent: Pattern,
    }
}

impl Debug for ChronoPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChronoPattern")
            .field("tmpl", self.borrow_owner())
            .finish()
    }
}

impl ChronoPattern {
    /// Compile the pg pattern to chrono pattern.
    // TODO: Chrono can not fully support the pg format, so consider using other implementations
    // later.
    pub fn compile(tmpl: &str) -> ChronoPattern {
        // mapping from pg pattern to chrono pattern
        // pg pattern: https://www.postgresql.org/docs/current/functions-formatting.html
        // chrono pattern: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        const PATTERNS: &[(&str, &str)] = &[
            ("HH24", "%H"),
            ("hh24", "%H"),
            ("HH12", "%I"),
            ("hh12", "%I"),
            ("HH", "%I"),
            ("hh", "%I"),
            ("AM", "%p"),
            ("PM", "%p"),
            ("am", "%P"),
            ("pm", "%P"),
            ("MI", "%M"),
            ("mi", "%M"),
            ("SS", "%S"),
            ("ss", "%S"),
            ("YYYY", "%Y"),
            ("yyyy", "%Y"),
            ("YY", "%y"),
            ("yy", "%y"),
            ("IYYY", "%G"),
            ("iyyy", "%G"),
            ("IY", "%g"),
            ("iy", "%g"),
            ("MM", "%m"),
            ("mm", "%m"),
            ("Month", "%B"),
            ("Mon", "%b"),
            ("DD", "%d"),
            ("dd", "%d"),
            ("US", "%6f"),
            ("us", "%6f"),
            ("MS", "%3f"),
            ("ms", "%3f"),
            ("TZH:TZM", "%:z"),
            ("tzh:tzm", "%:z"),
            ("TZHTZM", "%z"),
            ("tzhtzm", "%z"),
            ("TZH", "%#z"),
            ("tzh", "%#z"),
        ];
        // build an Aho-Corasick automaton for fast matching
        static AC: LazyLock<AhoCorasick> = LazyLock::new(|| {
            AhoCorasickBuilder::new()
                .ascii_case_insensitive(false)
                .match_kind(aho_corasick::MatchKind::LeftmostLongest)
                .build(PATTERNS.iter().map(|(k, _)| k))
                .expect("failed to build an Aho-Corasick automaton")
        });

        // replace all pg patterns with chrono patterns
        let mut chrono_tmpl = String::new();
        AC.replace_all_with(tmpl, &mut chrono_tmpl, |mat, _, dst| {
            dst.push_str(PATTERNS[mat.pattern()].1);
            true
        });
        tracing::debug!(tmpl, chrono_tmpl, "compile_pattern_to_chrono");
        ChronoPattern::new(chrono_tmpl, |tmpl| {
            StrftimeItems::new(tmpl).collect::<Vec<_>>()
        })
    }
}

#[function(
    "to_char(timestamp, varchar) -> varchar",
    prebuild = "ChronoPattern::compile($1)"
)]
fn timestamp_to_char(data: Timestamp, pattern: &ChronoPattern, writer: &mut impl Write) {
    let format = data.0.format_with_items(pattern.borrow_dependent().iter());
    write!(writer, "{}", format).unwrap();
}

#[function("to_char(timestamptz, varchar) -> varchar", rewritten)]
fn _timestamptz_to_char() {}

#[function(
    "to_char(timestamptz, varchar, varchar) -> varchar",
    prebuild = "ChronoPattern::compile($1)"
)]
fn timestamptz_to_char3(
    data: Timestamptz,
    zone: &str,
    tmpl: &ChronoPattern,
    writer: &mut impl Write,
) -> Result<()> {
    let format = data
        .to_datetime_in_zone(Timestamptz::lookup_time_zone(zone).map_err(time_zone_err)?)
        .format_with_items(tmpl.borrow_dependent().iter());
    write!(writer, "{}", format).unwrap();
    Ok(())
}
