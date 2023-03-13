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
use ouroboros::self_referencing;
use risingwave_common::types::NaiveDateTimeWrapper;

// use risingwave_expr_macro::function;

#[self_referencing]
pub struct ChronoPattern {
    pub(crate) tmpl: String,
    #[borrows(tmpl)]
    #[covariant]
    pub(crate) items: Vec<chrono::format::Item<'this>>,
}

impl Debug for ChronoPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChronoPattern")
            .field("tmpl", self.borrow_tmpl())
            .finish()
    }
}

/// Compile the pg pattern to chrono pattern.
// TODO: Chrono can not fully support the pg format, so consider using other implementations later.
pub fn compile_pattern_to_chrono(tmpl: &str) -> ChronoPattern {
    // https://www.postgresql.org/docs/current/functions-formatting.html
    static PG_PATTERNS: &[&str] = &[
        "HH24", "hh24", "HH12", "hh12", "HH", "hh", "MI", "mi", "SS", "ss", "YYYY", "yyyy", "YY",
        "yy", "IYYY", "iyyy", "IY", "iy", "MM", "mm", "Month", "Mon", "DD", "dd",
    ];
    // https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    static CHRONO_PATTERNS: &[&str] = &[
        "%H", "%H", "%I", "%I", "%I", "%I", "%M", "%M", "%S", "%S", "%Y", "%Y", "%y", "%y", "%G",
        "%G", "%g", "%g", "%m", "%m", "%B", "%b", "%d", "%d",
    ];
    static AC: LazyLock<AhoCorasick> = LazyLock::new(|| {
        AhoCorasickBuilder::new()
            .ascii_case_insensitive(false)
            .match_kind(aho_corasick::MatchKind::LeftmostLongest)
            .build(PG_PATTERNS)
    });

    let mut chrono_tmpl = String::new();
    AC.replace_all_with(tmpl, &mut chrono_tmpl, |mat, _, dst| {
        dst.push_str(CHRONO_PATTERNS[mat.pattern()]);
        true
    });
    ChronoPatternBuilder {
        tmpl: chrono_tmpl,
        items_builder: |tmpl| StrftimeItems::new(tmpl).collect::<Vec<_>>(),
    }
    .build()
}

// #[function("to_char(timestamp, varchar) -> varchar")]
pub fn to_char_timestamp(data: NaiveDateTimeWrapper, tmpl: &str, writer: &mut dyn Write) {
    let pattern = compile_pattern_to_chrono(tmpl);
    let format = data.0.format_with_items(pattern.borrow_items().iter());
    write!(writer, "{}", format).unwrap();
}
