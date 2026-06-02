// Copyright 2026 RisingWave Labs
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

use std::collections::BTreeSet;

fn parse_pub_mods(lib_rs: &str) -> BTreeSet<String> {
    let mut mods = BTreeSet::new();
    for line in lib_rs.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("pub mod ") else {
            continue;
        };
        let rest = rest.trim();
        let Some(name) = rest.strip_suffix(';') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            mods.insert(name.to_owned());
        }
    }
    mods
}

fn parse_models_in_for_all_meta_model_entities(lib_rs: &str) -> BTreeSet<String> {
    let Some((_, after_macro_start)) =
        lib_rs.split_once("macro_rules! for_all_meta_model_entities")
    else {
        panic!("for_all_meta_model_entities macro not found");
    };
    let Some((macro_body, _)) = after_macro_start.split_once("};\n}\n") else {
        panic!("for_all_meta_model_entities macro end not found");
    };

    macro_body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.strip_suffix(','))
        .filter(|line| line.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .map(ToOwned::to_owned)
        .collect()
}

#[test]
fn for_all_meta_model_entities_should_cover_all_entity_modules() {
    let lib_rs = include_str!("../src/lib.rs");

    let all_meta_model_modules = parse_pub_mods(lib_rs);
    let expected: BTreeSet<String> = all_meta_model_modules
        .into_iter()
        .filter(|m| m != "prelude")
        .collect();
    let actual = parse_models_in_for_all_meta_model_entities(lib_rs);

    let missing: Vec<_> = expected.difference(&actual).cloned().collect();
    let unexpected: Vec<_> = actual.difference(&expected).cloned().collect();

    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "for_all_meta_model_entities is out of sync with risingwave_meta_model::lib.\n\
Missing in for_all_meta_model_entities: {missing:?}\n\
Unexpected in for_all_meta_model_entities: {unexpected:?}"
    );
}
