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
use std::path::PathBuf;

use thiserror_ext::AsReport;

fn parse_pub_mods_with_syn(lib_rs: &str) -> BTreeSet<String> {
    syn::parse_file(lib_rs)
        .expect("failed to parse lib.rs content")
        .items
        .into_iter()
        .filter_map(|item| match item {
            syn::Item::Mod(module) if matches!(module.vis, syn::Visibility::Public(_)) => {
                Some(module.ident.to_string())
            }
            _ => None,
        })
        .collect()
}

macro_rules! collect_modules_in_for_all_meta_model_entities {
    ($($module:ident),* $(,)?) => {{
        [$(stringify!($module)),*]
            .into_iter()
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<String>>()
    }};
}

fn parse_table_names_with_syn(source: &str, source_name: &str) -> BTreeSet<String> {
    let mut table_names = BTreeSet::new();
    let parsed = syn::parse_file(source)
        .unwrap_or_else(|err| panic!("failed to parse {source_name}: {}", err.as_report()));
    for item in parsed.items {
        let syn::Item::Struct(item_struct) = item else {
            continue;
        };
        let mut saw_sea_orm_attr = false;
        let mut saw_table_name = false;
        for attr in item_struct.attrs {
            if !attr.path().is_ident("sea_orm") {
                continue;
            }
            saw_sea_orm_attr = true;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("table_name") {
                    let value = meta.value()?;
                    table_names.insert(value.parse::<syn::LitStr>()?.value());
                    saw_table_name = true;
                }
                Ok(())
            })
            .unwrap_or_else(|err| {
                panic!(
                    "failed to parse #[sea_orm(...)] attribute on struct `{}` in {source_name}: {}",
                    item_struct.ident,
                    err.as_report()
                )
            });
        }
        assert!(
            !saw_sea_orm_attr || saw_table_name,
            "struct `{}` has #[sea_orm(...)] but no table_name",
            item_struct.ident
        );
    }
    table_names
}

fn parse_table_names_in_modules(modules: impl IntoIterator<Item = String>) -> BTreeSet<String> {
    let model_src_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    modules
        .into_iter()
        .flat_map(|module| {
            let module_path = model_src_dir.join(format!("{module}.rs"));
            let source = std::fs::read_to_string(&module_path).unwrap_or_else(|err| {
                panic!(
                    "failed to read {}: {}",
                    module_path.display(),
                    err.as_report()
                )
            });
            parse_table_names_with_syn(&source, &module_path.display().to_string())
        })
        .collect()
}

#[test]
fn for_all_meta_model_entities_should_cover_all_entity_table_names() {
    let lib_rs = include_str!("../src/lib.rs");

    let expected = parse_table_names_in_modules(
        parse_pub_mods_with_syn(lib_rs)
            .into_iter()
            .filter(|m| m != "prelude")
            .collect::<BTreeSet<_>>(),
    );
    let actual = parse_table_names_in_modules(risingwave_meta_model::for_all_meta_model_entities!(
        collect_modules_in_for_all_meta_model_entities
    ));

    let missing: Vec<_> = expected.difference(&actual).cloned().collect();
    let unexpected: Vec<_> = actual.difference(&expected).cloned().collect();

    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "for_all_meta_model_entities is out of sync with meta model sea_orm table_name definitions.\n\
Missing in for_all_meta_model_entities: {missing:?}\n\
Unexpected in for_all_meta_model_entities: {unexpected:?}"
    );
}
