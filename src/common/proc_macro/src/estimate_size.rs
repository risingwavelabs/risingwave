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

// Copyright (c) 2022 Denis Kerp
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

fn has_nested_flag_attribute(
    attr: &syn::Attribute,
    name: &'static str,
    flag: &'static str,
) -> bool {
    if let Ok(meta) = attr.parse_meta() {
        if let Some(ident) = meta.path().get_ident() {
            if *ident == name {
                if let syn::Meta::List(list) = meta {
                    for nested in list.nested.iter() {
                        if let syn::NestedMeta::Meta(syn::Meta::Path(path)) = nested {
                            let path = path
                                .get_ident()
                                .expect("Invalid attribute syntax! (no ident)")
                                .to_string();
                            if path == flag {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }

    false
}

pub fn has_nested_flag_attribute_list(
    list: &[syn::Attribute],
    name: &'static str,
    flag: &'static str,
) -> bool {
    for attr in list.iter() {
        if has_nested_flag_attribute(attr, name, flag) {
            return true;
        }
    }

    false
}

pub fn extract_ignored_generics_list(list: &[syn::Attribute]) -> Vec<String> {
    let mut collection = Vec::new();

    for attr in list.iter() {
        let mut list = extract_ignored_generics(attr);

        collection.append(&mut list);
    }

    collection
}

pub fn extract_ignored_generics(attr: &syn::Attribute) -> Vec<String> {
    let mut collection = Vec::new();

    if let Ok(meta) = attr.parse_meta() {
        if let Some(ident) = meta.path().get_ident() {
            if &ident.to_string() != "get_size" {
                return collection;
            }
            if let syn::Meta::List(list) = meta {
                for nested in list.nested.iter() {
                    if let syn::NestedMeta::Meta(nmeta) = nested {
                        let ident = nmeta
                            .path()
                            .get_ident()
                            .expect("Invalid attribute syntax! (no iden)");
                        if &ident.to_string() != "ignore" {
                            panic!(
                                "Invalid attribute syntax! Unknown name {:?}",
                                ident.to_string()
                            );
                        }

                        if let syn::Meta::List(list) = nmeta {
                            for nested in list.nested.iter() {
                                if let syn::NestedMeta::Meta(syn::Meta::Path(path)) = nested {
                                    let path = path
                                        .get_ident()
                                        .expect("Invalid attribute syntax! (no ident)")
                                        .to_string();
                                    collection.push(path);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    collection
}

// Add a bound `T: EstimateSize` to every type parameter T, unless we ignore it.
pub fn add_trait_bounds(mut generics: syn::Generics, ignored: &[String]) -> syn::Generics {
    for param in &mut generics.params {
        if let syn::GenericParam::Type(type_param) = param {
            let name = type_param.ident.to_string();
            let mut found = false;
            for ignored in ignored.iter() {
                if ignored == &name {
                    found = true;
                    break;
                }
            }
            if found {
                continue;
            }
            type_param.bounds.push(syn::parse_quote!(EstimateSize));
        }
    }
    generics
}
