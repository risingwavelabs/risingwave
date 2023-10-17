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

use proc_macro2::Ident;
use syn::spanned::Spanned;
use syn::{Token, VisRestricted, Visibility};

/// Convert a string from `snake_case` to `CamelCase`.
pub fn to_camel_case(input: &str) -> String {
    input
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first_char) => {
                    format!("{}{}", first_char.to_uppercase(), chars.as_str())
                }
            }
        })
        .collect()
}

pub(crate) fn extend_vis_with_super(vis: Visibility) -> Visibility {
    let Visibility::Restricted(vis) = vis else {
        return vis;
    };
    let VisRestricted {
        pub_token,
        paren_token,
        mut in_token,
        mut path,
    } = vis;
    let first_segment = path.segments.first_mut().unwrap();
    if first_segment.ident == "self" {
        *first_segment = Ident::new("super", first_segment.span()).into();
    } else if first_segment.ident == "super" {
        let span = first_segment.span();
        path.segments.insert(0, Ident::new("super", span).into());
        in_token.get_or_insert(Token![in](in_token.span()));
    }
    Visibility::Restricted(VisRestricted {
        pub_token,
        paren_token,
        in_token,
        path,
    })
}

#[cfg(test)]
mod tests {
    use quote::ToTokens;
    use syn::Visibility;

    use crate::utils::extend_vis_with_super;

    #[test]
    fn test_extend_vis_with_super() {
        let cases = [
            ("pub", "pub"),
            ("pub(crate)", "pub(crate)"),
            ("pub(self)", "pub(super)"),
            ("pub(super)", "pub(in super::super)"),
            ("pub(in self)", "pub(in super)"),
            (
                "pub(in self::context::data)",
                "pub(in super::context::data)",
            ),
            (
                "pub(in super::context::data)",
                "pub(in super::super::context::data)",
            ),
            ("pub(in crate::func::impl_)", "pub(in crate::func::impl_)"),
            (
                "pub(in ::risingwave_expr::func::impl_)",
                "pub(in ::risingwave_expr::func::impl_)",
            ),
        ];
        for (input, expected) in cases {
            let input: Visibility = syn::parse_str(input).unwrap();
            let expected: Visibility = syn::parse_str(expected).unwrap();
            let output = extend_vis_with_super(input);
            let expected = expected.into_token_stream().to_string();
            let output = output.into_token_stream().to_string();
            assert_eq!(expected, output);
        }
    }
}
