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

use std::borrow::Cow;
use std::collections::BTreeMap;

use anyhow::Result;

/// The runtime-visible variable name for UDF WITH options.
const UDF_OPTIONS_GLOBAL: &str = "RW_UDF_OPTIONS";

pub(super) fn prepend_python_udf_options<'a>(
    body: &'a str,
    options: &BTreeMap<String, String>,
) -> Result<Cow<'a, str>> {
    if options.is_empty() {
        return Ok(Cow::Borrowed(body));
    }
    let options_json = serde_json::to_string(options)?;
    let options_json_literal = serde_json::to_string(&options_json)?;
    let prelude = format!(
        "import json as __rw_json\nfrom types import MappingProxyType as __rw_mapping_proxy_type\n{UDF_OPTIONS_GLOBAL} = __rw_mapping_proxy_type(__rw_json.loads({options_json_literal}))\n"
    );
    Ok(Cow::Owned(format!("{prelude}{body}")))
}

pub(super) fn prepend_javascript_udf_options<'a>(
    body: &'a str,
    options: &BTreeMap<String, String>,
) -> Result<Cow<'a, str>> {
    if options.is_empty() {
        return Ok(Cow::Borrowed(body));
    }
    let options_json = serde_json::to_string(options)?;
    let options_json_literal = serde_json::to_string(&options_json)?;
    let prelude = format!(
        "globalThis.{UDF_OPTIONS_GLOBAL} = Object.freeze(JSON.parse({options_json_literal}));\n"
    );
    Ok(Cow::Owned(format!("{prelude}{body}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_prelude_not_injected_for_empty_options() {
        let body = "def f(x):\n    return x\n";
        let rendered = prepend_python_udf_options(body, &BTreeMap::new()).unwrap();
        assert!(matches!(rendered, Cow::Borrowed(_)));
        assert_eq!(rendered, body);
    }

    #[test]
    fn javascript_prelude_not_injected_for_empty_options() {
        let body = "export function f(x) { return x; }\n";
        let rendered = prepend_javascript_udf_options(body, &BTreeMap::new()).unwrap();
        assert!(matches!(rendered, Cow::Borrowed(_)));
        assert_eq!(rendered, body);
    }

    #[test]
    fn python_prelude_contains_runtime_options_global() {
        let options = BTreeMap::from([("api_token".to_owned(), "token_a".to_owned())]);
        let rendered = prepend_python_udf_options("def f():\n    return 1\n", &options).unwrap();
        assert!(rendered.contains("RW_UDF_OPTIONS"));
        assert!(rendered.contains("MappingProxyType"));
    }

    #[test]
    fn javascript_prelude_contains_runtime_options_global() {
        let options = BTreeMap::from([("api_token".to_owned(), "token_a".to_owned())]);
        let rendered =
            prepend_javascript_udf_options("export function f() { return 1; }\n", &options)
                .unwrap();
        assert!(rendered.contains("RW_UDF_OPTIONS"));
        assert!(rendered.contains("Object.freeze"));
    }
}
