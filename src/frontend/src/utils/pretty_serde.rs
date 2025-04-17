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

//! @kwannoel:
//! This module implements Serde for the Pretty struct. Why not implement it directly on our plan nodes?
//! That's because Pretty already summarizes the fields that are important to us.
//! You can see that when `explain()` is called, we directly return the `Pretty` struct.
//! The _proper way_ to do this would be to create a new data structure that plan nodes get converted into,
//! and then implement `Serialize` and `Deserialize` on that data structure (including to `Pretty`).
//! But that's a lot of refactoring work.
//! So we just wrap `Pretty` in a newtype and implement `Serialize` on that,
//! since it's a good enough intermediate representation.

use std::collections::BTreeMap;

use pretty_xmlish::Pretty;
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Serialize, Serializer};

// Second anymous field is include_children.
// If true the children information will be serialized.
pub struct PrettySerde<'a>(pub Pretty<'a>, pub bool);

impl Serialize for PrettySerde<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.0 {
            Pretty::Text(text) => serializer.serialize_str(text.as_ref()),

            Pretty::Record(node) => {
                let mut state = serializer.serialize_struct("XmlNode", 3)?;
                state.serialize_field("name", node.name.as_ref())?;
                state.serialize_field(
                    "fields",
                    &node
                        .fields
                        .iter()
                        .map(|(k, v)| (k.as_ref(), PrettySerde(v.clone(), self.1)))
                        .collect::<BTreeMap<_, _>>(),
                )?;
                if self.1 {
                    state.serialize_field(
                        "children",
                        &node
                            .children
                            .iter()
                            .map(|c| PrettySerde(c.clone(), self.1))
                            .collect::<Vec<_>>(),
                    )?;
                }
                state.end()
            }

            Pretty::Array(elements) => {
                let mut seq = serializer.serialize_seq(Some(elements.len()))?;
                for element in elements {
                    seq.serialize_element(&PrettySerde((*element).clone(), self.1))?;
                }
                seq.end()
            }

            Pretty::Linearized(inner, size) => {
                let mut state = serializer.serialize_struct("Linearized", 2)?;
                state.serialize_field("inner", &PrettySerde((**inner).clone(), self.1))?;
                state.serialize_field("size", size)?;
                state.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use expect_test::{Expect, expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_pretty_serde() {
        let pretty = Pretty::childless_record("root", vec![("a", Pretty::Text("1".into()))]);
        let pretty_serde = PrettySerde(pretty, true);
        let serialized = serde_json::to_string(&pretty_serde).unwrap();
        check(
            serialized,
            expect![[r#""{\"name\":\"root\",\"fields\":{\"a\":\"1\"},\"children\":[]}""#]],
        );
    }
}
