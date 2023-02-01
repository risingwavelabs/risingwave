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

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use itertools::Itertools;
use yaml_rust::Yaml;

/// Expands `provide-xxx: ["a", "b", "c"]` to their corresponding IDs.
pub struct ProvideExpander {
    all_items: HashMap<String, Yaml>,
}

impl ProvideExpander {
    pub fn new(y: &Yaml) -> Result<Self> {
        let y = y.as_vec().ok_or_else(|| anyhow!("expect an array"))?;
        let mut all_items = HashMap::new();
        for v in y {
            let v = v.as_hash().ok_or_else(|| anyhow!("expect a hashmap"))?;
            let id = v
                .get(&Yaml::String("id".into()))
                .ok_or_else(|| anyhow!("missing id field"))?;
            let id = id
                .as_str()
                .ok_or_else(|| anyhow!("expect id to be a string"))?;
            all_items.insert(id.to_string(), Yaml::Hash(v.clone()));
        }
        Ok(Self {
            all_items: Self::remove_provide(all_items)?,
        })
    }

    fn remove_provide(all_items: HashMap<String, Yaml>) -> Result<HashMap<String, Yaml>> {
        let all_items = all_items.into_iter().map(|(k, v)| {
            let v = v.into_hash().ok_or_else(|| anyhow!("expect a hashmap"))?;
            let v = v
                .into_iter()
                .filter(|(k, _)| {
                    if let Some(k) = k.as_str() {
                        !k.starts_with("provide-")
                    } else {
                        true
                    }
                })
                .collect();
            Ok::<_, anyhow::Error>((k, Yaml::Hash(v)))
        });
        all_items.try_collect()
    }

    pub fn visit(&mut self, yaml: Yaml) -> Result<Yaml> {
        let yaml = yaml
            .into_vec()
            .ok_or_else(|| anyhow!("expect an array"))?
            .into_iter()
            .map(|yaml| {
                let map = yaml
                    .into_hash()
                    .ok_or_else(|| anyhow!("expect a hashmap"))?;
                let map = map.into_iter().map(|(k, v)| {
                    if let Some(k) = k.as_str() && k.starts_with("provide-") {
                        let array = v
                            .as_vec()
                            .ok_or_else(|| anyhow!("expect an array of provide-"))?;
                        let array = array.iter().map(|item| {
                            let item = item
                                .as_str()
                                .ok_or_else(|| anyhow!("expect a string from provide"))?;
                            Ok::<_, anyhow::Error>(
                                self.all_items
                                    .get(item)
                                    .ok_or_else(|| anyhow!("{} not found", item))?
                                    .clone(),
                            )
                        });
                        return Ok::<_, anyhow::Error>((
                            Yaml::String(k.to_string()),
                            Yaml::Array(array.try_collect()?),
                        ));
                    }
                    Ok::<_, anyhow::Error>((k, v))
                });
                Ok::<_, anyhow::Error>(Yaml::Hash(map.try_collect()?))
            });
        Ok(Yaml::Array(yaml.try_collect()?))
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust::YamlLoader;

    use super::*;

    #[test]
    fn test_expand_provide() {
        let source = YamlLoader::load_from_str(
            "
- provide-b: [\"b\"]
  test_field: a
  id: a
- provide-a: [\"a\"]
  test_field: a
  id: b
      ",
        )
        .unwrap()
        .remove(0);

        let expected_result = YamlLoader::load_from_str(
            "
- provide-b:
    - test_field: a
      id: b
  test_field: a
  id: a
- provide-a:
    - test_field: a
      id: a
  test_field: a
  id: b
  ",
        )
        .unwrap()
        .remove(0);

        let mut visitor = ProvideExpander::new(&source).unwrap();

        assert_eq!(visitor.visit(source).unwrap(), expected_result);
    }
}
