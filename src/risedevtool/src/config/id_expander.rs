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

use anyhow::{anyhow, Result};
use regex::Regex;
use yaml_rust::Yaml;

/// Expands `x-*` to `x-2333` by finding id in map.
pub struct IdExpander {
    ids: Vec<String>,
}

impl IdExpander {
    pub fn new(yaml: &Yaml) -> Result<Self> {
        let yaml = yaml
            .as_vec()
            .ok_or_else(|| anyhow!("Not an array: {:?}", yaml))?;
        let mut ids = vec![];
        for item in yaml {
            if let Some(item) = item.as_hash() {
                let id = item
                    .get(&Yaml::String("id".to_string()))
                    .ok_or_else(|| anyhow!("Missing id field: {:?}", item))?
                    .as_str()
                    .ok_or_else(|| anyhow!("Id isn't a string: {:?}", item))?;
                ids.push(id.to_string());
            } else {
                return Err(anyhow!("Not an hashmap: {:?}", item));
            }
        }
        Ok(Self { ids })
    }

    pub fn visit_vec(&mut self, mut yv: Vec<Yaml>) -> Result<Vec<Yaml>> {
        for y in &mut yv {
            *y = self.visit(y.clone())?;
        }
        Ok(yv)
    }

    pub fn visit(&mut self, y: Yaml) -> Result<Yaml> {
        match y {
            Yaml::Hash(y) => {
                let mut ny = y;
                for (_, v) in &mut ny {
                    let result = if let Some(v) = v.as_str() {
                        if let Some((before, after)) = v.split_once('*') {
                            let regex = Regex::new(&format!("^{}(.*){}$", before, after))?;
                            let mut matched_ids = vec![];
                            for id in &self.ids {
                                if regex.is_match(id) {
                                    matched_ids.push(Yaml::String(id.clone()));
                                }
                            }
                            Yaml::Array(matched_ids)
                        } else {
                            Yaml::String(v.to_string())
                        }
                    } else {
                        self.visit(v.clone())?
                    };
                    *v = result;
                }
                Ok(Yaml::Hash(ny))
            }
            Yaml::Array(y) => Ok(Yaml::Array(self.visit_vec(y)?)),
            other => Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust::YamlLoader;

    use super::*;

    #[test]
    fn test_expand_id() {
        let yaml = YamlLoader::load_from_str(
            "
- id: b-233
  provide: \"a-*\"
- id: a-233
    ",
        )
        .unwrap();
        let yaml_result = YamlLoader::load_from_str(
            "
- id: b-233
  provide: [\"a-233\"]
- id: a-233
      ",
        )
        .unwrap();
        let mut visitor = IdExpander::new(&yaml[0]).unwrap();

        assert_eq!(visitor.visit_vec(yaml).unwrap(), yaml_result);
    }

    #[test]
    fn test_expand_id_array() {
        let yaml = YamlLoader::load_from_str(
            "
- id: b-233
  provide: \"a-*\"
- id: a-233
- id: a-234
- id: aa-233
    ",
        )
        .unwrap()
        .remove(0);
        let yaml_result = YamlLoader::load_from_str(
            "
- id: b-233
  provide: [\"a-233\", \"a-234\"]
- id: a-233
- id: a-234
- id: aa-233
      ",
        )
        .unwrap()
        .remove(0);
        let mut visitor = IdExpander::new(&yaml).unwrap();

        assert_eq!(visitor.visit(yaml).unwrap(), yaml_result);
    }
}
