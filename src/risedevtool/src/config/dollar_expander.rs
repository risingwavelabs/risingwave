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
use regex::Regex;
use yaml_rust::Yaml;

/// Expands `x-${port}` to `x-2333`.
pub struct DollarExpander {
    re: Regex,
    extra_info: HashMap<String, String>,
}

fn yaml_to_string(y: &Yaml) -> Result<String> {
    match y {
        Yaml::Integer(x) => Ok(format!("{}", x)),
        Yaml::String(x) => Ok(x.clone()),
        _ => Err(anyhow!("{:?} cannot be converted to string", y)),
    }
}

impl DollarExpander {
    pub fn new(extra_info: HashMap<String, String>) -> Self {
        Self {
            re: Regex::new(r"\$\{(.*?)\}").unwrap(),
            extra_info,
        }
    }

    pub fn visit(&mut self, y: Yaml) -> Result<Yaml> {
        match y {
            Yaml::Hash(y) => {
                let mut ny = y.clone();
                for (_, v) in &mut ny {
                    let result = if let Some(v) = v.as_str() {
                        let mut target = String::new();
                        let mut last_location = 0;
                        for cap in self.re.captures_iter(v) {
                            let cap = cap.get(1).unwrap();
                            let name = cap.as_str();
                            let value = if let Some(item) = y.get(&Yaml::String(name.to_string())) {
                                yaml_to_string(item)?
                            } else if let Some(item) = self.extra_info.get(name) {
                                item.clone()
                            } else {
                                return Err(anyhow!("{} not found in {:?}", name, y));
                            };
                            target += &v[last_location..(cap.start() - 2)]; // ignore `${`
                            target += &value;
                            last_location = cap.end() + 1; // ignore `}`
                        }
                        target += &v[last_location..v.len()];
                        Yaml::String(target)
                    } else {
                        self.visit(v.clone())?
                    };
                    *v = result;
                }
                Ok(Yaml::Hash(ny))
            }
            Yaml::Array(mut yv) => {
                for y in &mut yv {
                    *y = self.visit(y.clone())?;
                }
                Ok(Yaml::Array(yv))
            }
            other => Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use yaml_rust::YamlLoader;

    use super::*;

    #[test]
    fn test_expand_dollar() {
        let yaml = YamlLoader::load_from_str(
            "
a:
  b:
    c:
      d: \"${x}${y},${test:key}\"
      x: 2333
      y: 2334
    ",
        )
        .unwrap()
        .remove(0);
        let yaml_result = YamlLoader::load_from_str(
            "
  a:
    b:
      c:
        d: \"23332334,value\"
        x: 2333
        y: 2334
      ",
        )
        .unwrap()
        .remove(0);
        let mut visitor = DollarExpander::new(
            vec![("test:key".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        );

        assert_eq!(visitor.visit(yaml).unwrap(), yaml_result);
    }
}
