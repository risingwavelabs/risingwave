// Copyright 2024 RisingWave Labs
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

/// Extension trait to deserialize from a `&HashMap` or `&BTreeMap` of string-like
/// keys and values.
#[easy_ext::ext(DeserializeFromMap)]
impl<T> T
where
    T: serde::de::DeserializeOwned,
{
    /// Deserialize from a `&HashMap` or `&BTreeMap` of string-like keys and values.
    pub fn deserialize_from_map<'a, M, K, V>(map: &'a M) -> Result<Self, serde::de::value::Error>
    where
        &'a M: IntoIterator<Item = (&'a K, &'a V)>,
        K: AsRef<str> + 'a,
        V: AsRef<str> + 'a,
    {
        Self::deserialize(serde::de::value::MapDeserializer::<
            _,
            serde::de::value::Error,
        >::new(
            map.into_iter().map(|(k, v)| (k.as_ref(), v.as_ref()))
        ))
    }
}
