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

use std::hash::Hash;
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub(crate) struct Refer(Arc<()>);

impl Refer {
    pub(crate) fn new() -> Self {
        Self(Arc::new(()))
    }

    pub(crate) fn refer(&self) -> ReferWeak {
        ReferWeak(Arc::downgrade(&self.0))
    }

    pub(crate) fn can_delete(&self) -> bool {
        Arc::weak_count(&self.0) == 0
    }

    pub(crate) fn used_count(&self) -> usize {
        Arc::weak_count(&self.0)
    }
}

#[derive(Clone)]
pub(crate) struct ReferWeak(pub(crate) Weak<()>);

impl Hash for ReferWeak {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Weak::as_ptr(&self.0).hash(state);
    }
}

impl PartialEq<Self> for ReferWeak {
    fn eq(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ReferWeak {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;

    use crate::manager::catalog::reference_checker::{Refer, ReferWeak};

    #[test]
    fn test_refer_count() {
        struct MockItem {
            refer: Refer,
            dependencies: Vec<ReferWeak>,
        }
        impl MockItem {
            fn new(dependencies: impl IntoIterator<Item = ReferWeak>) -> Self {
                Self {
                    refer: Refer::new(),
                    dependencies: dependencies.into_iter().collect_vec(),
                }
            }
        }

        let mut items = HashMap::new();
        for id in 0..10 {
            let item = MockItem::new(vec![].into_iter());
            items.insert(id, item);
        }
        let item = MockItem::new((0..10).map(|i| items[&i].refer.refer()));
        assert_eq!(item.dependencies.len(), 10);
        assert!(items.values().all(|t| t.refer.used_count() == 1));
        drop(item);
        assert!(items.values().all(|t| t.refer.can_delete()));
    }
}
