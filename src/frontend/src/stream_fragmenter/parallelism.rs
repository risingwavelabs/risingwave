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

use risingwave_common::session_config::parallelism::ConfigParallelism;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;

pub(crate) fn derive_parallelism(
    specific_type_parallelism: Option<ConfigParallelism>,
    global_streaming_parallelism: ConfigParallelism,
) -> Option<Parallelism> {
    match specific_type_parallelism {
        // fallback to global streaming_parallelism
        Some(ConfigParallelism::Default) | None => match global_streaming_parallelism {
            // for streaming_parallelism, `Default` is `Adaptive`
            ConfigParallelism::Default | ConfigParallelism::Adaptive => None,
            ConfigParallelism::Fixed(n) => Some(Parallelism {
                parallelism: n.get(),
            }),
        },

        // specific type parallelism is set to `Adaptive` or `Fixed(0)`
        Some(ConfigParallelism::Adaptive) => None,

        // specific type parallelism is set to `Fixed(n)
        Some(ConfigParallelism::Fixed(n)) => Some(Parallelism {
            parallelism: n.get(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    #[test]
    fn test_none_global_fixed() {
        let global = ConfigParallelism::Fixed(NonZeroU64::new(4).unwrap());
        assert_eq!(
            derive_parallelism(None, global).map(|p| p.parallelism),
            Some(4)
        );
    }

    #[test]
    fn test_none_global_default() {
        let global = ConfigParallelism::Default;
        assert_eq!(derive_parallelism(None, global), None);
    }

    #[test]
    fn test_none_global_adaptive() {
        let global = ConfigParallelism::Adaptive;
        assert_eq!(derive_parallelism(None, global), None);
    }

    #[test]
    fn test_default_global_fixed() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Fixed(NonZeroU64::new(2).unwrap());
        assert_eq!(
            derive_parallelism(specific, global).map(|p| p.parallelism),
            Some(2)
        );
    }

    #[test]
    fn test_default_global_default() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Default;
        assert_eq!(derive_parallelism(specific, global), None);
    }

    #[test]
    fn test_default_global_adaptive() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Adaptive;
        assert_eq!(derive_parallelism(specific, global), None);
    }

    #[test]
    fn test_adaptive_any_global() {
        let specific = Some(ConfigParallelism::Adaptive);
        let globals = [
            ConfigParallelism::Default,
            ConfigParallelism::Adaptive,
            ConfigParallelism::Fixed(NonZeroU64::new(8).unwrap()),
        ];

        for global in globals {
            assert_eq!(derive_parallelism(specific, global), None);
        }
    }

    #[test]
    fn test_fixed_override_global() {
        let specific = Some(ConfigParallelism::Fixed(NonZeroU64::new(6).unwrap()));
        let globals = [
            ConfigParallelism::Default,
            ConfigParallelism::Adaptive,
            ConfigParallelism::Fixed(NonZeroU64::new(3).unwrap()),
        ];

        for global in globals {
            assert_eq!(
                derive_parallelism(specific, global).map(|p| p.parallelism),
                Some(6)
            );
        }
    }
}
