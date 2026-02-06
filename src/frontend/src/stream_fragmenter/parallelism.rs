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

use std::num::NonZeroUsize;

use risingwave_common::session_config::parallelism::{
    ConfigAdaptiveParallelismStrategy, ConfigParallelism,
};
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;

use super::GraphJobType;

/// Default parallelism bound for tables
const DEFAULT_TABLE_PARALLELISM_BOUND: usize = 4;

/// Default parallelism bound for sources
const DEFAULT_SOURCE_PARALLELISM_BOUND: usize = 4;

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

pub(crate) fn derive_parallelism_strategy(
    specific_strategy: Option<ConfigAdaptiveParallelismStrategy>,
    global_strategy: ConfigAdaptiveParallelismStrategy,
    job_type: Option<&GraphJobType>,
) -> Option<AdaptiveParallelismStrategy> {
    let to_strategy =
        |cfg: ConfigAdaptiveParallelismStrategy| -> Option<AdaptiveParallelismStrategy> {
            cfg.into()
        };

    match specific_strategy.unwrap_or(ConfigAdaptiveParallelismStrategy::Default) {
        ConfigAdaptiveParallelismStrategy::Default => {
            // For tables and sources, Default maps to BOUNDED(4) to limit resource usage.
            // For other job types, Default falls back to the global strategy.
            match job_type {
                Some(GraphJobType::Table) => Some(AdaptiveParallelismStrategy::Bounded(
                    NonZeroUsize::new(DEFAULT_TABLE_PARALLELISM_BOUND).unwrap(),
                )),
                Some(GraphJobType::Source) => Some(AdaptiveParallelismStrategy::Bounded(
                    NonZeroUsize::new(DEFAULT_SOURCE_PARALLELISM_BOUND).unwrap(),
                )),
                _ => to_strategy(global_strategy),
            }
        }
        other => to_strategy(other),
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

    #[test]
    fn test_parallelism_strategy_fallback() {
        assert_eq!(
            derive_parallelism_strategy(
                None,
                ConfigAdaptiveParallelismStrategy::Auto,
                Some(&GraphJobType::MaterializedView)
            ),
            Some(AdaptiveParallelismStrategy::Auto)
        );
        assert_eq!(
            derive_parallelism_strategy(
                Some(ConfigAdaptiveParallelismStrategy::Default),
                ConfigAdaptiveParallelismStrategy::Full,
                Some(&GraphJobType::Sink)
            ),
            Some(AdaptiveParallelismStrategy::Full)
        );
    }

    #[test]
    fn test_parallelism_strategy_override() {
        assert_eq!(
            derive_parallelism_strategy(
                Some(ConfigAdaptiveParallelismStrategy::Ratio(0.5)),
                ConfigAdaptiveParallelismStrategy::Full,
                Some(&GraphJobType::Table)
            ),
            Some(AdaptiveParallelismStrategy::Ratio(0.5))
        );
    }

    #[test]
    fn test_table_source_default_to_bounded() {
        // Tables default to BOUNDED(4)
        assert_eq!(
            derive_parallelism_strategy(
                Some(ConfigAdaptiveParallelismStrategy::Default),
                ConfigAdaptiveParallelismStrategy::Auto,
                Some(&GraphJobType::Table)
            ),
            Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(DEFAULT_TABLE_PARALLELISM_BOUND).unwrap()
            ))
        );
        // Sources default to BOUNDED(4)
        assert_eq!(
            derive_parallelism_strategy(
                Some(ConfigAdaptiveParallelismStrategy::Default),
                ConfigAdaptiveParallelismStrategy::Full,
                Some(&GraphJobType::Source)
            ),
            Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(DEFAULT_SOURCE_PARALLELISM_BOUND).unwrap()
            ))
        );
        // MVs fall back to global strategy
        assert_eq!(
            derive_parallelism_strategy(
                Some(ConfigAdaptiveParallelismStrategy::Default),
                ConfigAdaptiveParallelismStrategy::Auto,
                Some(&GraphJobType::MaterializedView)
            ),
            Some(AdaptiveParallelismStrategy::Auto)
        );
    }
}
