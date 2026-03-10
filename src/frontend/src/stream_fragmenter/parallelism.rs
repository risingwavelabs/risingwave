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

use risingwave_common::session_config::parallelism::{
    ConfigBackfillParallelism, ConfigParallelism,
};
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedParallelism {
    pub parallelism: Option<Parallelism>,
    pub adaptive_strategy: Option<AdaptiveParallelismStrategy>,
}

fn normalize_global_parallelism(
    global_streaming_parallelism: ConfigParallelism,
) -> ConfigParallelism {
    match global_streaming_parallelism {
        ConfigParallelism::Default => ConfigParallelism::Adaptive,
        other => other,
    }
}

pub(crate) fn derive_parallelism(
    specific_type_parallelism: Option<ConfigParallelism>,
    global_streaming_parallelism: ConfigParallelism,
) -> ResolvedParallelism {
    let effective_parallelism = match specific_type_parallelism
        .unwrap_or(ConfigParallelism::Default)
    {
        ConfigParallelism::Default => normalize_global_parallelism(global_streaming_parallelism),
        other => other,
    };

    match effective_parallelism {
        ConfigParallelism::Default => unreachable!("effective streaming parallelism must be set"),
        ConfigParallelism::Fixed(n) => ResolvedParallelism {
            parallelism: Some(Parallelism {
                parallelism: n.get(),
            }),
            adaptive_strategy: None,
        },
        ConfigParallelism::Adaptive
        | ConfigParallelism::Bounded(_)
        | ConfigParallelism::Ratio(_) => ResolvedParallelism {
            parallelism: None,
            adaptive_strategy: effective_parallelism.adaptive_strategy(),
        },
    }
}

pub(crate) fn derive_backfill_parallelism(
    specific_backfill_parallelism: ConfigBackfillParallelism,
    global_streaming_parallelism: ConfigParallelism,
) -> Option<Parallelism> {
    match specific_backfill_parallelism {
        ConfigBackfillParallelism::Default => None,
        ConfigBackfillParallelism::Fixed(n) => Some(Parallelism {
            parallelism: n.get(),
        }),
        ConfigBackfillParallelism::Adaptive => {
            match normalize_global_parallelism(global_streaming_parallelism) {
                ConfigParallelism::Fixed(n) => Some(Parallelism {
                    parallelism: n.get(),
                }),
                _ => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;

    #[test]
    fn test_none_global_fixed() {
        let global = ConfigParallelism::Fixed(NonZeroU64::new(4).unwrap());
        assert_eq!(
            derive_parallelism(None, global)
                .parallelism
                .map(|p| p.parallelism),
            Some(4)
        );
    }

    #[test]
    fn test_none_global_default() {
        let global = ConfigParallelism::Default;
        assert_eq!(derive_parallelism(None, global).parallelism, None);
        assert_eq!(
            derive_parallelism(None, global).adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Auto)
        );
    }

    #[test]
    fn test_none_global_adaptive() {
        let global = ConfigParallelism::Adaptive;
        assert_eq!(derive_parallelism(None, global).parallelism, None);
        assert_eq!(
            derive_parallelism(None, global).adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Auto)
        );
    }

    #[test]
    fn test_default_global_fixed() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Fixed(NonZeroU64::new(2).unwrap());
        assert_eq!(
            derive_parallelism(specific, global)
                .parallelism
                .map(|p| p.parallelism),
            Some(2)
        );
    }

    #[test]
    fn test_default_global_default() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Default;
        assert_eq!(derive_parallelism(specific, global).parallelism, None);
    }

    #[test]
    fn test_default_global_adaptive() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Adaptive;
        assert_eq!(derive_parallelism(specific, global).parallelism, None);
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
            assert_eq!(derive_parallelism(specific, global).parallelism, None);
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
                derive_parallelism(specific, global)
                    .parallelism
                    .map(|p| p.parallelism),
                Some(6)
            );
        }
    }

    #[test]
    fn test_bounded_parallelism_resolves_strategy() {
        assert_eq!(
            derive_parallelism(
                Some(ConfigParallelism::Bounded(NonZeroU64::new(4).unwrap())),
                ConfigParallelism::Adaptive
            )
            .adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(4).unwrap()
            ))
        );
    }

    #[test]
    fn test_ratio_parallelism_resolves_strategy() {
        assert_eq!(
            derive_parallelism(
                Some(ConfigParallelism::Ratio(0.5)),
                ConfigParallelism::Adaptive
            )
            .adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Ratio(0.5))
        );
    }

    #[test]
    fn test_backfill_parallelism_adaptive_uses_fixed_global() {
        assert_eq!(
            derive_backfill_parallelism(
                ConfigBackfillParallelism::Adaptive,
                ConfigParallelism::Fixed(NonZeroU64::new(2).unwrap())
            )
            .map(|p| p.parallelism),
            Some(2)
        );
    }

    #[test]
    fn test_backfill_parallelism_default_does_not_resolve_to_fixed() {
        assert_eq!(
            derive_backfill_parallelism(
                ConfigBackfillParallelism::Default,
                ConfigParallelism::Adaptive
            ),
            None
        );
    }
}
