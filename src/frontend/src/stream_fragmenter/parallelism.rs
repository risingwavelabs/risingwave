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

use std::num::NonZeroU64;

use risingwave_common::session_config::parallelism::{
    ConfigBackfillParallelism, ConfigParallelism,
};
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;

use super::GraphJobType;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedParallelism {
    pub parallelism: Option<Parallelism>,
    pub adaptive_strategy: Option<AdaptiveParallelismStrategy>,
}

fn resolve_global_parallelism(
    global_streaming_parallelism: ConfigParallelism,
) -> ConfigParallelism {
    match global_streaming_parallelism {
        ConfigParallelism::Default => ConfigParallelism::Bounded(NonZeroU64::new(64).unwrap()),
        other => other,
    }
}

fn resolve_default_parallelism(
    job_type: Option<GraphJobType>,
    global_streaming_parallelism: ConfigParallelism,
) -> ConfigParallelism {
    let resolved_global_parallelism = resolve_global_parallelism(global_streaming_parallelism);

    match job_type {
        Some(GraphJobType::Table | GraphJobType::Source) => {
            if matches!(resolved_global_parallelism, ConfigParallelism::Fixed(_)) {
                resolved_global_parallelism
            } else {
                ConfigParallelism::Bounded(NonZeroU64::new(4).unwrap())
            }
        }
        Some(GraphJobType::MaterializedView | GraphJobType::Sink | GraphJobType::Index) | None => {
            resolved_global_parallelism
        }
    }
}

pub(crate) fn derive_parallelism(
    job_type: Option<GraphJobType>,
    specific_type_parallelism: Option<ConfigParallelism>,
    global_streaming_parallelism: ConfigParallelism,
) -> ResolvedParallelism {
    let effective_parallelism =
        match specific_type_parallelism.unwrap_or(ConfigParallelism::Default) {
            ConfigParallelism::Default => {
                resolve_default_parallelism(job_type, global_streaming_parallelism)
            }
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
) -> ResolvedParallelism {
    match specific_backfill_parallelism {
        ConfigBackfillParallelism::Default => ResolvedParallelism {
            parallelism: None,
            adaptive_strategy: None,
        },
        ConfigBackfillParallelism::Fixed(n) => ResolvedParallelism {
            parallelism: Some(Parallelism {
                parallelism: n.get(),
            }),
            adaptive_strategy: None,
        },
        ConfigBackfillParallelism::Adaptive
        | ConfigBackfillParallelism::Bounded(_)
        | ConfigBackfillParallelism::Ratio(_) => ResolvedParallelism {
            parallelism: None,
            adaptive_strategy: specific_backfill_parallelism.adaptive_strategy(),
        },
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
            derive_parallelism(None, None, global)
                .parallelism
                .map(|p| p.parallelism),
            Some(4)
        );
    }

    #[test]
    fn test_none_global_default() {
        let global = ConfigParallelism::Default;
        assert_eq!(derive_parallelism(None, None, global).parallelism, None);
        assert_eq!(
            derive_parallelism(None, None, global).adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(64).unwrap()
            ))
        );
    }

    #[test]
    fn test_none_global_adaptive() {
        let global = ConfigParallelism::Adaptive;
        assert_eq!(derive_parallelism(None, None, global).parallelism, None);
        assert_eq!(
            derive_parallelism(None, None, global).adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Auto)
        );
    }

    #[test]
    fn test_default_global_fixed() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Fixed(NonZeroU64::new(2).unwrap());
        assert_eq!(
            derive_parallelism(None, specific, global)
                .parallelism
                .map(|p| p.parallelism),
            Some(2)
        );
    }

    #[test]
    fn test_default_global_default() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Default;
        assert_eq!(derive_parallelism(None, specific, global).parallelism, None);
        assert_eq!(
            derive_parallelism(None, specific, global).adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(64).unwrap()
            ))
        );
    }

    #[test]
    fn test_default_global_adaptive() {
        let specific = Some(ConfigParallelism::Default);
        let global = ConfigParallelism::Adaptive;
        assert_eq!(derive_parallelism(None, specific, global).parallelism, None);
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
            assert_eq!(derive_parallelism(None, specific, global).parallelism, None);
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
                derive_parallelism(None, specific, global)
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
                None,
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
                None,
                Some(ConfigParallelism::Ratio(0.5)),
                ConfigParallelism::Adaptive
            )
            .adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Ratio(0.5))
        );
    }

    #[test]
    fn test_table_default_resolves_to_legacy_bound() {
        assert_eq!(
            derive_parallelism(
                Some(GraphJobType::Table),
                Some(ConfigParallelism::Default),
                ConfigParallelism::Default
            )
            .adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(4).unwrap()
            ))
        );
        assert_eq!(
            derive_parallelism(
                Some(GraphJobType::Table),
                Some(ConfigParallelism::Default),
                ConfigParallelism::Ratio(0.5)
            )
            .adaptive_strategy,
            Some(AdaptiveParallelismStrategy::Bounded(
                NonZeroUsize::new(4).unwrap()
            ))
        );
    }

    #[test]
    fn test_source_default_follows_fixed_global() {
        assert_eq!(
            derive_parallelism(
                Some(GraphJobType::Source),
                Some(ConfigParallelism::Default),
                ConfigParallelism::Fixed(NonZeroU64::new(7).unwrap())
            )
            .parallelism
            .map(|p| p.parallelism),
            Some(7)
        );
    }

    #[test]
    fn test_backfill_parallelism_adaptive_resolves_strategy() {
        assert_eq!(
            derive_backfill_parallelism(ConfigBackfillParallelism::Adaptive),
            ResolvedParallelism {
                parallelism: None,
                adaptive_strategy: Some(AdaptiveParallelismStrategy::Auto),
            }
        );
    }

    #[test]
    fn test_backfill_parallelism_default_does_not_resolve_to_fixed() {
        assert_eq!(
            derive_backfill_parallelism(ConfigBackfillParallelism::Default),
            ResolvedParallelism {
                parallelism: None,
                adaptive_strategy: None,
            }
        );
    }

    #[test]
    fn test_backfill_parallelism_bounded_resolves_strategy() {
        assert_eq!(
            derive_backfill_parallelism(ConfigBackfillParallelism::Bounded(
                NonZeroU64::new(2).unwrap()
            )),
            ResolvedParallelism {
                parallelism: None,
                adaptive_strategy: Some(AdaptiveParallelismStrategy::Bounded(
                    NonZeroUsize::new(2).unwrap()
                )),
            }
        );
    }
}
