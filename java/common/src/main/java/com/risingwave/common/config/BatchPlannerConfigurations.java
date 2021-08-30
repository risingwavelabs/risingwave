package com.risingwave.common.config;

public class BatchPlannerConfigurations {
  private BatchPlannerConfigurations() {}

  public static final ConfigEntry<Boolean> ENABLE_HASH_AGG =
      ConfigEntry.<Boolean>builder("risingwave.batch.planner.enableHashAgg")
          .setOptional(true)
          .withConverter(Parsers.BOOLEAN_PARSER)
          .withDefaultValue(true)
          .withDoc("Enable hash agg for batch execution.")
          .build();

  public static final ConfigEntry<Boolean> ENABLE_SORT_AGG =
      ConfigEntry.<Boolean>builder("risingwave.batch.planner.enableSortAgg")
          .setOptional(true)
          .withConverter(Parsers.BOOLEAN_PARSER)
          .withDefaultValue(true)
          .withDoc("Enable sort agg for batch execution.")
          .build();
}
