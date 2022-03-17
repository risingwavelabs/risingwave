package com.risingwave.common.config;

import static com.risingwave.common.config.Parsers.BOOLEAN_PARSER;

/** Manage all config entry that can be set up in batch planner. */
public class BatchPlannerConfigurations {
  private BatchPlannerConfigurations() {}

  /**
   * The key of config entry should be set exactly the same as the parameter explicitly decalred in
   * SQL. For example, SET enable_hashagg to TRUE => the key of `ENABLE_HASH_AGG` config should be
   * set to 'enable_hashagg'.
   */
  @Config
  public static final ConfigEntry<Boolean> ENABLE_HASH_AGG =
      ConfigEntry.<Boolean>builder("enable_hashagg")
          .setOptional(true)
          .withConverter(Parsers.BOOLEAN_PARSER)
          .withDefaultValue(true)
          .withDoc("Enable hash agg for batch execution.")
          .build();

  @Config
  public static final ConfigEntry<Boolean> ENABLE_SORT_AGG =
      ConfigEntry.<Boolean>builder("enable_sortagg")
          .setOptional(true)
          .withConverter(Parsers.BOOLEAN_PARSER)
          .withDefaultValue(true)
          .withDoc("Enable sort agg for batch execution.")
          .build();

  @Config
  public static final ConfigEntry<Boolean> ENABLE_HASH_JOIN =
      ConfigEntry.<Boolean>builder("enable_hashjoin")
          .setOptional(true)
          .withConverter(Parsers.BOOLEAN_PARSER)
          .withDefaultValue(true)
          .withDoc("Enable hash join for batch execution.")
          .build();

  @Config
  public static final ConfigEntry<Boolean> ENABLE_SORT_MERGE_JOIN =
      ConfigEntry.<Boolean>builder("enable_mergejoin")
          .setOptional(true)
          .withConverter(Parsers.BOOLEAN_PARSER)
          .withDefaultValue(true)
          .withDoc("Enable sort agg for batch execution.")
          .build();

  @Config
  public static final ConfigEntry<Boolean> ENABLE_NEW_SUBQUERY_PLANNER =
      ConfigEntry.<Boolean>builder("enable_new_subquery_batch_planner")
          .setOptional(true)
          .withDefaultValue(false)
          .withConverter(BOOLEAN_PARSER)
          .withDoc("Optimizer config to enable new subquery expand")
          .build();
}
