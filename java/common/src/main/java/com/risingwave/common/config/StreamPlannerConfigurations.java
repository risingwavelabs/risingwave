package com.risingwave.common.config;

import static com.risingwave.common.config.Parsers.BOOLEAN_PARSER;

/** Manage all config entry that can be set up in stream planner. */
public class StreamPlannerConfigurations {
  private StreamPlannerConfigurations() {}

  /**
   * The key of config entry should be set exactly the same as the parameter explicitly decalred in
   * SQL. For example, SET enable_new_subquery_stream_planner to TRUE => the key of
   * `ENABLE_NEW_SUBQUERY_PLANNER` config should be set to 'enable_new_subquery_stream_planner'.
   */
  @Config
  public static final ConfigEntry<Boolean> ENABLE_NEW_SUBQUERY_PLANNER =
      ConfigEntry.<Boolean>builder("enable_new_subquery_stream_planner")
          .setOptional(true)
          .withDefaultValue(false)
          .withConverter(BOOLEAN_PARSER)
          .withDoc("Optimizer config to enable new subquery expand in stream planner")
          .build();
}
