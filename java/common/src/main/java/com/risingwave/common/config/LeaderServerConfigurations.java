package com.risingwave.common.config;

import static com.risingwave.common.config.Parsers.ADDRESSES_PARSER;

import java.util.List;

public class LeaderServerConfigurations {
  private LeaderServerConfigurations() {}

  @Config
  public static final ConfigEntry<List<String>> COMPUTE_NODES =
      ConfigEntry.<List<String>>builder("risingwave.leader.computenodes")
          .setOptional(false)
          .withDoc("Compute node addresses")
          .withConverter(ADDRESSES_PARSER)
          .build();
}
