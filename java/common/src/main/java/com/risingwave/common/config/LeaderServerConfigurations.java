package com.risingwave.common.config;

import static com.risingwave.common.config.Parsers.ADDRESSES_PARSER;
import static com.risingwave.common.config.Parsers.enumParserOf;

import java.util.List;

public class LeaderServerConfigurations {
  private LeaderServerConfigurations() {}

  public enum ClusterMode {
    /** Single mean that we have only one worker node */
    Single,

    /** Cluster has at least one worker node. */
    Distributed
  }

  @Config
  public static final ConfigEntry<List<String>> COMPUTE_NODES =
      ConfigEntry.<List<String>>builder("risingwave.leader.computenodes")
          .setOptional(false)
          .withDoc("Compute node addresses")
          .withConverter(ADDRESSES_PARSER)
          .build();

  @Config
  public static final ConfigEntry<ClusterMode> CLUSTER_MODE =
      ConfigEntry.<ClusterMode>builder("risingwave.leader.clustermode")
          .setOptional(false)
          .withDefaultValue(ClusterMode.Single)
          .withDoc("Risingwave's cluster mode")
          .withConverter(enumParserOf(ClusterMode.class))
          .build();
}
