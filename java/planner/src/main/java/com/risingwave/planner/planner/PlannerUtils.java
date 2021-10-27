package com.risingwave.planner.planner;

import static com.risingwave.common.config.LeaderServerConfigurations.CLUSTER_MODE;

import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.context.ExecutionContext;

/** Utilities functions for Planner */
public class PlannerUtils {
  public static boolean isSingleMode(ExecutionContext context) {
    return context.getConf().get(CLUSTER_MODE) == LeaderServerConfigurations.ClusterMode.Single;
  }

  public static boolean isDistributedMode(ExecutionContext context) {
    return context.getConf().get(CLUSTER_MODE)
        == LeaderServerConfigurations.ClusterMode.Distributed;
  }
}
