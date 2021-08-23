package com.risingwave.planner.planner;

import static com.risingwave.common.config.LeaderServerConfigurations.CLUSTER_MODE;

import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.context.ExecutionContext;

public class PlannerUtils {
  public static boolean isSingleMode(ExecutionContext context) {
    return context.getConf().get(CLUSTER_MODE) == LeaderServerConfigurations.ClusterMode.Single;
  }
}
