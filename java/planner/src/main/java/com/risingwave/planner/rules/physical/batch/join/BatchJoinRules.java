package com.risingwave.planner.rules.physical.batch.join;

import org.apache.calcite.rel.core.JoinInfo;

public class BatchJoinRules {
  private BatchJoinRules() {}

  public static boolean isEquiJoin(JoinInfo joinInfo) {
    return joinInfo.nonEquiConditions.isEmpty() && !joinInfo.leftKeys.isEmpty();
  }
}
