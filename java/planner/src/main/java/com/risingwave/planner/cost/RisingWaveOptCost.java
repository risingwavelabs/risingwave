package com.risingwave.planner.cost;

import org.apache.calcite.plan.RelOptCost;

public interface RisingWaveOptCost extends RelOptCost {

  double getNetwork();

  double getMemory();
}
