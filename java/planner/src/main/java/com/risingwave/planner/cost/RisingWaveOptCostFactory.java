package com.risingwave.planner.cost;

import org.apache.calcite.plan.RelOptCostFactory;

public interface RisingWaveOptCostFactory extends RelOptCostFactory {
  RisingWaveOptCost makeCost(double rowCount, double cpu, double memory, double io, double network);
}
