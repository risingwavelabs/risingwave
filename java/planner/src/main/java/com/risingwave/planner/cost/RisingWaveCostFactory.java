package com.risingwave.planner.cost;

import org.apache.calcite.plan.RelOptCost;

public class RisingWaveCostFactory implements RisingWaveOptCostFactory {

  static final RisingWaveCost INFINITY =
      new RisingWaveCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY);

  private static final RisingWaveCost HUGE =
      new RisingWaveCost(
          Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);

  private static final RisingWaveCost ZERO = new RisingWaveCost(0, 0, 0, 0, 0);

  private static final RisingWaveCost TINY = new RisingWaveCost(1, 1, 0, 0, 0);

  @Override
  public RisingWaveOptCost makeCost(
      double rowCount, double cpu, double memory, double io, double network) {
    return new RisingWaveCost(rowCount, cpu, io, network, memory);
  }

  @Override
  public RelOptCost makeCost(double rowCount, double cpu, double io) {
    return makeCost(rowCount, cpu, io, 0, 0);
  }

  @Override
  public RelOptCost makeHugeCost() {
    return HUGE;
  }

  @Override
  public RelOptCost makeInfiniteCost() {
    return INFINITY;
  }

  @Override
  public RelOptCost makeTinyCost() {
    return TINY;
  }

  @Override
  public RelOptCost makeZeroCost() {
    return ZERO;
  }
}
