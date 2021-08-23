package com.risingwave.planner.rel.common.dist;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.ImmutableIntList;

public class RwDistributions {
  public static final ImmutableIntList EMPTY = ImmutableIntList.of();

  /** The singleton singleton distribution. */
  public static final RwDistributionTrait SINGLETON =
      new RwDistributionTrait(RelDistribution.Type.SINGLETON, EMPTY);

  /** The singleton random distribution. */
  public static final RwDistributionTrait RANDOM_DISTRIBUTED =
      new RwDistributionTrait(RelDistribution.Type.RANDOM_DISTRIBUTED, EMPTY);

  /** The singleton broadcast distribution. */
  public static final RwDistributionTrait BROADCAST_DISTRIBUTED =
      new RwDistributionTrait(RelDistribution.Type.BROADCAST_DISTRIBUTED, EMPTY);

  public static final RwDistributionTrait ANY =
      new RwDistributionTrait(RelDistribution.Type.ANY, EMPTY);

  private RwDistributions() {}
}
