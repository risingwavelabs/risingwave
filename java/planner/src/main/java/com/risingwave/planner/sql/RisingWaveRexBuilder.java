package com.risingwave.planner.sql;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

/**
 * Overrides the default RexBuilder to change part of the default behaviors.
 *
 * <p>See the overrided methods for more details.
 */
public class RisingWaveRexBuilder extends RexBuilder {
  /**
   * Creates a RexBuilder.
   *
   * @param typeFactory Type factory
   */
  public RisingWaveRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }
}
