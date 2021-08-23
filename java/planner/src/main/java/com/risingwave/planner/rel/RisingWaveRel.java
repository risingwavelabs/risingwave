package com.risingwave.planner.rel;

import org.apache.calcite.rel.RelNode;

public interface RisingWaveRel extends RelNode {

  /**
   * Verify calling convention of plan node.
   *
   * <p><b>NOTICE:</b> If called in constructor, it should be called as last statement, or at least
   * after assignment of trait set.
   */
  void checkConvention();
}
