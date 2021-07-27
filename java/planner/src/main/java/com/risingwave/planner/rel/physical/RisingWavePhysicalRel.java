package com.risingwave.planner.rel.physical;

import com.risingwave.planner.rel.RisingWaveRel;
import com.risingwave.proto.plan.PlanNode;

public interface RisingWavePhysicalRel extends RisingWaveRel {
  PlanNode serialize();
}
