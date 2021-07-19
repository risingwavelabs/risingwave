package com.risingwave.planner.rel.physical;

import com.risingwave.planner.rel.RisingWaveRel;
import org.apache.calcite.rel.PhysicalNode;

public interface RisingWavePhyRel extends RisingWaveRel, PhysicalNode {}
