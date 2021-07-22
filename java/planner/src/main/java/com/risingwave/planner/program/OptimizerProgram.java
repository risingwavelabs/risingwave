package com.risingwave.planner.program;

import com.risingwave.planner.planner.PlannerContext;
import org.apache.calcite.rel.RelNode;

/** An optimizer program is used to group a collection of op */
public interface OptimizerProgram {
  RelNode optimize(RelNode root, PlannerContext context);
}
