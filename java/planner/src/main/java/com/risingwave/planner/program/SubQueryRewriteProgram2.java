package com.risingwave.planner.program;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rules.physical.BatchRuleSets;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** New subquery program. */
public class SubQueryRewriteProgram2 implements OptimizerProgram {
  private static final Logger LOG = LoggerFactory.getLogger(SubQueryRewriteProgram2.class);

  private static final HepProgram PROGRAM = create();

  public SubQueryRewriteProgram2() {}

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    var planner = new HepPlanner(PROGRAM, context);
    planner.setRoot(root);

    var ret = planner.findBestExp();

    LOG.debug("Plan after preparing new subquery rewrite: \n{}", ExplainWriter.explainPlan(ret));

    return ret;
  }

  private static HepProgram create() {
    var builder = HepProgram.builder().addMatchOrder(HepMatchOrder.BOTTOM_UP);

    BatchRuleSets.NEW_SUB_QUERY_RULES.forEach(builder::addRuleInstance);

    return builder.build();
  }
}
