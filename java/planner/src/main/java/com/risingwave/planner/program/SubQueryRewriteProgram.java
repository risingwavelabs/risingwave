package com.risingwave.planner.program;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.rules.BatchRuleSets;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;

public class SubQueryRewriteProgram implements OptimizerProgram {
  public static final SubQueryRewriteProgram INSTANCE = new SubQueryRewriteProgram();
  private static final HepProgram PROGRAM = create();

  private SubQueryRewriteProgram() {}

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    var planner = new HepPlanner(PROGRAM, context);
    planner.setRoot(root);

    var ret = planner.findBestExp();

    var relBuilder = RelBuilder.proto(context).create(root.getCluster(), null);
    return RelDecorrelator.decorrelateQuery(ret, relBuilder);
  }

  private static HepProgram create() {
    var builder = HepProgram.builder().addMatchOrder(HepMatchOrder.BOTTOM_UP);

    BatchRuleSets.SUB_QUERY_REWRITE_RULES.forEach(builder::addRuleInstance);

    return builder.build();
  }
}
