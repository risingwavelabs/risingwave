package com.risingwave.planner.program;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.metadata.RisingWaveRelMetadataProvider;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;

public class HepOptimizerProgram implements OptimizerProgram {
  private final HepProgram hepProgram;

  private HepOptimizerProgram(HepProgram hepProgram) {
    this.hepProgram = hepProgram;
  }

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    HepPlanner optimizer = new HepPlanner(hepProgram, context);
    optimizer.setRoot(root);

    RisingWaveRelMetadataProvider.getMetadataProvider();

    return optimizer.findBestExp();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private HepMatchOrder matchOrder = HepMatchOrder.ARBITRARY;
    private int matchLimit = Integer.MAX_VALUE;
    private List<RelOptRule> rules = new ArrayList<>();
    private boolean useRuleInstance = false;

    public Builder withMatchOrder(HepMatchOrder matchOrder) {
      this.matchOrder = matchOrder;
      return this;
    }

    public Builder withMatchLimit(int matchLimit) {
      this.matchLimit = matchLimit;
      return this;
    }

    public Builder addRules(RuleSet ruleSet) {
      ruleSet.forEach(this.rules::add);

      return this;
    }

    public Builder addRule(RelOptRule rule) {
      rules.add(rule);
      return this;
    }

    public Builder withUseRuleInstance(boolean useRuleInstance) {
      this.useRuleInstance = useRuleInstance;
      return this;
    }

    public HepOptimizerProgram build() {
      HepProgramBuilder hepProgramBuilder =
          HepProgram.builder().addMatchOrder(matchOrder).addMatchLimit(matchLimit);

      if (useRuleInstance) {
        this.rules.forEach(hepProgramBuilder::addRuleInstance);
      } else {
        hepProgramBuilder.addRuleCollection(rules);
      }

      return new HepOptimizerProgram(hepProgramBuilder.build());
    }
  }
}
