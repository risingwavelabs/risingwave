package com.risingwave.planner.rules.logical;

import com.risingwave.planner.rel.logical.LogicalFilterScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class BatchFilterScanRule extends RelRule<BatchFilterScanRule.Config> {
  private BatchFilterScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalTableScan source = call.rel(0);

    LogicalFilterScan newTableScan =
        LogicalFilterScan.create(source.getCluster(), source.getTraitSet(), source.getTable());

    call.transformTo(newTableScan);
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withDescription("Convert calcite table scan to rising wave logical table scan.")
            .withOperandSupplier(
                t -> t.operand(LogicalTableScan.class).trait(Convention.NONE).noInputs())
            .as(Config.class);

    @Override
    default BatchFilterScanRule toRule() {
      return new BatchFilterScanRule(this);
    }
  }
}
