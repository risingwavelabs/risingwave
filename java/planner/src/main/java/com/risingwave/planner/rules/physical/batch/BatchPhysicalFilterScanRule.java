package com.risingwave.planner.rules.physical.batch;

import com.risingwave.planner.rel.logical.LogicalFilterScan;
import com.risingwave.planner.rel.logical.RisingWaveLogicalRel;
import com.risingwave.planner.rel.physical.batch.PhysicalFilterScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

public class BatchPhysicalFilterScanRule extends RelRule<BatchPhysicalFilterScanRule.Config> {
  public BatchPhysicalFilterScanRule(BatchPhysicalFilterScanRule.Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilterScan source = call.rel(0);

    PhysicalFilterScan newTableScan =
        PhysicalFilterScan.create(source.getCluster(), source.getTraitSet(), source.getTable());

    call.transformTo(newTableScan);
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        Config.EMPTY
            .withDescription("Converting logical filter scan to physical filter scan")
            .withOperandSupplier(
                t ->
                    t.operand(LogicalFilterScan.class)
                        .trait(RisingWaveLogicalRel.LOGICAL)
                        .noInputs())
            .as(Config.class);

    default BatchPhysicalFilterScanRule toRule() {
      return new BatchPhysicalFilterScanRule(this);
    }
  }
}
