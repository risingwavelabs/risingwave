package com.risingwave.planner.rules.logical;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.planner.rel.logical.LogicalCalc;
import com.risingwave.planner.rel.logical.LogicalFilterScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;

public class CalcToScanRule extends RelRule<CalcToScanRule.Config> {
  public CalcToScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    // TODO: We need to optimize this
    LogicalCalc calc = call.rel(0);
    LogicalFilterScan scan = call.rel(1);

    if (calc.canBePushedToScan()) {
      ImmutableList<ColumnCatalog.ColumnId> prevColumnIds = scan.getColumnIds();
      ImmutableList.Builder<ColumnCatalog.ColumnId> newColumnIdsBuilder = ImmutableList.builder();

      calc.getProgram().getExprList().stream()
          .map(rex -> (RexInputRef) rex)
          .forEachOrdered(input -> newColumnIdsBuilder.add(prevColumnIds.get(input.getIndex())));

      call.transformTo(scan.copy(newColumnIdsBuilder.build()));
    }
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Push projection and filter into scan")
            .withOperandSupplier(
                t ->
                    t.operand(LogicalCalc.class)
                        .oneInput(input -> input.operand(TableScan.class).noInputs()))
            .as(Config.class);

    default CalcToScanRule toRule() {
      return new CalcToScanRule(this);
    }
  }
}
