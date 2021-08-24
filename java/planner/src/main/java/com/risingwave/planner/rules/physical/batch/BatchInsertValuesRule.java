package com.risingwave.planner.rules.physical.batch;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalInsert;
import com.risingwave.planner.rel.logical.RwLogicalValues;
import com.risingwave.planner.rel.physical.batch.RwBatchInsertValues;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;

public class BatchInsertValuesRule extends RelRule<RelRule.Config> {
  private BatchInsertValuesRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalInsert insert = call.rel(0);
    RwLogicalValues values = call.rel(1);

    TableCatalog tableCatalog = insert.getTable().unwrapOrThrow(TableCatalog.class);
    ImmutableList<ColumnCatalog.ColumnId> columnIds =
        Optional.ofNullable(insert.getUpdateColumnList())
            .map(
                columns ->
                    columns.stream()
                        .map(tableCatalog::getColumnChecked)
                        .map(ColumnCatalog::getId)
                        .collect(ImmutableList.toImmutableList()))
            .orElseGet(ImmutableList::of);

    RelDistribution distTrait =
        isSingleMode(contextOf(call))
            ? RwDistributions.SINGLETON
            : RwDistributions.RANDOM_DISTRIBUTED;
    RelTraitSet newTraitSet = insert.getTraitSet().plus(BATCH_PHYSICAL).plus(distTrait);

    call.transformTo(
        new RwBatchInsertValues(
            insert.getCluster(), newTraitSet, tableCatalog, columnIds, values.getTuples()));
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withDescription("Merge logical insert and logical values")
            .withOperandSupplier(
                t ->
                    t.operand(RwLogicalInsert.class)
                        .oneInput(t1 -> t1.operand(RwLogicalValues.class).noInputs()))
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new BatchInsertValuesRule(this);
    }
  }
}
