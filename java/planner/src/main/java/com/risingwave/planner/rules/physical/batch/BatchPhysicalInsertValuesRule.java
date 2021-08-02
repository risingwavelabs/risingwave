package com.risingwave.planner.rules.physical.batch;

import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.logical.RwInsert;
import com.risingwave.planner.rel.logical.RwValues;
import com.risingwave.planner.rel.physical.batch.BatchInsertValues;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;

public class BatchPhysicalInsertValuesRule extends RelRule<RelRule.Config> {
  private BatchPhysicalInsertValuesRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwInsert insert = call.rel(0);
    RwValues values = call.rel(1);

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

    RelTraitSet newTraitSet = insert.getTraitSet().replace(BATCH_PHYSICAL);

    call.transformTo(
        new BatchInsertValues(
            insert.getCluster(), newTraitSet, tableCatalog, columnIds, values.getTuples()));
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withDescription("Merge logical insert and logical values")
            .withOperandSupplier(
                t ->
                    t.operand(RwInsert.class).oneInput(t1 -> t1.operand(RwValues.class).noInputs()))
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new BatchPhysicalInsertValuesRule(this);
    }
  }
}
