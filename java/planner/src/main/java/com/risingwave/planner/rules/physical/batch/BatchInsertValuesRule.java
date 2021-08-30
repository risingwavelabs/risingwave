package com.risingwave.planner.rules.physical.batch;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalInsert;
import com.risingwave.planner.rel.logical.RwLogicalProject;
import com.risingwave.planner.rel.logical.RwLogicalValues;
import com.risingwave.planner.rel.physical.batch.RwBatchInsertValues;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

public class BatchInsertValuesRule extends RelRule<RelRule.Config> {

  private BatchInsertValuesRule(Config config) {
    super(config);
  }

  //  @Override
  //  public boolean matches(RelOptRuleCall call) {
  //    var input = call.rel(1);
  //    if (input instanceof RwLogicalProject) {
  //      var project = (RwLogicalProject) input;
  //      if (!(project.getInput() instanceof RwLogicalValues)) {
  //        return false;
  //      }
  //      var values = (RwLogicalValues) ((RwLogicalProject) input).getInput();
  //      return values.getTuples().size() == 1 &&
  //          values.getTuples().get(0).size() == 1 &&
  //          BigDecimal.ZERO.equals(values.getTuples().get(0).get(0).getValue());
  //    }
  //    return true;
  //  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalInsert insert = call.rel(0);
    var tuples = getTuples((RelNode) call.rel(1));

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
        new RwBatchInsertValues(insert.getCluster(), newTraitSet, tableCatalog, columnIds, tuples));
  }

  private static ImmutableList<ImmutableList<RexNode>> getTuples(RelNode input) {
    if (input instanceof RwLogicalProject) {
      return getTuples((RwLogicalProject) input);
    } else if (input instanceof RwLogicalValues) {
      return getTuples((RwLogicalValues) input);
    } else {
      throw new PgException(
          PgErrorCode.INTERNAL_ERROR, "Unrecognized input type: %s", input.getClass());
    }
  }

  private static ImmutableList<ImmutableList<RexNode>> getTuples(RwLogicalProject project) {
    return ImmutableList.of(ImmutableList.copyOf(project.getProjects()));
  }

  private static ImmutableList<ImmutableList<RexNode>> getTuples(RwLogicalValues values) {
    return values.getTuples().stream()
        .map(
            row ->
                row.stream()
                    .map(literal -> (RexNode) literal)
                    .collect(ImmutableList.toImmutableList()))
        .collect(ImmutableList.toImmutableList());
  }

  public interface Config extends RelRule.Config {
    Config VALUES =
        EMPTY
            .withDescription("Merge logical insert and logical values")
            .withOperandSupplier(
                t ->
                    t.operand(RwLogicalInsert.class)
                        .oneInput(t1 -> t1.operand(RwLogicalValues.class).noInputs()))
            .as(Config.class);

    Config PROJECT_VALUES =
        EMPTY
            .withDescription("Merge logical insert and logical project values")
            .withOperandSupplier(
                t ->
                    t.operand(RwLogicalInsert.class)
                        .oneInput(t1 -> t1.operand(RwLogicalProject.class).anyInputs()))
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new BatchInsertValuesRule(this);
    }
  }
}
