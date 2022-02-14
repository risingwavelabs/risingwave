package com.risingwave.planner.rel.physical;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.planner.rel.common.RwValues;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalValues;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.plan.Field;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.ValuesNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Physical version values operator.
 *
 * @see RwLogicalValues
 */
public class RwBatchValues extends RwValues implements RisingWaveBatchPhyRel {
  protected RwBatchValues(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexNode>> tuples,
      RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
    checkConvention();
  }

  @Override
  public PlanNode serialize() {
    ValuesNode.Builder valuesNodeBuilder = ValuesNode.newBuilder();
    for (int i = 0; i < tuples.size(); ++i) {
      ImmutableList<RexNode> tuple = tuples.get(i);
      ValuesNode.ExprTuple.Builder exprTupleBuilder = ValuesNode.ExprTuple.newBuilder();
      for (int j = 0; j < tuple.size(); ++j) {
        RexNode value = tuple.get(j);

        RexToProtoSerializer rexToProtoSerializer = new RexToProtoSerializer();

        // Add to Expr tuple.
        exprTupleBuilder.addCells(value.accept(rexToProtoSerializer));
      }
      valuesNodeBuilder.addTuples(exprTupleBuilder.build());
    }

    for (var field : rowType.getFieldList()) {
      DataType dataType = ((RisingWaveDataType) field.getType()).getProtobufType();
      valuesNodeBuilder.addFields(
          Field.newBuilder().setDataType(dataType).setName(field.getName()).build());
    }

    return PlanNode.newBuilder()
        .setValues(valuesNodeBuilder.build())
        .setIdentity("RwBatchValuesExecutor")
        .build();
  }

  public RelNode copy(RelTraitSet traitSet) {
    return new RwBatchValues(getCluster(), rowType, tuples, traitSet);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet);
  }

  @Override
  public RelNode convertToDistributed() {
    return copy(getTraitSet().plus(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON));
  }

  /** Values converter rule between logical and physical. */
  public static class BatchValuesConverterRule extends ConverterRule {
    public static final BatchValuesConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchValuesConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalValues.class).noInputs())
            .withDescription("Converting batch values")
            .as(Config.class)
            .toRule(BatchValuesConverterRule.class);

    protected BatchValuesConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalValues rwLogicalValues = (RwLogicalValues) rel;
      return new RwBatchValues(
          rwLogicalValues.getCluster(),
          rwLogicalValues.getRowType(),
          rwLogicalValues.getTuples(),
          rwLogicalValues.getTraitSet().plus(BATCH_PHYSICAL));
    }
  }
}
