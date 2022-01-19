package com.risingwave.planner.rel.physical;

import static com.google.common.base.Verify.verify;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalGenerateSeries;
import com.risingwave.proto.plan.GenerateInt32SeriesNode;
import com.risingwave.proto.plan.PlanNode;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Physical operator of generate_series, in batch convention. */
public class RwBatchGenerateSeries extends TableFunctionScan implements RisingWaveBatchPhyRel {
  protected RwBatchGenerateSeries(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      @Nullable Type elementType,
      RelDataType rowType,
      @Nullable Set<RelColumnMapping> columnMappings) {
    super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);
    checkConvention();

    // For simplicity, the arguments in generate_series are only allowed literals.
    verify(getCall() instanceof RexCall);
    var call = (RexCall) getCall();
    verify(call.getOperands().size() >= 2);
    verify(call.getOperands().get(0) instanceof RexLiteral);
    verify(call.getOperands().get(1) instanceof RexLiteral);
  }

  @Override
  public TableFunctionScan copy(
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      @Nullable Type elementType,
      RelDataType rowType,
      @Nullable Set<RelColumnMapping> columnMappings) {
    return new RwBatchGenerateSeries(
        getCluster(), traitSet, inputs, rexCall, elementType, rowType, columnMappings);
  }

  @Override
  public PlanNode serialize() {
    var call = (RexCall) getCall();
    var start = (RexLiteral) call.getOperands().get(0);
    var stop = (RexLiteral) call.getOperands().get(1);
    GenerateInt32SeriesNode node = null;
    if (start.getTypeName().equals(SqlTypeName.DECIMAL)
        && stop.getTypeName().equals(SqlTypeName.DECIMAL)) {
      node = serializeI32(start, stop);
    }
    if (node == null) {
      throw new IllegalArgumentException("no matching overload of generate_series");
    }
    return PlanNode.newBuilder().setGenerateInt32Series(node).build();
  }

  private GenerateInt32SeriesNode serializeI32(RexLiteral start, RexLiteral stop) {
    // generate_series(int, int)
    Integer startValue = start.getValueAs(Integer.class);
    verify(startValue != null);
    Integer stopValue = stop.getValueAs(Integer.class);
    verify(stopValue != null);
    return GenerateInt32SeriesNode.newBuilder()
        .setStart(startValue)
        .setStop(stopValue)
        .setStep(1)
        .build();
  }

  @Override
  public RelNode convertToDistributed() {
    return copy(
        getTraitSet().replace(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON), getInputs());
  }

  /** Converter rule to physical operator. */
  public static class BatchGenerateSeriesConverterRule extends ConverterRule {
    public static final BatchGenerateSeriesConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchGenerateSeriesConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalGenerateSeries.class).anyInputs())
            .withDescription("RwBatchGenerateSeries")
            .as(Config.class)
            .toRule(BatchGenerateSeriesConverterRule.class);

    protected BatchGenerateSeriesConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalGenerateSeries operator = (RwLogicalGenerateSeries) rel;
      return new RwBatchGenerateSeries(
          rel.getCluster(),
          rel.getTraitSet().plus(BATCH_PHYSICAL),
          operator.getInputs(),
          operator.getCall(),
          operator.getElementType(),
          operator.getRowType(),
          operator.getColumnMappings());
    }
  }
}
