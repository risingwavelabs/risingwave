package com.risingwave.planner.rel.logical;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Logical operator of <code>generate_series</code>. */
public class RwLogicalGenerateSeries extends TableFunctionScan implements RisingWaveLogicalRel {
  protected RwLogicalGenerateSeries(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      @Nullable Type elementType,
      RelDataType rowType,
      @Nullable Set<RelColumnMapping> columnMappings) {
    super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);
    checkConvention();
  }

  @Override
  public TableFunctionScan copy(
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      @Nullable Type elementType,
      RelDataType rowType,
      @Nullable Set<RelColumnMapping> columnMappings) {
    return new RwLogicalGenerateSeries(
        getCluster(),
        traitSet,
        getInputs(),
        getCall(),
        getElementType(),
        getRowType(),
        getColumnMappings());
  }

  /**
   * <code>generate_series</code> is initially identified as a LogicalTableFunctionScan by calcite.
   * This rule converts LogicalTableFunctionScan to RwLogicalGenerateSeries.
   */
  public static class RwGenerateSeriesConverterRule extends ConverterRule {
    public static final RwGenerateSeriesConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwGenerateSeriesConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalTableFunctionScan.class).anyInputs())
            .withDescription("RwLogicalGenerateSeries")
            .as(Config.class)
            .toRule(RwGenerateSeriesConverterRule.class);

    protected RwGenerateSeriesConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      LogicalTableFunctionScan operator = (LogicalTableFunctionScan) rel;
      return new RwLogicalGenerateSeries(
          rel.getCluster(),
          rel.getTraitSet().plus(LOGICAL),
          operator.getInputs(),
          operator.getCall(),
          operator.getElementType(),
          operator.getRowType(),
          operator.getColumnMappings());
    }
  }
}
