package com.risingwave.planner.rel.logical;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.rel.common.RwValues;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Logical Values in RisingWave. */
public class RwLogicalValues extends RwValues implements RisingWaveLogicalRel {
  protected RwLogicalValues(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexNode>> tuples,
      RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
    checkConvention();
  }

  /** Rule to convert from {@link LogicalValues} to {@link RwLogicalValues}. */
  public static class RwValuesConverterRule extends ConverterRule {
    public static final RwValuesConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwValuesConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalValues.class).noInputs())
            .withDescription("Converting logical values")
            .as(Config.class)
            .toRule(RwValuesConverterRule.class);

    protected RwValuesConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      LogicalValues logicalValues = (LogicalValues) rel;
      var tuples =
          enforceRowTypes(
              logicalValues.getCluster().getRexBuilder(),
              logicalValues.getRowType(),
              logicalValues.getTuples());
      return new RwLogicalValues(
          logicalValues.getCluster(),
          logicalValues.getRowType(),
          tuples,
          logicalValues.getTraitSet().replace(LOGICAL));
    }

    /**
     * Enforce the tuples to have same type as rowType. This is adapted from `Values.assertRowType`
     * but is stricter than it.
     */
    private ImmutableList<ImmutableList<RexNode>> enforceRowTypes(
        RexBuilder rexBuilder,
        RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples) {
      ImmutableList.Builder<ImmutableList<RexNode>> newTuples = ImmutableList.builder();
      for (List<RexLiteral> tuple : tuples) {
        ImmutableList.Builder<RexNode> newTuple = ImmutableList.builder();
        assert tuple.size() == rowType.getFieldCount();
        for (Pair<RexLiteral, RelDataTypeField> pair : Pair.zip(tuple, rowType.getFieldList())) {
          RexLiteral literal = pair.left;
          RelDataType fieldType = pair.right.getType();

          var newExpr = rexBuilder.ensureType(fieldType, literal, false);
          newTuple.add(newExpr);
        }
        newTuples.add(newTuple.build());
      }
      return newTuples.build();
    }
  }
}
