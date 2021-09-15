package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.logical.RwLogicalInsertValues;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.plan.InsertValueNode;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.rpc.Messages;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwBatchInsertValues extends AbstractRelNode implements RisingWaveBatchPhyRel {
  private final TableCatalog table;
  private final ImmutableList<ColumnCatalog.ColumnId> columnIds;
  // We change this to rex node here since we may have cast expression.
  private final ImmutableList<ImmutableList<RexNode>> tuples;

  public RwBatchInsertValues(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      TableCatalog table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds,
      ImmutableList<ImmutableList<RexNode>> tuples) {
    super(cluster, traitSet);
    this.table = requireNonNull(table, "Table can't be null!");
    this.columnIds = requireNonNull(columnIds, "columnIds can't be null!");
    this.tuples = requireNonNull(tuples, "tuples can't be null!");
    checkConvention();
  }

  @Override
  public PlanNode serialize() {
    InsertValueNode.Builder insertValueNodeBuilder =
        InsertValueNode.newBuilder().setTableRefId(Messages.getTableRefId(table.getId()));
    // TODO: Only consider constant values (no casting) for now.
    for (ColumnCatalog columnCatalog : table.getAllColumnCatalogs()) {
      insertValueNodeBuilder.addColumnIds(columnCatalog.getId().getValue());
    }

    for (int i = 0; i < tuples.size(); ++i) {
      ImmutableList<RexNode> tuple = tuples.get(i);
      InsertValueNode.ExprTuple.Builder exprTupleBuilder = InsertValueNode.ExprTuple.newBuilder();
      for (int j = 0; j < tuple.size(); ++j) {
        RexNode value = tuple.get(j);

        RexToProtoSerializer rexToProtoSerializer = new RexToProtoSerializer();

        // Add to Expr tuple.
        exprTupleBuilder.addCells(value.accept(rexToProtoSerializer));
      }
      insertValueNodeBuilder.addInsertTuples(exprTupleBuilder.build());
    }

    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.INSERT_VALUE)
        .setBody(Any.pack(insertValueNodeBuilder.build()))
        .build();
  }

  @Override
  protected RelDataType deriveRowType() {
    return RelOptUtil.createDmlRowType(SqlKind.INSERT, getCluster().getTypeFactory());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);

    pw.item("table", table.getEntityName().getValue());

    if (!columnIds.isEmpty()) {
      String columnNames = table.joinColumnNames(columnIds, ",");
      pw.item("columns", columnNames);
    }

    String values =
        tuples.stream()
            .map(RwBatchInsertValues::toString)
            .collect(Collectors.joining(",", "(", ")"));
    pw.item("values", values);
    return pw;
  }

  private static String toString(ImmutableList<RexNode> row) {
    requireNonNull(row, "row");
    return row.stream().map(RexNode::toString).collect(Collectors.joining(",", "(", ")"));
  }

  public static class BatchInsertValuesConverterRule extends ConverterRule {
    public static final BatchInsertValuesConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchInsertValuesConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalInsertValues.class).noInputs())
            .withDescription("Converting insert values to batch physical.")
            .as(Config.class)
            .toRule(BatchInsertValuesConverterRule.class);

    protected BatchInsertValuesConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var logicalInsertValues = (RwLogicalInsertValues) rel;
      return new RwBatchInsertValues(
          rel.getCluster(),
          rel.getTraitSet().plus(BATCH_PHYSICAL),
          logicalInsertValues.getTableCatalog(),
          logicalInsertValues.getColumnIds(),
          logicalInsertValues.getTuples());
    }
  }
}
