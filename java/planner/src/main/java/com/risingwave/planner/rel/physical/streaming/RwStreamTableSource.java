package com.risingwave.planner.rel.physical.streaming;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalScan;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.proto.streaming.plan.TableSourceNode;
import com.risingwave.rpc.Messages;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwStreamTableSource extends TableScan implements RisingWaveStreamingRel {
  protected final TableCatalog.TableId tableId;
  protected final ImmutableList<ColumnCatalog.ColumnId> columnIds;
  protected boolean isDispatched = false;

  public RwStreamTableSource(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    super(cluster, traitSet, hints, table);
    this.tableId = tableId;
    this.columnIds = columnIds;
  }

  @Override
  public StreamNode serialize() {
    TableRefId tableRefId = Messages.getTableRefId(tableId);

    TableSourceNode.Builder tableSourceNodeBuilder =
        TableSourceNode.newBuilder().setTableRefId(tableRefId);
    columnIds.forEach(c -> tableSourceNodeBuilder.addColumnIds(c.getValue()));
    return StreamNode.newBuilder()
        .setNodeType(StreamNode.StreamNodeType.TABLE_INGRESS)
        .setBody(Any.pack(tableSourceNodeBuilder.build()))
        .build();
  }

  @Override
  public RelDataType deriveRowType() {
    RelDataTypeFactory.Builder typeBuilder = getCluster().getTypeFactory().builder();
    TableCatalog tableCatalog = getTable().unwrapOrThrow(TableCatalog.class);
    columnIds.stream()
        .map(tableCatalog::getColumnChecked)
        .forEachOrdered(
            col -> typeBuilder.add(col.getEntityName().getValue(), col.getDesc().getDataType()));
    return typeBuilder.build();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw.item("table", table.getQualifiedName());
    if (!columnIds.isEmpty()) {
      TableCatalog tableCatalog = getTable().unwrapOrThrow(TableCatalog.class);
      String columnNames =
          columnIds.stream()
              .map(tableCatalog::getColumnChecked)
              .map(ColumnCatalog::getEntityName)
              .map(ColumnCatalog.ColumnName::getValue)
              .collect(Collectors.joining(","));

      pw.item("columns", columnNames);
    }
    if (isDispatched) {
      pw.item("dispatched", true);
    }
    return pw;
  }

  /**
   * Remark by Yanghao (2021.9.25): in the current version, the LogicalFilterScan node in logical
   * plan contains no condition, hence we treat it as a TableScan node and convert it to a
   * TableSource node directly.
   */
  public static class StreamTableSourceConverterRule extends ConverterRule {
    public static final RwStreamTableSource.StreamTableSourceConverterRule INSTANCE =
        ConverterRule.Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(STREAMING)
            .withRuleFactory(StreamTableSourceConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalScan.class).anyInputs())
            .withDescription("Converting logical filter scan to streaming source node.")
            .as(Config.class)
            .toRule(StreamTableSourceConverterRule.class);

    protected StreamTableSourceConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalScan source = (RwLogicalScan) rel;

      TableCatalog tableCatalog = source.getTable().unwrapOrThrow(TableCatalog.class);

      RelDistribution distTrait =
          isSingleMode(contextOf(source.getCluster()))
              ? RwDistributions.SINGLETON
              : RwDistributions.RANDOM_DISTRIBUTED;

      RelTraitSet newTraitSet =
          source.getTraitSet().plus(RisingWaveStreamingRel.STREAMING).plus(distTrait);

      return new RwStreamTableSource(
          source.getCluster(),
          newTraitSet,
          Collections.emptyList(),
          source.getTable(),
          tableCatalog.getId(),
          source.getColumnIds());
    }
  }

  public void setDispatched() {
    isDispatched = true;
  }

  public boolean isDispatched() {
    return isDispatched;
  }
}
