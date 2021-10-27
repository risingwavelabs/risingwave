package com.risingwave.planner.rel.physical.streaming;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
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

/** The `RelNode` for streaming table source. */
public class RwStreamTableSource extends TableScan implements RisingWaveStreamingRel {
  protected final TableCatalog.TableId tableId;
  protected final ImmutableList<ColumnCatalog.ColumnId> columnIds;

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

  /** Serialize to protobuf */
  @Override
  public StreamNode serialize() {
    TableRefId tableRefId = Messages.getTableRefId(tableId);

    TableSourceNode.Builder tableSourceNodeBuilder =
        TableSourceNode.newBuilder().setTableRefId(tableRefId);
    columnIds.forEach(c -> tableSourceNodeBuilder.addColumnIds(c.getValue()));
    return StreamNode.newBuilder()
        .setNodeType(StreamNode.StreamNodeType.TABLE_INGRESS)
        .setTableSourceNode(tableSourceNodeBuilder.build())
        .build();
  }

  /** Derive row type from table catalog */
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

  /** Explain */
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
    return pw;
  }

  /**
   * The converter rule for converting a `RwLogicalScan` node to a `RwStreamTableSource`. In
   * distributed mode, an exchange node will be placed on top of `RwStreamTableSource`.
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

    /** Convert RwLogicalScan to RwStreamTableSource */
    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalScan source = (RwLogicalScan) rel;

      TableCatalog tableCatalog = source.getTable().unwrapOrThrow(TableCatalog.class);

      boolean isSingle = isSingleMode(contextOf(source.getCluster()));

      RelDistribution distTrait =
          isSingle ? RwDistributions.SINGLETON : RwDistributions.RANDOM_DISTRIBUTED;

      RelTraitSet newTraitSet =
          source.getTraitSet().plus(RisingWaveStreamingRel.STREAMING).plus(distTrait);

      var streamTableSource =
          new RwStreamTableSource(
              source.getCluster(),
              newTraitSet,
              Collections.emptyList(),
              source.getTable(),
              tableCatalog.getId(),
              source.getColumnIds());

      if (isSingle) {
        return streamTableSource;
      } else {
        int[] distFields = {0};
        RwDistributionTrait exchangeDistributionTrait = RwDistributions.hash(distFields);
        return RwStreamExchange.create(streamTableSource, exchangeDistributionTrait);
      }
    }
  }
}
