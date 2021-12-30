package com.risingwave.planner.rel.streaming;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.metadata.RisingWaveRelMetadataQuery;
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The `RelNode` for streaming table source. */
public class RwStreamMaterializedViewSource extends TableScan implements RisingWaveStreamingRel {
  protected final TableCatalog.TableId tableId;
  protected final ImmutableList<ColumnCatalog.ColumnId> columnIds;

  public RwStreamMaterializedViewSource(
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

  public TableCatalog.TableId getTableId() {
    return tableId;
  }

  public ImmutableList<ColumnCatalog.ColumnId> getColumnIds() {
    return columnIds;
  }

  /** Serialize to protobuf */
  @Override
  public StreamNode serialize() {
    TableRefId tableRefId = Messages.getTableRefId(tableId);

    TableSourceNode.Builder tableSourceNodeBuilder =
        TableSourceNode.newBuilder()
            .setTableRefId(tableRefId)
            .setSourceType(TableSourceNode.SourceType.SOURCE);

    columnIds.forEach(c -> tableSourceNodeBuilder.addColumnIds(c.getValue()));
    var primaryKeyIndices =
        ((RisingWaveRelMetadataQuery) getCluster().getMetadataQuery()).getPrimaryKeyIndices(this);
    return StreamNode.newBuilder()
        .setTableSourceNode(tableSourceNodeBuilder.build())
        .addAllPkIndices(primaryKeyIndices)
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

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }

  /**
   * The converter rule for converting a `RwLogicalScan` node to a `RwStreamTableSource`. In
   * distributed mode, an exchange node will be placed on top of `RwStreamTableSource`.
   */
  public static class StreamMaterializedViewSourceConverterRule extends ConverterRule {
    public static final StreamMaterializedViewSourceConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(STREAMING)
            .withRuleFactory(StreamMaterializedViewSourceConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalScan.class).anyInputs())
            .withDescription("Converting logical filter scan to streaming source node.")
            .as(Config.class)
            .toRule(StreamMaterializedViewSourceConverterRule.class);

    protected StreamMaterializedViewSourceConverterRule(Config config) {
      super(config);
    }

    /** Convert RwLogicalScan to RwStreamMaterializedViewSource and add an Exchange on top of it */
    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalScan source = (RwLogicalScan) rel;

      TableCatalog tableCatalog = source.getTable().unwrapOrThrow(TableCatalog.class);
      RelTraitSet traits =
          source
              .getTraitSet()
              .plus(RisingWaveStreamingRel.STREAMING)
              .plus(RwDistributions.RANDOM_DISTRIBUTED);

      var streamMaterializedViewSource =
          new RwStreamMaterializedViewSource(
              source.getCluster(),
              traits,
              Collections.emptyList(),
              source.getTable(),
              tableCatalog.getId(),
              source.getColumnIds());

      // TODO: will be removed once single mode removed
      boolean isSingle = isSingleMode(contextOf(source.getCluster()));
      if (isSingle) {
        return streamMaterializedViewSource;
      }

      // Add an Exchange node on top of the TableSource operator.
      //
      // Currently, storage service does not guarantee a DELETE or UPDATE message comes from
      // the node where this row being inserted. To accomplish that, an Exchange operator
      // is added to redistribute rows by their first field.
      //
      // Distribution by first field may not always be a good solution, but for now, just
      // simply do it.
      if (!streamMaterializedViewSource.getColumnIds().isEmpty()) {
        int[] distFields = {0};
        RwDistributionTrait exchangeDistributionTrait = RwDistributions.hash(distFields);
        return RwStreamExchange.create(streamMaterializedViewSource, exchangeDistributionTrait);
      } else {
        return streamMaterializedViewSource;
      }
    }
  }
}
