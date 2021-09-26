package com.risingwave.planner.rel.physical.streaming;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.logical.RwLogicalFilterScan;
import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.checkerframework.checker.nullness.qual.Nullable;

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

  @Override
  public StreamNode serialize() {
    throw new UnsupportedOperationException("Not implemented yet");
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
            .withRuleFactory(RwStreamTableSource.StreamTableSourceConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalFilterScan.class).anyInputs())
            .withDescription("Converting logical filter scan to streaming source node.")
            .as(Config.class)
            .toRule(RwStreamTableSource.StreamTableSourceConverterRule.class);

    protected StreamTableSourceConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalFilterScan source = (RwLogicalFilterScan) rel;

      return new RwStreamTableSource(
          source.getCluster(),
          source.getTraitSet(),
          source.getHints(),
          source.getTable(),
          source.getTableId(),
          source.getColumnIds());
    }
  }
}
