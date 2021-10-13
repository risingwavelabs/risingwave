package com.risingwave.planner.rel.physical.streaming;

import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.proto.streaming.plan.MaterializedViewNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

/**
 * We need to explicitly specify a materialized view node in a streaming plan.
 *
 * <p>A sequential streaming plan (no parallel degree) roots with a materialized view node.
 */
public class RwStreamMaterializedView extends Project implements RisingWaveStreamingRel {
  // TODO: define more attributes corresponding to TableCatalog.
  TableCatalog.TableId tableId;

  public RwStreamMaterializedView(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traits, hints, input, projects, rowType);
    this.rowType = rowType;
    checkConvention();
  }

  @Override
  public Project copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new RwStreamMaterializedView(
        getCluster(), traitSet, getHints(), input, projects, rowType);
  }

  // public RwStreamMaterializedView copy(RelTraitSet traitSet, RelNode input, RelDataType rowType)
  // {
  //  return new RwStreamMaterializedView(getCluster(), traitSet, input, rowType);
  // }

  @Override
  public StreamNode serialize() {
    MaterializedViewNode.Builder materializedViewNodeBuilder = MaterializedViewNode.newBuilder();
    for (Pair<String, ColumnDesc> pair : getColumns()) {
      com.risingwave.proto.plan.ColumnDesc.Builder columnDescBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();
      columnDescBuilder
          .setEncoding(com.risingwave.proto.plan.ColumnDesc.ColumnEncodingType.RAW)
          .setColumnType(pair.getValue().getDataType().getProtobufType())
          .setIsPrimary(false);
      materializedViewNodeBuilder.addColumnDescs(columnDescBuilder);
    }
    MaterializedViewNode materializedViewNode =
        materializedViewNodeBuilder.setTableRefId(Messages.getTableRefId(tableId)).build();
    return StreamNode.newBuilder()
        .setNodeType(StreamNode.StreamNodeType.MEMTABLE_MATERIALIZED_VIEW)
        .setBody(Any.pack(materializedViewNode))
        .setInput(((RisingWaveStreamingRel) input).serialize())
        .build();
  }

  public void setTableId(TableCatalog.TableId tableId) {
    // An ugly implementation to receive TableId from TableCatalog.
    this.tableId = tableId;
  }

  public List<Pair<String, ColumnDesc>> getColumns() {
    List<Pair<String, ColumnDesc>> list = new ArrayList<>();
    var rowType = getRowType();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      var field = rowType.getFieldList().get(i);
      ColumnDesc columnDesc =
          new ColumnDesc((RisingWaveDataType) field.getType(), false, ColumnEncoding.RAW);
      list.add(Pair.of(field.getName(), columnDesc));
    }
    return list;
  }
}
