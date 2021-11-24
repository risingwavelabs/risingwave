package com.risingwave.planner.rel.physical.streaming;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.proto.streaming.plan.MViewNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Pair;

/**
 * We need to explicitly specify a materialized view node in a streaming plan.
 *
 * <p>A sequential streaming plan (no parallel degree) roots with a materialized view node.
 */
public class RwStreamMaterializedView extends SingleRel implements RisingWaveStreamingRel {
  // TODO: define more attributes corresponding to TableCatalog.
  private TableCatalog.TableId tableId;

  private final SqlIdentifier name;

  private final ImmutableList<Integer> primaryKeyIndices;

  public RwStreamMaterializedView(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      SqlIdentifier name,
      ImmutableList<Integer> primaryKeyIndices) {
    super(cluster, traits, input);
    checkConvention();
    this.name = name;
    this.primaryKeyIndices = primaryKeyIndices;
  }

  public List<Integer> getPrimaryKeyIndices() {
    return this.primaryKeyIndices;
  }

  /** Serialize to protobuf */
  @Override
  public StreamNode serialize() {
    MViewNode.Builder materializedViewNodeBuilder = MViewNode.newBuilder();
    // Add columns.
    for (Pair<String, ColumnDesc> pair : getColumns()) {
      com.risingwave.proto.plan.ColumnDesc.Builder columnDescBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();
      columnDescBuilder
          .setEncoding(com.risingwave.proto.plan.ColumnDesc.ColumnEncodingType.RAW)
          .setColumnType(pair.getValue().getDataType().getProtobufType())
          .setIsPrimary(false);
      materializedViewNodeBuilder.addColumnDescs(columnDescBuilder);
    }
    // Set primary key columns.
    materializedViewNodeBuilder.addAllPkIndices(this.getPrimaryKeyIndices());
    // Set table ref id.
    materializedViewNodeBuilder.setTableRefId(Messages.getTableRefId(tableId));
    // Build and return.
    MViewNode materializedViewNode = materializedViewNodeBuilder.build();
    return StreamNode.newBuilder().setMviewNode(materializedViewNode).build();
  }

  public void setTableId(TableCatalog.TableId tableId) {
    // An ugly implementation to receive TableId from TableCatalog.
    this.tableId = tableId;
  }

  /** Explain */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("name", name);
  }

  /**
   * Return a list of column descriptions from the underlying expression. The column descriptions
   * can be used to generate metadata for storage.
   *
   * @return List of name->column description pairs.
   */
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

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    throw new UnsupportedOperationException();
  }
}
