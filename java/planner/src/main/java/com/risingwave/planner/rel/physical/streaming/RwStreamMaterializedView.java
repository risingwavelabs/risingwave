package com.risingwave.planner.rel.physical.streaming;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.plan.ColumnOrder;
import com.risingwave.proto.plan.OrderType;
import com.risingwave.proto.streaming.plan.MViewNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.SerializationException;

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

  private final RelCollation collation;

  private final RexNode offset;

  private final RexNode fetch;

  public RwStreamMaterializedView(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      SqlIdentifier name,
      ImmutableList<Integer> primaryKeyIndices) {
    this(cluster, traits, input, name, primaryKeyIndices, null, null, null);
  }

  public RwStreamMaterializedView(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      SqlIdentifier name,
      ImmutableList<Integer> primaryKeyIndices,
      @Nullable RelCollation relCollation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traits, input);
    checkConvention();
    this.name = name;
    this.primaryKeyIndices = primaryKeyIndices;
    this.collation = relCollation;
    this.offset = offset;
    this.fetch = fetch;
  }

  public List<Integer> getPrimaryKeyIndices() {
    return this.primaryKeyIndices;
  }

  /**
   * Serialize to protobuf
   *
   * <p>We remark that we DO need to tell the backend about the sort keys, but we do NOT need to
   * tell the backend about the offset and fetch. These two properties should only be documented by
   * the frontend so that when the OLAP system query the MV, it can properly insert proper
   * executors/operators to enforce fetch/offset.
   */
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
    // Set column orders.
    // Sort key serialization starts
    if (collation != null) {
      List<ColumnOrder> columnOrders = new ArrayList<ColumnOrder>();
      List<RelFieldCollation> rfc = collation.getFieldCollations();
      for (RelFieldCollation relFieldCollation : rfc) {
        RexInputRef inputRef =
            getCluster().getRexBuilder().makeInputRef(input, relFieldCollation.getFieldIndex());
        DataType returnType = ((RisingWaveDataType) inputRef.getType()).getProtobufType();
        InputRefExpr inputRefExpr =
            InputRefExpr.newBuilder().setColumnIdx(inputRef.getIndex()).build();
        RelFieldCollation.Direction dir = relFieldCollation.getDirection();
        OrderType orderType;
        if (dir == RelFieldCollation.Direction.ASCENDING) {
          orderType = OrderType.ASCENDING;
        } else if (dir == RelFieldCollation.Direction.DESCENDING) {
          orderType = OrderType.DESCENDING;
        } else {
          throw new SerializationException(String.format("%s direction not supported", dir));
        }
        ColumnOrder columnOrder =
            ColumnOrder.newBuilder()
                .setOrderType(orderType)
                .setInputRef(inputRefExpr)
                .setReturnType(returnType)
                .build();
        columnOrders.add(columnOrder);
      }
      materializedViewNodeBuilder.addAllColumnOrders(columnOrders);
    }
    // Sort key serialization ends
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
    var writer = super.explainTerms(pw).item("name", name);
    if (collation != null) {
      writer = writer.item("collation", collation);
    }
    if (offset != null) {
      writer = writer.item("offset", offset);
    }
    if (fetch != null) {
      writer = writer.item("limit", fetch);
    }
    return writer;
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
