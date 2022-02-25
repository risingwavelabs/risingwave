package com.risingwave.planner.rel.streaming;

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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexInputRef;
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

  private TableCatalog.TableId associatedTableId;

  private final SqlIdentifier name;

  private final ImmutableList<Integer> primaryKeyIndices;

  private final RelCollation collation;

  public RwStreamMaterializedView(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      SqlIdentifier name,
      ImmutableList<Integer> primaryKeyIndices) {
    this(cluster, traits, input, name, primaryKeyIndices, null);
  }

  public RwStreamMaterializedView(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      SqlIdentifier name,
      ImmutableList<Integer> primaryKeyIndices,
      @Nullable RelCollation relCollation) {
    super(cluster, traits, input);
    checkConvention();
    this.name = name;
    this.primaryKeyIndices = primaryKeyIndices;
    this.collation = relCollation;
  }

  public List<Integer> getPrimaryKeyIndices() {
    return this.primaryKeyIndices;
  }

  public RelCollation getCollation() {
    return collation;
  }

  private static void generateDependentTables(
      RelNode root, List<TableCatalog.TableId> dependencies) {
    if (root instanceof RwStreamTableSource) {
      dependencies.add(((RwStreamTableSource) root).getTableId());
    } else if (root instanceof RwStreamChain) {
      dependencies.add(((RwStreamChain) root).getTableId());
    }
    root.getInputs()
        .forEach(
            node -> {
              generateDependentTables(node, dependencies);
            });
  }

  public List<TableCatalog.TableId> getDependentTables() {
    List<TableCatalog.TableId> dependencies = new ArrayList<>();
    generateDependentTables(this, dependencies);
    return dependencies;
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
    // Add column IDs (mocked)
    // FIXME: Column ID should be in the catalog
    for (int i = 0; i < this.getColumns().size(); i++) {
      materializedViewNodeBuilder.addColumnIds(i);
    }
    // Set table ref id.
    materializedViewNodeBuilder.setTableRefId(Messages.getTableRefId(tableId));
    if (associatedTableId != null) {
      materializedViewNodeBuilder.setAssociatedTableRefId(
          Messages.getTableRefId(associatedTableId));
    }
    // Set column orders.
    // Sort key serialization starts. The column that is in primary key but not sort key should be
    // ordered by `Ascending`
    List<ColumnOrder> columnOrders = new ArrayList<ColumnOrder>();
    Set<Integer> columnAdded = new HashSet<Integer>();
    if (collation != null) {
      List<RelFieldCollation> rfc = collation.getFieldCollations();
      for (RelFieldCollation relFieldCollation : rfc) {
        RexInputRef inputRef =
            getCluster().getRexBuilder().makeInputRef(input, relFieldCollation.getFieldIndex());
        DataType returnType = ((RisingWaveDataType) inputRef.getType()).getProtobufType();
        columnAdded.add(inputRef.getIndex());
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
    }
    for (var primaryKeyIndex : this.getPrimaryKeyIndices()) {
      if (!columnAdded.contains(primaryKeyIndex)) {
        RexInputRef inputRef = getCluster().getRexBuilder().makeInputRef(input, primaryKeyIndex);
        DataType returnType = ((RisingWaveDataType) inputRef.getType()).getProtobufType();
        columnAdded.add(inputRef.getIndex());
        InputRefExpr inputRefExpr =
            InputRefExpr.newBuilder().setColumnIdx(inputRef.getIndex()).build();
        OrderType orderType = OrderType.ASCENDING;
        ColumnOrder columnOrder =
            ColumnOrder.newBuilder()
                .setOrderType(orderType)
                .setInputRef(inputRefExpr)
                .setReturnType(returnType)
                .build();
        columnOrders.add(columnOrder);
        columnAdded.add(primaryKeyIndex);
      }
    }
    materializedViewNodeBuilder.addAllColumnOrders(columnOrders);
    // Sort key serialization ends
    // Build and return.
    MViewNode materializedViewNode = materializedViewNodeBuilder.build();
    return StreamNode.newBuilder()
        .setMviewNode(materializedViewNode)
        .setIdentity(StreamingPlan.getCurrentNodeIdentity(this))
        .build();
  }

  public void setTableId(TableCatalog.TableId tableId) {
    // An ugly implementation to receive TableId from TableCatalog.
    this.tableId = tableId;
  }

  public void setAssociatedTableId(TableCatalog.TableId tableId) {
    this.associatedTableId = tableId;
  }

  public TableCatalog.TableId getAssociatedTableId() {
    return this.associatedTableId;
  }

  /** Explain */
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    var writer = super.explainTerms(pw).item("name", name);
    if (collation != null) {
      writer = writer.item("collation", collation);
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
