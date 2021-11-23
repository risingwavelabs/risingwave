package com.risingwave.planner.rel.physical.streaming;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.physical.streaming.join.RwStreamHashJoin;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This visitor is used to construct the new plan and also the primary key for each operator. We
 * remark that we denote primary key by a list instead of a set as the order of the keys matters in
 * certain cases. For example, select v1, v2, count(*) from t group by v1, v2. In this case, the
 * order of v1, v2 does not matter. However, select v1, v2, count(*) from t group by v1, v2 order by
 * v2. In this case, the order has to be [v2, v1] but not [v1, v2].
 */
public class PrimaryKeyDerivationVisitor implements RwStreamingRelVisitor<ImmutableList<Integer>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryKeyDerivationVisitor.class);

  /**
   * @param node We want to find node's primary key
   * @return Result contains both the new node and its primary key indices
   */
  RwStreamingRelVisitor.Result<ImmutableList<Integer>> passThrough(RisingWaveStreamingRel node) {
    assert node.getInputs().size() == 1;
    RisingWaveStreamingRel input = (RisingWaveStreamingRel) node.getInput(0);
    var p = input.accept(this);
    RisingWaveStreamingRel newNode;
    if (p.node != input) {
      newNode = (RisingWaveStreamingRel) node.copy(node.getTraitSet(), ImmutableList.of(p.node));
    } else {
      newNode = node;
    }
    return new Result<>(newNode, p.info);
  }

  /**
   * FIXME: If we select some columns instead of * from the join result, there will be a project on
   * top of that. However, this project may remove some keys returned by Join. It cannot know that
   * it still needs to remove that key, as child can indeed produce more key after
   * RwStreamRelVisitor has visited the child. Hence, this function is solely designed for select *
   * from xxx join xxx; Here are some explicit or implicit assumptions/conditions enforced by
   * calcite: 1. The hash join's row type is derived by concatenating the row type of left input and
   * row type of right input. See Join's deriveRowType. 2. The leftKeys and rightKeys of JoinInfo
   * are one-to-one correspondent. 3. The leftKeys and rightKeys refer to the position of the row
   * type of join. RightKeys do NOT refer to the positions in the row type of right child. 4. The
   * newly added columns of children are always appended at last.
   *
   * @param hashJoin Join's primary key derivation rule is: outer_input.key() || inner_input.key()
   * @return New join and its output primary key.
   */
  @Override
  public RwStreamingRelVisitor.Result<ImmutableList<Integer>> visit(RwStreamHashJoin hashJoin) {
    LOGGER.debug("visit RwStreamHashJoin");
    RisingWaveStreamingRel leftInput = (RisingWaveStreamingRel) hashJoin.getInput(0);
    var originalLeftFieldCount = leftInput.getRowType().getFieldCount();
    var leftRes = leftInput.accept(this);
    var newLeftFieldCount = leftRes.node.getRowType().getFieldCount();
    RisingWaveStreamingRel rightInput = (RisingWaveStreamingRel) hashJoin.getInput(1);
    var rightRes = rightInput.accept(this);

    var joinInfo = hashJoin.analyzeCondition();
    var joinCondition = hashJoin.getCondition();
    var leftToRightJoinKeyIndices = new HashMap<Integer, Integer>();
    var rightJoinKeyIndices = new HashSet<Integer>();
    for (var p : joinInfo.pairs()) {
      leftToRightJoinKeyIndices.putIfAbsent(p.source, p.target);
    }
    var joinType = hashJoin.getJoinType();
    LOGGER.debug("originalLeftInput row type:" + leftInput.getRowType());
    LOGGER.debug("newLeftInput row type:" + leftRes.node.getRowType());
    LOGGER.debug("originalRightInput row type:" + rightInput.getRowType());
    LOGGER.debug("newLeftInput row type:" + rightRes.node.getRowType());

    LOGGER.debug("leftToRightJoinKeyIndices:" + leftToRightJoinKeyIndices);
    LOGGER.debug("rightJoinKeyIndices:" + rightJoinKeyIndices);

    List<Integer> primaryKeyIndices = new ArrayList<Integer>();
    switch (joinType) {
      case INNER:
        // Since the left child may add new columns, this would invalidate rightJoinKeyIndices.
        // Therefore, we need to renumber it.
        RexShuttle inputRefReplaceShuttle =
            new RexShuttle() {
              @Override
              public RexNode visitInputRef(RexInputRef inputRef) {
                var index = inputRef.getIndex();
                if (index >= originalLeftFieldCount) {
                  return new RexInputRef(
                      index + newLeftFieldCount - originalLeftFieldCount, inputRef.getType());
                } else {
                  return inputRef;
                }
              }
            };
        RexNode newJoinCondition = joinCondition.accept(inputRefReplaceShuttle);
        LOGGER.debug("join condition:" + joinCondition);
        LOGGER.debug("new join condition:" + newJoinCondition);
        var newHashJoin =
            new RwStreamHashJoin(
                hashJoin.getCluster(),
                hashJoin.getTraitSet(),
                hashJoin.getHints(),
                leftRes.node,
                rightRes.node,
                newJoinCondition,
                joinType);
        // First add all the output key from new left child
        for (var newLeftPrimaryKeyIndices : leftRes.info) {
          primaryKeyIndices.add(newLeftPrimaryKeyIndices);
          if (leftToRightJoinKeyIndices.containsKey(newLeftPrimaryKeyIndices)) {
            rightJoinKeyIndices.add(leftToRightJoinKeyIndices.get(newLeftPrimaryKeyIndices));
          }
        }
        // It is possible that the output key of left child may be a join key
        // We do NOT want to add the same join key twice when we process new right child.
        // But we DO need to add other non-join-key primary key from the new right child.
        for (var rightPrimaryKeyIndex : rightRes.info) {
          if (!rightJoinKeyIndices.contains(rightPrimaryKeyIndex + originalLeftFieldCount)) {
            primaryKeyIndices.add(rightPrimaryKeyIndex + newLeftFieldCount);
          }
        }
        LOGGER.debug("primary key indices:" + primaryKeyIndices);
        return new Result<>(newHashJoin, ImmutableList.copyOf(primaryKeyIndices));
      default:
        throw new IllegalArgumentException("Only support inner hash join now");
    }
  }

  /**
   * @param aggregate Aggregate's group by key is its output key.
   * @return Original aggregate
   */
  @Override
  public RwStreamingRelVisitor.Result<ImmutableList<Integer>> visit(RwStreamAgg aggregate) {
    LOGGER.debug("visit RwStreamAgg");
    var groupSet = aggregate.getGroupSet();
    // If the aggregate is a simple aggregate, it has no primary key or let's say it has a one and
    // only unique key.
    // This is fine because we would have only one row.
    var groupList = ImmutableList.copyOf(groupSet);
    return new Result<ImmutableList<Integer>>(aggregate, groupList);
  }

  /**
   * @param exchange Exchange does not alter primary key and input columns, just pass its input's
   *     primary key and columns up
   * @return Exchange and its output primary key
   */
  @Override
  public RwStreamingRelVisitor.Result<ImmutableList<Integer>> visit(RwStreamExchange exchange) {
    return passThrough(exchange);
  }

  /**
   * @param filter Filter does not alter primary key and input columns, just pass its input's
   *     primary key and columns up
   * @return Filter and its output primary key
   */
  @Override
  public RwStreamingRelVisitor.Result<ImmutableList<Integer>> visit(RwStreamFilter filter) {
    return passThrough(filter);
  }

  /**
   * Project needs extra care as it must keep the columns of primary key in its output.
   *
   * @param project Project does not alter primary key but DO alter input columns.
   * @return New project and its output primary key
   */
  @Override
  public RwStreamingRelVisitor.Result<ImmutableList<Integer>> visit(RwStreamProject project) {
    LOGGER.debug("visit RwStreamProject");
    var rexBuilder = project.getCluster().getRexBuilder();
    var originalInputRowType = project.getInput().getRowType();
    LOGGER.debug("originalInputRowType:" + originalInputRowType);

    RisingWaveStreamingRel input = (RisingWaveStreamingRel) project.getInput();
    var p = input.accept(this);
    var newInputRowType = p.node.getRowType();
    var newInputPrimaryKeyIndices = p.info;
    var newOutputPrimaryKeyIndices = new ArrayList<Integer>();
    LOGGER.debug("newInputRowType:" + newInputRowType);

    RwStreamProject newProject;

    // When the current project misses some primary keys from its child's, we need to change the
    // current project.
    // More specifically, we always need to add new columns in the new project and never delete one.
    var newProjects = new ArrayList<>(project.getProjects());
    var newFields = new ArrayList<>(project.getRowType().getFieldList());

    // We try to find whether the primary key of its child has been put into new project's projects:
    // 1. If yes, nothing happen.
    // 2. If not, we have to add a new InputRef so that that primary key will be one of the output
    // column in new project.
    for (int idx = 0; idx < newInputPrimaryKeyIndices.size(); idx++) {
      var primaryKeyIndex = newInputPrimaryKeyIndices.get(idx);
      // This primary key is in some old project expression, we have already added
      if (primaryKeyIndex < project.getProjects().size()) {
        newOutputPrimaryKeyIndices.add(primaryKeyIndex);
        continue;
      }
      boolean exist = false;
      for (int newIdx = 0; newIdx < newProjects.size(); newIdx++) {
        var newProjectRex = newProjects.get(newIdx);
        if (newProjectRex instanceof RexInputRef) {
          var inputRefIndex = ((RexInputRef) newProjectRex).getIndex();
          if (primaryKeyIndex == inputRefIndex) {
            exist = true;
            break;
          }
        }
      }
      // If we miss an input ref to a primary key in the child, we need to add one.
      if (!exist) {
        var field = newInputRowType.getFieldList().get(primaryKeyIndex);
        newProjects.add(rexBuilder.makeInputRef(field.getType(), primaryKeyIndex));
        newFields.add(field);
        newOutputPrimaryKeyIndices.add(newProjects.size() - 1);
      }
    }
    newProject =
        new RwStreamProject(
            project.getCluster(),
            project.getTraitSet(),
            project.getHints(),
            p.node,
            newProjects,
            rexBuilder.getTypeFactory().createStructType(newFields));
    LOGGER.debug("newInputPrimaryKeyIndices:" + newInputPrimaryKeyIndices);
    LOGGER.debug("newOutputPrimaryKeyIndices:" + newOutputPrimaryKeyIndices);
    LOGGER.debug("child:" + p.node);
    LOGGER.debug("newProject:" + newProject);
    return new Result<>(newProject, ImmutableList.copyOf(newOutputPrimaryKeyIndices));
  }

  /**
   * @param tableSource column 0 is the row id column.
   * @return A new Table source with possibly row id column added
   */
  @Override
  public RwStreamingRelVisitor.Result<ImmutableList<Integer>> visit(
      RwStreamTableSource tableSource) {
    LOGGER.debug("visit RwStreamTableSource");
    var tableCatalog = tableSource.getTable().unwrapOrThrow(TableCatalog.class);
    // There are two cases:
    // 1. We have already read the row id column, thus we don't need to read additionally
    // 2. We don't have the row id column, then we need to read it and put it as its only primary
    // key
    var rowIdColumn = tableCatalog.getRowIdColumn().getId();
    var columnIds = tableSource.getColumnIds();
    for (int idx = 0; idx < columnIds.size(); idx++) {
      var columnId = columnIds.get(idx);
      if (columnId.equals(rowIdColumn)) {
        LOGGER.debug("Already has row id column, no need to read again");
        return new Result<>(tableSource, ImmutableList.of(idx));
      }
    }
    // The other one is that we add the row id column to the back, otherwise it will mess up the
    // input ref index in the upstream operator.
    var columns =
        ImmutableList.<ColumnCatalog.ColumnId>builder()
            .addAll(tableSource.getColumnIds())
            .add(rowIdColumn)
            .build();
    var newTableSource =
        new RwStreamTableSource(
            tableSource.getCluster(),
            tableSource.getTraitSet(),
            tableSource.getHints(),
            tableSource.getTable(),
            tableSource.getTableId(),
            columns);
    var primaryKey = ImmutableList.of(columns.size() - 1);
    return new Result<ImmutableList<Integer>>(newTableSource, primaryKey);
  }
}
