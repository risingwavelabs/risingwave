package com.risingwave.planner.rules.logical;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.planner.rel.logical.RwLogicalProject;
import com.risingwave.planner.rel.logical.RwLogicalScan;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;

public class ProjectToTableScanRule extends RelRule<ProjectToTableScanRule.Config> {
  protected ProjectToTableScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalProject project = call.rel(0);
    RwLogicalScan scan = call.rel(1);

    SortedSet<Integer> inputIndexes = new TreeSet<>();
    new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitInputRef(RexInputRef inputRef) {
        inputIndexes.add(inputRef.getIndex());
        return null;
      }
    }.visitEach(project.getProjects());

    ImmutableList<ColumnCatalog.ColumnId> oldColumnIds = scan.getColumnIds();
    ImmutableList<ColumnCatalog.ColumnId> newColumnIds =
        inputIndexes.stream().map(oldColumnIds::get).collect(ImmutableList.toImmutableList());

    Map<Integer, Integer> oldIndexToNewIndexMap = new HashMap<>();
    int newIndex = 0;
    for (Integer oldInputIndex : inputIndexes) {
      oldIndexToNewIndexMap.put(oldInputIndex, newIndex);
      newIndex++;
    }

    RexShuttle inputRefReplaceShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitInputRef(RexInputRef inputRef) {
            return new RexInputRef(
                oldIndexToNewIndexMap.get(inputRef.getIndex()), inputRef.getType());
          }
        };

    List<RexNode> newProjects =
        project.getProjects().stream()
            .map(inputRefReplaceShuttle::apply)
            .collect(Collectors.toList());

    boolean allAreInputRef = newProjects.stream().allMatch(t -> t instanceof RexInputRef);

    if (allAreInputRef) {
      ImmutableList<ColumnCatalog.ColumnId> projectColumnIds =
          newProjects.stream()
              .map(rex -> newColumnIds.get(((RexInputRef) rex).getIndex()))
              .collect(ImmutableList.toImmutableList());

      RwLogicalScan newScan = scan.copy(projectColumnIds);
      call.transformTo(newScan);
    } else {
      RwLogicalScan newScan = scan.copy(newColumnIds);
      RwLogicalProject newProject =
          project.copy(project.getTraitSet(), newScan, newProjects, project.getRowType());

      call.transformTo(newProject);
    }
  }

  public interface Config extends RelRule.Config {
    Config INSTANCE =
        EMPTY
            .withDescription("Push project to table scan")
            .withOperandSupplier(
                t ->
                    t.operand(RwLogicalProject.class)
                        .oneInput(t1 -> t1.operand(RwLogicalScan.class).noInputs()))
            .as(Config.class);

    default ProjectToTableScanRule toRule() {
      return new ProjectToTableScanRule(this);
    }
  }
}
