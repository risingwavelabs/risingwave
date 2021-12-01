package com.risingwave.planner.rules.logical;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.logical.RwLogicalScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.sql.SqlAggFunction;

/**
 * This rule is used to remove unnecessary columns in scan not used by aggregate function The only
 * case for now is that aggregate's functions require no inputs such as Count(*). We change it to
 * read from row id column. In the future, we may pick a column whose type is of the smallest size.
 * For example, select count(*) from t. Calcite reads all the columns from t. However, select v1,
 * v2, count(*) from t group by v1, v2. Calcite only reads v1, v2.
 */
public class SimpleCountStarColumnPruningRule
    extends RelRule<SimpleCountStarColumnPruningRule.Config> {
  protected SimpleCountStarColumnPruningRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RwLogicalAggregate aggregate = call.rel(0);
    // We remark that group by case does not lead to unnecessary reads
    // as count(*) would use the columns of group by keys
    if (!aggregate.isSimpleAgg()) {
      return false;
    }
    boolean flag = true;
    // If there are other aggregate functions such as count(v1)/sum(v1) in select,
    // then aggregate would just read v1 and avoid the case to be solved.
    for (var aggCall : aggregate.getAggCallList()) {
      SqlAggFunction function = aggCall.getAggregation();
      var argList = aggCall.getArgList();
      if (!argList.isEmpty()) {
        flag = false;
        break;
      }
    }
    return flag;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate aggregate = call.rel(0);
    RwLogicalScan scan = call.rel(1);
    TableCatalog table = scan.getTable().unwrapOrThrow(TableCatalog.class);
    // Here we do not want the scan to read any columns,
    // This would be a special case for the backend.
    ImmutableList<ColumnCatalog.ColumnId> columnList = ImmutableList.of();
    RwLogicalScan newScan = scan.copy(columnList);
    Aggregate newAggregate =
        aggregate.copy(
            aggregate.getTraitSet(),
            newScan,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList());
    call.transformTo(newAggregate);
  }

  /** Default Config */
  public interface Config extends RelRule.Config {
    SimpleCountStarColumnPruningRule.Config INSTANCE =
        EMPTY
            .withDescription("Remove Unnecessary Columns In Scan Not Used By Aggregate Function")
            .withOperandSupplier(
                t ->
                    t.operand(RwLogicalAggregate.class)
                        .oneInput(t1 -> t1.operand(RwLogicalScan.class).noInputs()))
            .as(SimpleCountStarColumnPruningRule.Config.class);

    default SimpleCountStarColumnPruningRule toRule() {
      return new SimpleCountStarColumnPruningRule(this);
    }
  }
}
