package com.risingwave.planner;

import static com.risingwave.common.config.BatchPlannerConfigurations.OPTIMIZER_ENABLE_CALCITE_SUBQUERY_EXPAND;

import com.risingwave.planner.program.HepOptimizerProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.rules.logical.subquery.JoinPushExpressionsRule;
import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JoinConditionTypeCoerceRuleTest extends BatchPlanTestBase {
  private OptimizerProgram program;

  @BeforeAll
  public void initAll() {
    super.init();
    program = HepOptimizerProgram.builder().addRule(JoinPushExpressionsRule.INSTANCE).build();
  }

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("JoinConditionTypeCoerceRuleTest")
  @ArgumentsSource(PlanTestCaseLoader.class)
  public void testPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    runTestCaseWithProgram(testCase, program);
  }
}
