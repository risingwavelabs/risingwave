package com.risingwave.planner;

import static com.risingwave.common.config.LeaderServerConfigurations.CLUSTER_MODE;
import static com.risingwave.common.config.LeaderServerConfigurations.ClusterMode.Distributed;

import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SubQueryPlannerTest extends BatchPlanTestBase {
  @BeforeAll
  public void initAll() {
    super.init();
    var conf = executionContext.getConf();
    conf.set(CLUSTER_MODE, Distributed);
  }

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("SubQuery plan tests")
  @ArgumentsSource(PlanTestCaseLoader.class)
  public void testSubQueryPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    runTestCase(testCase);
  }
}
