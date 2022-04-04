package com.risingwave.planner.tpch;

import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.planner.BatchPlanTestBase;
import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TpchDistributedQueryPlanTest extends BatchPlanTestBase {
  @BeforeAll
  public void initAll() {
    super.init();
    executionContext
        .getConf()
        .set(
            LeaderServerConfigurations.CLUSTER_MODE,
            LeaderServerConfigurations.ClusterMode.Distributed);
  }

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("TPCH distributed plan tests")
  @ArgumentsSource(PlanTestCaseLoader.class)
  public void testTpchPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    runTestCase(testCase);
  }
}
