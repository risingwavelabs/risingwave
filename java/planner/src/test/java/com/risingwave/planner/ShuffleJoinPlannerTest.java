package com.risingwave.planner;

import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Contains query plan for join plan contains hash shuffle, e.g. sort merge join, hash join. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ShuffleJoinPlannerTest extends BatchPlanTestBase {

  @BeforeAll
  public void initAll() {
    super.init();
  }

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("Shuffle join plan tests")
  @ArgumentsSource(PlanTestCaseLoader.class)
  public void testShuffleJoinPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    runTestCase(testCase);
  }
}
