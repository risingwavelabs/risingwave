package com.risingwave.planner;

import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Tests for streaming job */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StreamAggPlannerDistributedModeTest extends StreamPlanTestBase {
  @BeforeAll
  public void initAll() {
    super.init();
  }

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("Streaming Agg Plan in Distributed Mode tests")
  @ArgumentsSource(PlanTestCaseLoader.class)
  @Order(0)
  public void testStreamingPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    runTestCase(testCase);
  }
}
