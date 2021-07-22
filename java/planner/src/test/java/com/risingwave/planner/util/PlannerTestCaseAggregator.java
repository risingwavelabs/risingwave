package com.risingwave.planner.util;

import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.aggregator.ArgumentsAggregationException;
import org.junit.jupiter.params.aggregator.ArgumentsAggregator;

public class PlannerTestCaseAggregator implements ArgumentsAggregator {
  @Override
  public PlannerTestCase aggregateArguments(ArgumentsAccessor accessor, ParameterContext context)
      throws ArgumentsAggregationException {
    return accessor.get(0, PlannerTestCase.class);
  }
}
