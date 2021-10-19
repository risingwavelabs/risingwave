package com.risingwave.planner.util;

import java.util.Optional;

/** Abstraction for a single test case. Load by TestCase Loader. */
public class PlannerTestCase {
  private final String name;
  private final String sql;
  private final String plan;
  private final Optional<String> json;

  public PlannerTestCase(String name, String sql, String plan, String json) {
    this.name = name;
    this.sql = sql;
    this.plan = plan;
    this.json = Optional.ofNullable(json);
  }

  public String getSql() {
    return sql;
  }

  public String getPlan() {
    return plan;
  }

  public Optional<String> getJson() {
    return json;
  }

  @Override
  public String toString() {
    return name;
  }
}
