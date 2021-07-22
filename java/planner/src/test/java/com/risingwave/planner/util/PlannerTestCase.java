package com.risingwave.planner.util;

public class PlannerTestCase {
  private final String name;
  private final String sql;
  private final String plan;

  public PlannerTestCase(String name, String sql, String plan) {
    this.name = name;
    this.sql = sql;
    this.plan = plan;
  }

  public String getName() {
    return name;
  }

  public String getSql() {
    return sql;
  }

  public String getPlan() {
    return plan;
  }

  @Override
  public String toString() {
    return name;
  }
}
