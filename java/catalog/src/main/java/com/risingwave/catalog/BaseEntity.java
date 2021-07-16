package com.risingwave.catalog;

abstract class BaseEntity {
  private final int id;
  private final String name;

  public BaseEntity(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }
}
