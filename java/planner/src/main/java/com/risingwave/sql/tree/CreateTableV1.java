/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package com.risingwave.sql.tree;

import java.util.List;
import java.util.Optional;

/** Planner node for CREATE TABLE_V1 statement. */
public class CreateTableV1<T> extends Statement {

  private final Table<T> name;
  private final List<TableElement<T>> tableElements;
  private final Optional<PartitionedBy<T>> partitionedBy;
  private final Optional<ClusteredBy<T>> clusteredBy;
  private final boolean ifNotExists;
  private final GenericProperties<T> properties;

  public CreateTableV1(
      Table<T> name,
      List<TableElement<T>> tableElements,
      Optional<PartitionedBy<T>> partitionedBy,
      Optional<ClusteredBy<T>> clusteredBy,
      GenericProperties<T> genericProperties,
      boolean ifNotExists) {
    this.name = name;
    this.tableElements = tableElements;
    this.partitionedBy = partitionedBy;
    this.clusteredBy = clusteredBy;
    this.ifNotExists = ifNotExists;
    this.properties = genericProperties;
  }

  public boolean ifNotExists() {
    return ifNotExists;
  }

  public Table<T> name() {
    return name;
  }

  public List<TableElement<T>> tableElements() {
    return tableElements;
  }

  public Optional<ClusteredBy<T>> clusteredBy() {
    return clusteredBy;
  }

  public Optional<PartitionedBy<T>> partitionedBy() {
    return partitionedBy;
  }

  public GenericProperties<T> properties() {
    return properties;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTableV1(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CreateTableV1 that = (CreateTableV1) o;

    if (ifNotExists != that.ifNotExists) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    if (!tableElements.equals(that.tableElements)) {
      return false;
    }
    if (!partitionedBy.equals(that.partitionedBy)) {
      return false;
    }
    if (!clusteredBy.equals(that.clusteredBy)) {
      return false;
    }
    return properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + tableElements.hashCode();
    result = 31 * result + partitionedBy.hashCode();
    result = 31 * result + clusteredBy.hashCode();
    result = 31 * result + (ifNotExists ? 1 : 0);
    result = 31 * result + properties.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "CreateTableV1{"
        + "name="
        + name
        + ", tableElements="
        + tableElements
        + ", partitionedBy="
        + partitionedBy
        + ", clusteredBy="
        + clusteredBy
        + ", ifNotExists="
        + ifNotExists
        + ", properties="
        + properties
        + '}';
  }
}
