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

import java.util.Objects;

public class CopyFrom<T> extends Statement {

  private final Table<T> table;
  private final T path;
  private final GenericProperties<T> properties;
  private final boolean returnSummary;

  public CopyFrom(Table<T> table, T path, GenericProperties<T> properties, boolean returnSummary) {
    this.table = table;
    this.path = path;
    this.properties = properties;
    this.returnSummary = returnSummary;
  }

  public Table<T> table() {
    return table;
  }

  public T path() {
    return path;
  }

  public GenericProperties<T> properties() {
    return properties;
  }

  public boolean isReturnSummary() {
    return returnSummary;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CopyFrom<?> copyFrom = (CopyFrom<?>) o;
    return returnSummary == copyFrom.returnSummary
        && Objects.equals(table, copyFrom.table)
        && Objects.equals(path, copyFrom.path)
        && Objects.equals(properties, copyFrom.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, path, properties, returnSummary);
  }

  @Override
  public String toString() {
    return "CopyFrom{"
        + "table="
        + table
        + ", path="
        + path
        + ", properties="
        + properties
        + ", returnSummary="
        + returnSummary
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCopyFrom(this, context);
  }
}
