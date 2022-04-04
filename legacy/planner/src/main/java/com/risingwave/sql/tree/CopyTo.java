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
import java.util.Objects;
import java.util.Optional;

public class CopyTo<T> extends Statement {

  private final Table<T> table;
  private final List<T> columns;
  private final Optional<T> whereClause;
  private final boolean directoryUri;
  private final T targetUri;
  private final GenericProperties<T> properties;

  public CopyTo(
      Table<T> table,
      List<T> columns,
      Optional<T> whereClause,
      boolean directoryUri,
      T targetUri,
      GenericProperties<T> properties) {

    this.table = table;
    this.directoryUri = directoryUri;
    this.targetUri = targetUri;
    this.properties = properties;
    this.columns = columns;
    this.whereClause = whereClause;
  }

  public Table<T> table() {
    return table;
  }

  public boolean directoryUri() {
    return directoryUri;
  }

  public T targetUri() {
    return targetUri;
  }

  public List<T> columns() {
    return columns;
  }

  public GenericProperties<T> properties() {
    return properties;
  }

  public Optional<T> whereClause() {
    return whereClause;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CopyTo<?> copyTo = (CopyTo<?>) o;
    return directoryUri == copyTo.directoryUri
        && Objects.equals(table, copyTo.table)
        && Objects.equals(targetUri, copyTo.targetUri)
        && Objects.equals(properties, copyTo.properties)
        && Objects.equals(columns, copyTo.columns)
        && Objects.equals(whereClause, copyTo.whereClause);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, directoryUri, targetUri, properties, columns, whereClause);
  }

  @Override
  public String toString() {
    return "CopyTo{"
        + "table="
        + table
        + ", directoryUri="
        + directoryUri
        + ", targetUri="
        + targetUri
        + ", properties="
        + properties
        + ", columns="
        + columns
        + ", whereClause="
        + whereClause
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCopyTo(this, context);
  }
}
