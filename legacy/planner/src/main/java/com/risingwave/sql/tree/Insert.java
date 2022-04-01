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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class Insert<T> extends Statement {

  private final Table<T> table;
  private final DuplicateKeyContext<T> duplicateKeyContext;
  private final List<String> columns;
  private final Query insertSource;
  private final List<SelectItem> returning;

  public Insert(
      Table<T> table,
      Query insertSource,
      List<String> columns,
      List<SelectItem> returning,
      DuplicateKeyContext<T> duplicateKeyContext) {
    this.table = table;
    this.columns = columns;
    this.insertSource = insertSource;
    this.duplicateKeyContext = duplicateKeyContext;
    this.returning = returning;
  }

  public Table<T> table() {
    return table;
  }

  public List<String> columns() {
    return columns;
  }

  public Query insertSource() {
    return insertSource;
  }

  public DuplicateKeyContext<T> duplicateKeyContext() {
    return duplicateKeyContext;
  }

  public List<SelectItem> returningClause() {
    return returning;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Insert<?> insert = (Insert<?>) o;
    return Objects.equals(table, insert.table)
        && Objects.equals(duplicateKeyContext, insert.duplicateKeyContext)
        && Objects.equals(columns, insert.columns)
        && Objects.equals(insertSource, insert.insertSource)
        && Objects.equals(returning, insert.returning);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, duplicateKeyContext, columns, insertSource, returning);
  }

  @Override
  public String toString() {
    return "Insert{"
        + "table="
        + table
        + ", duplicateKeyContext="
        + duplicateKeyContext
        + ", columns="
        + columns
        + ", insertSource="
        + insertSource
        + ", returning="
        + returning
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInsert(this, context);
  }

  public static class DuplicateKeyContext<T> {

    private static final DuplicateKeyContext<?> NONE =
        new DuplicateKeyContext<>(Type.NONE, Collections.emptyList(), Collections.emptyList());

    public static <T> DuplicateKeyContext<T> none() {
      return (DuplicateKeyContext<T>) NONE;
    }

    public enum Type {
      ON_CONFLICT_DO_UPDATE_SET,
      ON_CONFLICT_DO_NOTHING,
      NONE
    }

    private final Type type;
    private final List<Assignment<T>> onDuplicateKeyAssignments;
    private final List<T> constraintColumns;

    public DuplicateKeyContext(
        Type type, List<Assignment<T>> onDuplicateKeyAssignments, List<T> constraintColumns) {
      this.type = type;
      this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
      this.constraintColumns = constraintColumns;
    }

    public Type getType() {
      return type;
    }

    public List<Assignment<T>> getAssignments() {
      return onDuplicateKeyAssignments;
    }

    public List<T> getConstraintColumns() {
      return constraintColumns;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DuplicateKeyContext<?> that = (DuplicateKeyContext<?>) o;
      return type == that.type
          && Objects.equals(onDuplicateKeyAssignments, that.onDuplicateKeyAssignments)
          && Objects.equals(constraintColumns, that.constraintColumns);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, onDuplicateKeyAssignments, constraintColumns);
    }

    @Override
    public String toString() {
      return "DuplicateKeyContext{"
          + "type="
          + type
          + ", onDuplicateKeyAssignments="
          + onDuplicateKeyAssignments
          + ", constraintColumns="
          + constraintColumns
          + '}';
    }
  }
}
