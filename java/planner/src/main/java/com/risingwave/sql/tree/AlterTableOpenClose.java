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

public class AlterTableOpenClose<T> extends Statement {

  private final Table<T> table;
  private final boolean blob;
  private final boolean openTable;

  public AlterTableOpenClose(Table<T> table, boolean blob, boolean openTable) {
    this.table = table;
    this.blob = blob;
    this.openTable = openTable;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAlterTableOpenClose(this, context);
  }

  public Table<T> table() {
    return table;
  }

  public boolean blob() {
    return blob;
  }

  public boolean openTable() {
    return openTable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlterTableOpenClose<?> that = (AlterTableOpenClose<?>) o;
    return blob == that.blob && openTable == that.openTable && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, blob, openTable);
  }

  @Override
  public String toString() {
    return "AlterTableOpenClose{"
        + "table="
        + table
        + ", blob="
        + blob
        + ", open table="
        + openTable
        + '}';
  }
}
