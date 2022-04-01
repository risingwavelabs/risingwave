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

public class DropCheckConstraint<T> extends Statement {

  private final Table<T> table;
  private final String name;

  public DropCheckConstraint(Table<T> table, String name) {
    this.table = table;
    this.name = name;
  }

  public String name() {
    return name;
  }

  public Table<T> table() {
    return table;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropCheckConstraint(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, name);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (false == obj instanceof DropCheckConstraint) {
      return false;
    }
    DropCheckConstraint<T> that = (DropCheckConstraint<T>) obj;
    return Objects.equals(name, that.name) && Objects.equals(table, that.table);
  }

  @Override
  public String toString() {
    return "DropCheckConstraint{" + "table=" + table + ", name='" + name + '\'' + '}';
  }
}
