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

public class AlterTableReroute<T> extends Statement {

  private final Table<T> table;
  private final RerouteOption<T> rerouteOption;

  public AlterTableReroute(Table<T> table, RerouteOption<T> rerouteOption) {
    this.table = table;
    this.rerouteOption = rerouteOption;
  }

  public Table<T> table() {
    return table;
  }

  public RerouteOption<T> rerouteOption() {
    return rerouteOption;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlterTableReroute<?> that = (AlterTableReroute<?>) o;
    return Objects.equals(table, that.table) && Objects.equals(rerouteOption, that.rerouteOption);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, rerouteOption);
  }

  @Override
  public String toString() {
    return "AlterTableReroute{" + "table=" + table + ", reroute option=" + rerouteOption + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAlterTableReroute(this, context);
  }
}
