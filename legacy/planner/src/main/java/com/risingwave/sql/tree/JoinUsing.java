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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JoinUsing extends JoinCriteria {

  private static QualifiedName extendQualifiedName(QualifiedName name, String ext) {
    List<String> parts = new ArrayList<>(name.getParts().size() + 1);
    parts.addAll(name.getParts());
    parts.add(ext);
    return new QualifiedName(parts);
  }

  public static Expression toExpression(
      QualifiedName left, QualifiedName right, List<String> columns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("columns must not be empty");
    }
    List<ComparisonExpression> comp =
        columns.stream()
            .map(
                col ->
                    new ComparisonExpression(
                        ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(extendQualifiedName(left, col)),
                        new QualifiedNameReference((extendQualifiedName(right, col)))))
            .collect(Collectors.toList());
    if (1 == comp.size()) {
      return comp.get(0);
    }
    Expression expr = null;
    for (int i = comp.size() - 2; i >= 0; i--) {
      if (null == expr) {
        expr =
            new LogicalBinaryExpression(
                LogicalBinaryExpression.Type.AND, comp.get(i), comp.get(i + 1));
      } else {
        expr = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, comp.get(i), expr);
      }
    }
    return expr;
  }

  private final List<String> columns;

  public JoinUsing(List<String> columns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("columns must not be empty");
    }
    this.columns = new ArrayList<>(columns);
  }

  public List<String> getColumns() {
    return columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinUsing joinUsing = (JoinUsing) o;
    return Objects.equals(columns, joinUsing.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }

  @Override
  public String toString() {
    return "JoinUsing{" + "columns=" + columns + '}';
  }
}
