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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class JoinUsingTests {

  @Test
  public void testToExpression() {
    for (int n : new int[] {1, 7}) {

      List<String> cols = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        cols.add("col_" + i);
      }
      QualifiedName left = QualifiedName.of("doc", "t1");
      QualifiedName right = QualifiedName.of("doc", "t2");
      Expression expr = JoinUsing.toExpression(left, right, cols);
      Expression e = expr;
      for (int i = 0; i < n - 2; i++) {
        assertTrue(e instanceof LogicalBinaryExpression);
        LogicalBinaryExpression and = (LogicalBinaryExpression) e;
        assertTrue(and.getLeft() instanceof ComparisonExpression);
        assertTrue(and.getRight() instanceof LogicalBinaryExpression);
        e = and.getRight();
      }
      if (n > 0) {
        if (1 == n) {
          assertTrue(e instanceof ComparisonExpression);
        } else {
          assertTrue(e instanceof LogicalBinaryExpression);
          LogicalBinaryExpression and = (LogicalBinaryExpression) e;
          assertTrue(and.getLeft() instanceof ComparisonExpression);
          assertTrue(and.getRight() instanceof ComparisonExpression);
        }
      }
    }
  }
}
