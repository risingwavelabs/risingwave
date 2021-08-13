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

package com.risingwave.sql.parser;

import static com.risingwave.sql.SqlFormatter.formatSql;
import static com.risingwave.sql.parser.SqlParser.createStatement;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.risingwave.common.collections.Lists2;
import com.risingwave.sql.tree.DefaultTraversalVisitor;
import com.risingwave.sql.tree.Node;
import com.risingwave.sql.tree.Statement;
import java.util.ArrayList;
import java.util.List;

final class TreeAssertions {
  private TreeAssertions() {}

  static void assertFormattedSql(Node expected) {
    String formatted = formatSql(expected);

    // verify round-trip of formatting already-formatted SQL
    Statement actual = parseFormatted(formatted, expected);
    assertEquals(formatSql(actual), formatted);

    // compare parsed tree with parsed tree of formatted SQL
    if (!actual.equals(expected)) {
      // simplify finding the non-equal part of the tree
      assertListEquals(linearizeTree(actual), linearizeTree(expected));
    }
    assertEquals(actual, expected);
  }

  private static Statement parseFormatted(String sql, Node tree) {
    try {
      return createStatement(sql);
    } catch (ParsingException e) {
      throw new AssertionError(
          format(
              "failed to parse formatted SQL: %s\nerror: %s\ntree: %s", sql, e.getMessage(), tree));
    }
  }

  private static List<Node> linearizeTree(Node tree) {
    ArrayList<Node> nodes = new ArrayList<>();
    new DefaultTraversalVisitor<Node, Void>() {
      void process(Node node) {
        node.accept(this, null);
        nodes.add(node);
      }
    }.process(tree);
    return nodes;
  }

  private static <T> void assertListEquals(List<T> actual, List<T> expected) {
    if (actual.size() != expected.size()) {
      fail(
          format(
              "Lists not equal%nActual [%s]:%n    %s%nExpected [%s]:%n    %s",
              actual.size(),
              Lists2.joinOn("\n    ", actual, Object::toString),
              expected.size(),
              Lists2.joinOn("\n    ", expected, Object::toString)));
    }
    assertEquals(actual, expected);
  }
}
