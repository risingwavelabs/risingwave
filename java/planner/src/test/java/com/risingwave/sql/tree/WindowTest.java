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

import static com.risingwave.sql.tree.FrameBound.Type.CURRENT_ROW;
import static com.risingwave.sql.tree.WindowFrame.Mode.RANGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class WindowTest {

  private final Window emptyWindow = new Window(null, List.of(), List.of(), Optional.empty());

  private final List<SortItem> orderBy =
      List.of(
          new SortItem(
              new QualifiedNameReference(QualifiedName.of("x")),
              SortItem.Ordering.ASCENDING,
              SortItem.NullOrdering.FIRST));

  private final List<Expression> partitionBy = List.of(Literal.fromObject(1));

  private final WindowFrame frame =
      new WindowFrame(RANGE, new FrameBound(CURRENT_ROW), Optional.empty());

  @Test
  public void test_merge_empty_current_and_provided_with_partition_by() {
    var provided = new Window("w", partitionBy, List.of(), Optional.empty());

    var newWindowDef = emptyWindow.merge(provided);
    assertThat(newWindowDef.getPartitions(), is(provided.getPartitions()));
  }

  @Test
  public void test_merge_current_with_frame_and_provided_with_order_by() {
    var current = new Window("w", List.of(), List.of(), Optional.of(frame));
    var provided = new Window(null, List.of(), orderBy, Optional.empty());

    var newWindowDef = current.merge(provided);
    assertThat(newWindowDef.getOrderBy(), is(orderBy));
    assertThat(newWindowDef.getWindowFrame(), is(Optional.of(frame)));
  }

  @Test
  public void test_merge_empty_current_and_provided_returns_provided_definition() {
    var provided = new Window("w", partitionBy, orderBy, Optional.empty());
    var newWindowDef = emptyWindow.merge(provided);
    assertThat(newWindowDef, is(provided));
  }

  @Test
  public void test_merge_provided_window_cannot_specify_frame() {
    var current = new Window("w", List.of(), orderBy, Optional.empty());
    var provided = new Window(null, List.of(), List.of(), Optional.of(frame));

    assertThrows(
        IllegalArgumentException.class,
        () -> current.merge(provided),
        "Cannot copy window w because it has a frame clause");
  }

  @Test
  public void test_merge_current_window_order_by_cannot_be_overwritten() {
    Window current = new Window("w", List.of(), orderBy, Optional.empty());
    Window provided = new Window(null, List.of(), orderBy, Optional.empty());

    assertThrows(
        IllegalArgumentException.class,
        () -> current.merge(provided),
        "Cannot override ORDER BY clause of window w");
  }

  @Test
  public void test_merge_current_window_cannot_specify_partition_by() {
    var current = new Window("w", partitionBy, List.of(), Optional.empty());

    assertThrows(
        IllegalArgumentException.class,
        () -> current.merge(emptyWindow),
        "Cannot override PARTITION BY clause of window w");
  }
}
