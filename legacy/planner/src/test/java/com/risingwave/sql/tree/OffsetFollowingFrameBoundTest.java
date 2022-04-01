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

import static com.risingwave.sql.tree.FrameBound.Type.FOLLOWING;
import static com.risingwave.sql.tree.WindowFrame.Mode.RANGE;
import static com.risingwave.sql.tree.WindowFrame.Mode.ROWS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OffsetFollowingFrameBoundTest {

  private Comparator<Integer> intComparator;
  private List<Integer> partition;

  @BeforeEach
  public void setupPartitionAndComparator() {
    intComparator = Comparator.comparing(x -> x);
    partition = List.of(1, 2, 3, 6, 7);
  }

  @Test
  public void test_following_end_in_range_mode() {
    int frameStart = FOLLOWING.getEnd(RANGE, 1, 5, 1, 2, 4, intComparator, partition);
    assertThat(frameStart, is(3));
  }

  @Test
  public void test_following_end_in_rows_mode() {
    int frameStart = FOLLOWING.getEnd(ROWS, 1, 5, 1, 2L, null, intComparator, partition);
    assertThat(frameStart, is(4));
  }
}
