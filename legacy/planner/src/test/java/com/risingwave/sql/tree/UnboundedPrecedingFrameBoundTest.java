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

import static com.risingwave.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.risingwave.sql.tree.WindowFrame.Mode.RANGE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;
import java.util.List;
import org.junit.Before;
import org.junit.jupiter.api.Test;

public class UnboundedPrecedingFrameBoundTest {

  private List<Integer> partition;
  private Comparator<Integer> intComparator;

  @Before
  public void setupPartitionAndComparator() {
    intComparator = Comparator.comparing(x -> x);
    partition = List.of(1, 2, 2);
  }

  @Test
  public void testStartForFirstFrame() {
    int end = UNBOUNDED_PRECEDING.getStart(RANGE, 0, 3, 1, null, null, intComparator, partition);
    assertThat(
        "the start boundary should always be "
            + "the start of the partition for the UNBOUNDED PRECEDING frames",
        end,
        is(0));
  }

  @Test
  public void testStartForSecondFrame() {
    int end = UNBOUNDED_PRECEDING.getStart(RANGE, 0, 3, 2, null, null, intComparator, partition);
    assertThat(
        "the start boundary should always be "
            + "the start of the partition for the UNBOUNDED PRECEDING frames",
        end,
        is(0));
  }

  @Test
  public void testUnboundePrecedingCannotBeTheEndOfTheFrame() {
    assertThrows(
        IllegalStateException.class,
        () -> UNBOUNDED_PRECEDING.getEnd(RANGE, 0, 3, 1, null, null, intComparator, partition),
        "UNBOUNDED PRECEDING cannot be the start of a frame");
  }
}
