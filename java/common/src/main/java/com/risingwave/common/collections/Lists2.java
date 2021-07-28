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

package com.risingwave.common.collections;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public final class Lists2 {

  private Lists2() {}

  /** Create a new list that contains the elements of both arguments */
  public static <T> List<T> concat(Collection<? extends T> list1, Collection<? extends T> list2) {
    ArrayList<T> list = new ArrayList<>(list1.size() + list2.size());
    list.addAll(list1);
    list.addAll(list2);
    return list;
  }

  public static <T> List<T> concat(Collection<? extends T> list1, T item) {
    ArrayList<T> xs = new ArrayList<>(list1.size() + 1);
    xs.addAll(list1);
    xs.add(item);
    return xs;
  }

  public static <T> List<T> concatUnique(List<? extends T> list1, Collection<? extends T> list2) {
    List<T> result = new ArrayList<>(list1.size() + list2.size());
    result.addAll(list1);
    for (T item : list2) {
      if (!list1.contains(item)) {
        result.add(item);
      }
    }
    return result;
  }

  /**
   * /** Create a copy of the given list with {@code mapper} applied on each item. Opposed to {@link
   * java.util.stream.Stream#map(Function)} / {@link Collectors#toList()} this minimizes
   * allocations.
   */
  public static <I, O> List<O> map(Collection<I> list, Function<? super I, ? extends O> mapper) {
    if (list.isEmpty()) {
      return Collections.emptyList();
    }
    ArrayList<O> copy = new ArrayList<>(list.size());
    for (I item : list) {
      copy.add(mapper.apply(item));
    }
    return copy;
  }

  /**
   * Like `map` but ensures that the same list is returned if no elements changed. But is a view
   * onto the original list and applies the mapper lazy on a need basis.
   */
  public static <I, O> List<O> mapLazy(List<I> list, Function<? super I, ? extends O> mapper) {
    return new LazyMapList<>(list, mapper);
  }

  /** Like `map` but ensures that the same list is returned if no elements changed */
  public static <T> List<T> mapIfChange(List<T> list, Function<? super T, ? extends T> mapper) {
    if (list.isEmpty()) {
      return list;
    }
    ArrayList<T> copy = new ArrayList<>(list.size());
    boolean changed = false;
    for (T item : list) {
      T mapped = mapper.apply(item);
      changed = changed || item != mapped;
      copy.add(mapped);
    }
    return changed ? copy : list;
  }

  /**
   * Return the first element of a list or raise an IllegalArgumentException if there are more than
   * 1 items.
   *
   * <p>Similar to Guava's com.google.common.collect.Iterables#getOnlyElement(Iterable), but avoids
   * an iterator allocation
   *
   * @throws NoSuchElementException If the list is empty
   * @throws IllegalArgumentException If the list has more than 1 element
   */
  public static <T> T getOnlyElement(List<T> items) {
    switch (items.size()) {
      case 0:
        throw new NoSuchElementException("List is empty");

      case 1:
        return items.get(0);

      default:
        throw new IllegalArgumentException("Expected 1 element, got: " + items.size());
    }
  }

  public static <O, I> List<O> mapTail(O head, List<I> tail, Function<I, O> mapper) {
    ArrayList<O> list = new ArrayList<>(tail.size() + 1);
    list.add(head);
    for (I input : tail) {
      list.add(mapper.apply(input));
    }
    return list;
  }

  /**
   * Finds the first non peer element in the provided list of items between the begin and end
   * indexes. Two items are peers if the provided comparator designates them as equals.
   *
   * @return the position of the first item that's not equal with the item on the `begin` index in
   *     the list of items.
   */
  public static <T> int findFirstNonPeer(
      List<T> items, int begin, int end, @Nullable Comparator<T> cmp) {
    if (cmp == null || (begin + 1) >= end) {
      return end;
    }
    T fst = items.get(begin);
    if (cmp.compare(fst, items.get(begin + 1)) != 0) {
      return begin + 1;
    }
    /*
     * Adapted binarySearch algorithm to find the first non peer (instead of the first match)
     * This depends on there being at least some EQ values;
     * Whenever we find a EQ pair we check if the following element isn't EQ anymore.
     *
     * E.g.
     *
     * i:     0  1  2  3  4  5  6  7
     * rows: [1, 1, 1, 1, 4, 4, 5, 6]
     *        ^ [1  1  1  4  4  5  6]
     *        +-----------^
     *           cmp: -1
     *        1 [1  1  1  4] 4  5  6
     *        ^     ^
     *        +-----+
     *           cmp: 0 --> cmp (mid +1) != 0 --> false
     *        1  1  1 [1  4] 4  5  6
     *        ^        ^
     *        +--------+
     *           cmp: 0 --> cmp (mid +1) != 0 --> true
     */
    int low = begin + 1;
    int high = end;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      T t = items.get(mid);
      int cmpResult = cmp.compare(fst, t);
      if (cmpResult == 0) {
        int next = mid + 1;
        if (next == high || cmp.compare(fst, items.get(next)) != 0) {
          return next;
        } else {
          low = next;
        }
      } else if (cmpResult < 0) {
        high = mid;
      } else {
        low = mid;
      }
    }
    return end;
  }

  /**
   * Finds the first peer, in order of appearance in the items list, of the item at the given index.
   * If the provided comparator is null this will return 0 (all items are peers when no comparator
   * is specified). If the provided item has no peers amongst the items that appear before it, or if
   * it is the first item in the list, this will return the itemIdx.
   */
  public static <T> int findFirstPreviousPeer(
      List<T> items, int itemIdx, @Nullable Comparator<T> cmp) {
    if (cmp == null) {
      return 0;
    }

    int firstPeer = itemIdx;
    T item = items.get(itemIdx);
    for (int i = itemIdx - 1; i >= 0; i--) {
      if (cmp.compare(item, items.get(i)) == 0) {
        firstPeer = i;
      } else {
        break;
      }
    }
    return firstPeer;
  }

  /**
   * Finds the first item that's less than or equal to the probe in the slice of the sortedItems
   * that starts with the index specified by @param itemIdx, according to the provided comparator.
   *
   * @return the index of the first LTE item, or -1 if there isn't any (eg. probe is less than all
   *     items)
   */
  public static <T> int findFirstLteProbeValue(
      List<T> sortedItems, int upperBoundary, int itemIdx, T probe, Comparator<T> cmp) {
    int start = itemIdx;
    int end = upperBoundary - 1;

    int firstLteProbeIdx = -1;
    while (start <= end) {
      int mid = (start + end) >>> 1;
      // Move to left side if mid is greater than probe
      if (cmp.compare(sortedItems.get(mid), probe) > 0) {
        end = mid - 1;
      } else {
        firstLteProbeIdx = mid;
        start = mid + 1;
      }
    }
    return firstLteProbeIdx;
  }

  /**
   * Finds the first item that's greater than or equal to the probe in the slice of the sortedItems
   * that ends with the index specified by @param itemIdx, according to the provided comparator.
   *
   * @return the index of the first GTE item, or -1 if there isn't any (eg. probe is greater than
   *     all items)
   */
  public static <T> int findFirstGteProbeValue(
      List<T> sortedItems, int lowerBoundary, int itemIdx, T probe, Comparator<T> cmp) {
    int start = lowerBoundary;
    int end = itemIdx - 1;

    int firstGteProbeIdx = -1;
    while (start <= end) {
      int mid = (start + end) >>> 1;
      // Move to right side if mid is less than probe
      if (cmp.compare(sortedItems.get(mid), probe) < 0) {
        start = mid + 1;
      } else {
        firstGteProbeIdx = mid;
        end = mid - 1;
      }
    }
    return firstGteProbeIdx;
  }

  /**
   * Less garbage producing alternative to {@link java.util.stream.Stream#map(Function)} → {@link
   * java.util.stream.Stream#collect(Collector)} with a {@link Collectors#joining(CharSequence)}
   * collector.
   */
  public static <T> String joinOn(
      String delimiter, List<? extends T> items, Function<? super T, String> mapper) {
    StringJoiner joiner = new StringJoiner(delimiter);
    for (int i = 0; i < items.size(); i++) {
      joiner.add(mapper.apply(items.get(i)));
    }
    return joiner.toString();
  }

  /**
   * Same as {@link #joinOn(String, List, Function)} but uses iterator loop instead for random
   * access on List
   */
  public static <T> String joinOn(
      String delimiter, Iterable<? extends T> items, Function<? super T, String> mapper) {
    StringJoiner joiner = new StringJoiner(delimiter);
    for (T item : items) {
      joiner.add(mapper.apply(item));
    }
    return joiner.toString();
  }

  /**
   * {@code LazyMapList} is a wrapper around a list that lazily applies the {@code mapper} {@code
   * Function} on each item when it is accessed.
   */
  static class LazyMapList<I, O> extends AbstractList<O> implements RandomAccess {

    private final List<I> list;
    private final Function<? super I, ? extends O> mapper;

    LazyMapList(List<I> list, Function<? super I, ? extends O> mapper) {
      this.list = list;
      this.mapper = mapper;
    }

    @Override
    public O get(int index) {
      return mapper.apply(list.get(index));
    }

    @Override
    public int size() {
      return list.size();
    }
  }
}
