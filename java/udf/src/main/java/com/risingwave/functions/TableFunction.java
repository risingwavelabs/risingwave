// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.functions;

/**
 * Base interface for a user-defined table function. A user-defined table
 * function maps zero, one, or multiple scalar values to zero, one, or multiple
 * rows (or structured types). If an output record consists of only one field,
 * the structured record can be omitted, and a scalar value can be emitted that
 * will be implicitly wrapped into a row by the runtime.
 *
 * <p>
 * The behavior of a {@link TableFunction} can be defined by implementing a
 * custom evaluation method. An evaluation method must be declared publicly, not
 * static, and named <code>eval</code>. The return type must be an Iterator.
 * Multiple overloaded methods named <code>eval</code> are not supported yet.
 *
 * <p>
 * By default, input and output data types are automatically extracted using
 * reflection.
 *
 * <p>
 * The following examples show how to specify a table function:
 *
 * <pre>
 * {@code
 * // a function that accepts an INT arguments and emits the range from 0 to the
 * // given number.
 * class Series implements TableFunction {
 *     public Iterator<Integer> eval(int n) {
 *         return java.util.stream.IntStream.range(0, n).iterator();
 *     }
 * }
 * 
 * // a function that accepts an String arguments and emits the words of the
 * // given string.
 * class Split implements TableFunction {
 *     public static class Row {
 *         public String word;
 *         public int length;
 *     }
 * 
 *     public Iterator<Row> eval(String str) {
 *         return Stream.of(str.split(" ")).map(s -> {
 *             Row row = new Row();
 *             row.word = s;
 *             row.length = s.length();
 *             return row;
 *         }).iterator();
 *     }
 * }
 * }</pre>
 */
public interface TableFunction extends UserDefinedFunction {
}
