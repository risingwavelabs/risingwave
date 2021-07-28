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

package com.risingwave.sql;

import java.util.function.Predicate;

public class Literals {

  private static final String EPSILON = "E";

  static final String ESCAPED_UNICODE_ERROR =
      "Invalid Unicode escape (must be \\uXXXX or \\UXXXXXXXX)";

  public static String quoteStringLiteral(String literal) {
    return "'" + escapeStringLiteral(literal) + "'";
  }

  public static String quoteEscapedStringLiteral(String literal) {
    return EPSILON + "'" + literal + "'";
  }

  static String escapeStringLiteral(String literal) {
    return literal.replace("'", "''");
  }

  public static String replaceEscapedChars(String input) {
    int length = input.length();
    if (input.length() <= 1) {
      return input;
    }

    StringBuilder builder = new StringBuilder(length);
    int endIdx;
    for (int i = 0; i < length; i++) {
      char currentChar = input.charAt(i);
      if (currentChar == '\\' && i + 1 < length) {
        char nextChar = input.charAt(i + 1);
        switch (nextChar) {
          case 'b':
            builder.append('\b');
            i++;
            break;
          case 'f':
            builder.append('\f');
            i++;
            break;
          case 'n':
            builder.append('\n');
            i++;
            break;
          case 'r':
            builder.append('\r');
            i++;
            break;
          case 't':
            builder.append('\t');
            i++;
            break;
          case '\\':
          case '\'':
            builder.append(nextChar);
            i++;
            break;
          case 'u':
          case 'U':
            // handle unicode case
            final int charsToConsume = (nextChar == 'u') ? 4 : 8;
            if (i + 1 + charsToConsume >= length) {
              throw new IllegalArgumentException(ESCAPED_UNICODE_ERROR);
            }
            endIdx =
                calculateMaxCharsInSequence(input, i + 2, charsToConsume, Literals::isHexDigit);
            if (endIdx != i + 2 + charsToConsume) {
              throw new IllegalArgumentException(ESCAPED_UNICODE_ERROR);
            }
            builder.appendCodePoint(Integer.parseInt(input.substring(i + 2, endIdx), 16));
            i = endIdx - 1; // skip already consumed chars
            break;
          case 'x':
            // handle hex byte case - up to 2 chars for hex value
            endIdx = calculateMaxCharsInSequence(input, i + 2, 2, Literals::isHexDigit);
            if (endIdx > i + 2) {
              builder.appendCodePoint(Integer.parseInt(input.substring(i + 2, endIdx), 16));
              i = endIdx - 1; // skip already consumed chars
            } else {
              // hex sequence unmatched - output original char
              builder.append(nextChar);
              i++;
            }
            break;
          case '0':
          case '1':
          case '2':
          case '3':
            // handle octal case - up to 3 chars
            endIdx =
                calculateMaxCharsInSequence(
                    input,
                    i + 2,
                    2, // first char is already "consumed"
                    Literals::isOctalDigit);
            builder.appendCodePoint(Integer.parseInt(input.substring(i + 1, endIdx), 8));
            i = endIdx - 1; // skip already consumed chars
            break;
          default:
            // non-valid escaped char sequence
            builder.append(currentChar);
        }
      } else if (currentChar == '\'' && i > 1 && i + 1 < length) {
        // handle normal escaped quote: ''
        char nextChar = input.charAt(i + 1);
        if (nextChar == '\'') {
          builder.append(currentChar);
          i++;
        } else {
          throw new IllegalArgumentException("Invalid Escaped String Literal " + input);
        }
      } else {
        builder.append(currentChar);
      }
    }
    return builder.toString();
  }

  private static boolean isOctalDigit(final char ch) {
    return ch >= '0' && ch <= '7';
  }

  private static boolean isHexDigit(final char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f');
  }

  /**
   * Calculates the maximum number of consecutive characters of the {@link CharSequence} argument,
   * starting from {@code beginIndex}, that match a given {@link Predicate}. The number of
   * characters to match are either capped from the {@code maxCharsToMatch} parameter or the
   * sequence length.
   *
   * <p>Examples:
   *
   * <pre>{@code
   * calculateMaxCharsInSequence("12345", 0, 2, Character::isDigit) -> 2
   * calculateMaxCharsInSequence("12345", 3, 2, Character::isDigit) -> 5
   * calculateMaxCharsInSequence("12345", 4, 2, Character::isDigit) -> 5
   * }</pre>
   *
   * <p>This is used to identify the ending index of an Escaped Char Sequence {@see
   * Literals::replaceEscapedChars}
   *
   * @return the index of the first non-matching character
   */
  private static int calculateMaxCharsInSequence(
      CharSequence seq, int beginIndex, int maxCharsToMatch, Predicate<Character> predicate) {
    int idx = beginIndex;
    final int end = Math.min(seq.length(), beginIndex + maxCharsToMatch);
    while (idx < end && predicate.test(seq.charAt(idx))) {
      idx++;
    }
    return idx;
  }
}
