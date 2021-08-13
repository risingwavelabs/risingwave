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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.risingwave.sql.parser.SqlParser;
import com.risingwave.sql.tree.IntegerLiteral;
import org.junit.jupiter.api.Test;

public class LiteralsTest {

  @Test
  public void testEscape() throws Exception {
    assertThat(Literals.escapeStringLiteral(""), is(""));
    assertThat(Literals.escapeStringLiteral("foobar"), is("foobar"));
    assertThat(Literals.escapeStringLiteral("'"), is("''"));
    assertThat(Literals.escapeStringLiteral("''"), is("''''"));
    assertThat(Literals.escapeStringLiteral("'fooBar'"), is("''fooBar''"));
  }

  @Test
  public void testQuote() throws Exception {
    assertThat(Literals.quoteStringLiteral(""), is("''"));
    assertThat(Literals.quoteStringLiteral("foobar"), is("'foobar'"));
    assertThat(Literals.quoteStringLiteral("'"), is("''''"));
    assertThat(Literals.quoteStringLiteral("''"), is("''''''"));
    assertThat(Literals.quoteStringLiteral("'fooBar'"), is("'''fooBar'''"));
  }

  @Test
  public void testThatNoEscapedCharsAreNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars(""), is(""));
    assertThat(Literals.replaceEscapedChars("Hello World"), is("Hello World"));
  }

  // Single escaped chars supported

  @Test
  public void testThatEscapedTabLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\t"), is("\t"));
  }

  @Test
  public void testThatEscapedTabInMiddleOfLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("Hello\\tWorld"), is("Hello\tWorld"));
  }

  @Test
  public void testThatEscapedTabAtBeginningOfLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\tHelloWorld"), is("\tHelloWorld"));
  }

  @Test
  public void testThatEscapedTabAtEndOfLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("HelloWorld\\t"), is("HelloWorld\t"));
  }

  @Test
  public void testThatEscapedBackspaceInTheMiddleOfLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("Hello\\bWorld"), is("Hello\bWorld"));
  }

  @Test
  public void testThatEscapedFormFeedInTheMiddleOfLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("Hello\\fWorld"), is("Hello\fWorld"));
  }

  @Test
  public void testThatEscapedNewLineInTheMiddleOfLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("Hello\\nWorld"), is("Hello\nWorld"));
  }

  @Test
  public void testThatCarriageReturnInTheMiddleOfLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("Hello\\rWorld"), is("Hello\rWorld"));
  }

  @Test
  public void testThatMultipleConsecutiveSingleEscapedCharsAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\t\\n\\f"), is("\t\n\f"));
  }

  // Invalid escaped literals

  @Test
  public void testThatCharEscapeWithoutAnySequenceIsNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\"), is("\\"));
  }

  @Test
  public void testThatEscapedBackslashCharIsReplacedAndNotConsideredAsEscapeChar()
      throws Exception {
    assertThat(Literals.replaceEscapedChars("\\\\141"), is("\\141"));
  }

  @Test
  public void testThatEscapedQuoteCharIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\'141"), is("'141"));
  }

  @Test
  public void testThatInvalidEscapeSequenceIsNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\s"), is("\\s"));
  }

  @Test
  public void testThatInvalidEscapeSequenceAtBeginningOfLiteralIsNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\shello"), is("\\shello"));
  }

  @Test
  public void testThatInvalidEscapeSequenceAtEndOfLiteralIsNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("hello\\s"), is("hello\\s"));
  }

  // Octal Byte Values

  @Test
  public void testThatEscapedOctalValueLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\141"), is("a"));
  }

  @Test
  public void testThatMultipleConsecutiveEscapedOctalValuesAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\141\\141\\141"), is("aaa"));
  }

  @Test
  public void testThatDigitFollowingEscapedOctalValueIsNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\1411"), is("a1"));
  }

  @Test
  public void testThatSingleDigitEscapedOctalValueIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\0"), is("\u0000"));
  }

  @Test
  public void testThatDoubleDigitEscapedOctalValueIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\07"), is("\u0007"));
  }

  // Hexadecimal Byte Values

  @Test
  public void testThatEscapedHexValueLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\x61"), is("a"));
  }

  @Test
  public void testThatMultipleConsecutiveEscapedHexValueLiteralAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\x61\\x61\\x61"), is("aaa"));
  }

  @Test
  public void testThatMultipleNonConsecutiveEscapedHexValueLiteralAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\x61 \\x61"), is("a a"));
  }

  @Test
  public void testThatDigitsFollowingEscapedHexValueLiteralAreNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\x610000"), is("a0000"));
  }

  @Test
  public void testThatSingleDigitEscapedHexValueLiteralIsReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\xDg0000"), is("\rg0000"));
  }

  @Test
  public void testThatEscapedHexValueInTheMiddleOfTheLiteralAreReplacedIsReplaced()
      throws Exception {
    assertThat(
        Literals.replaceEscapedChars("What \\x61 wonderful world"), is("What a wonderful world"));
  }

  @Test
  public void testThatInvalidEscapedHexLiteralsAreNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\x\\x"), is("xx"));
  }

  // 16-bit Unicode Character Values

  @Test
  public void testThatEscaped16BitUnicodeCharsAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\u0061"), is("a"));
  }

  @Test
  public void testThatMultipleConsecutiveEscaped16BitUnicodeCharsAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\u0061\\u0061\\u0061"), is("aaa"));
  }

  @Test
  public void testThatMultipleNonConsecutiveEscaped16BitUnicodeCharsAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\u0061 \\u0061"), is("a a"));
  }

  @Test
  public void testThatDigitsFollowingEscaped16BitUnicodeCharsAreNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\u00610000"), is("a0000"));
  }

  @Test
  public void testThatEscaped16BitUnicodeCharsInTheMiddleOfTheLiteralAreReplaced()
      throws Exception {
    assertThat(
        Literals.replaceEscapedChars("What \\u0061 wonderful world"), is("What a wonderful world"));
  }

  @Test
  public void testThatInvalidLengthEscapedUnicode16SequenceThrowsException() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> Literals.replaceEscapedChars("\\u006"),
        Literals.ESCAPED_UNICODE_ERROR);
  }

  @Test
  public void testThatInvalidHexEscapedUnicode16SequenceThrowsException() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> Literals.replaceEscapedChars("\\u006G"),
        Literals.ESCAPED_UNICODE_ERROR);
  }

  @Test
  public void testThatEscaped32BitUnicodeCharsAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\U00000061"), is("a"));
  }

  @Test
  public void testThatMultipleConsecutiveEscaped32BitUnicodeCharsAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\U00000061\\U00000061\\U00000061"), is("aaa"));
  }

  @Test
  public void testThatMultipleNonConsecutiveEscaped32BitUnicodeCharsAreReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\U00000061 \\U00000061"), is("a a"));
  }

  @Test
  public void testThatDigitsFollowingEscaped32BitUnicodeCharsAreNotReplaced() throws Exception {
    assertThat(Literals.replaceEscapedChars("\\U000000610000"), is("a0000"));
  }

  @Test
  public void testThatEscaped32BitUnicodeCharsInTheMiddleOfTheLiteralAreReplaced()
      throws Exception {
    assertThat(
        Literals.replaceEscapedChars("What \\U00000061 wonderful world"),
        is("What a wonderful world"));
  }

  @Test
  public void testThatInvalidLengthEscapedUnicode32SequenceThrowsException() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> Literals.replaceEscapedChars("\\U0061"),
        Literals.ESCAPED_UNICODE_ERROR);
  }

  @Test
  public void testThatInvalidHexEscapedUnicode32SequenceThrowsException() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> Literals.replaceEscapedChars("\\U0000006G"),
        Literals.ESCAPED_UNICODE_ERROR);
  }

  @Test
  public void test_integer_literal() {
    var literal = SqlParser.createExpression("2147483647");
    assertThat(literal, is(new IntegerLiteral(Integer.MAX_VALUE)));
  }
}
