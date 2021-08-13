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
import static com.risingwave.sql.tree.QueryUtil.selectList;
import static com.risingwave.sql.tree.QueryUtil.table;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.risingwave.common.collections.Lists2;
import com.risingwave.sql.tree.Cast;
import com.risingwave.sql.tree.ColumnType;
import com.risingwave.sql.tree.CreateTable;
import com.risingwave.sql.tree.CurrentTime;
import com.risingwave.sql.tree.DoubleLiteral;
import com.risingwave.sql.tree.Expression;
import com.risingwave.sql.tree.FunctionCall;
import com.risingwave.sql.tree.Node;
import com.risingwave.sql.tree.ParameterExpression;
import com.risingwave.sql.tree.QualifiedName;
import com.risingwave.sql.tree.QualifiedNameReference;
import com.risingwave.sql.tree.Query;
import com.risingwave.sql.tree.QuerySpecification;
import com.risingwave.sql.tree.Statement;
import com.risingwave.sql.tree.StringLiteral;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class TestSqlParser {

  @Test
  public void testComments() {
    assertThat(
        SqlParser.createStatement("-- this is a line comment\nSelect 1"), instanceOf(Query.class));
    assertThat(
        SqlParser.createStatement("Select 1\n-- this is a line comment"), instanceOf(Query.class));
    assertThat(
        SqlParser.createStatement("-- this is a line comment\nSelect 1\n-- this is a line comment"),
        instanceOf(Query.class));
    assertThat(
        SqlParser.createStatement(
            "-- this is a line comment\nSelect \n-- this is a line comment\n1"),
        instanceOf(Query.class));
    assertThat(
        SqlParser.createStatement(
            "/* this\n" + "       is a multiline\n" + "       comment\n" + "    */\nSelect 1;"),
        instanceOf(Query.class));
    assertThat(
        SqlParser.createStatement(
            "Select 1;"
                + "    /* this\n"
                + "       is a multiline\n"
                + "       comment\n"
                + "    */"),
        instanceOf(Query.class));
    assertThat(
        SqlParser.createStatement(
            "Select"
                + "    /* this\n"
                + "       is a multiline\n"
                + "       comment\n"
                + "    */"
                + "1"),
        instanceOf(Query.class));
    assertThat(
        SqlParser.createStatement(
            "Select"
                + "    /* this\n"
                + "       is a multiline\n"
                + "       comment\n"
                + "    */\n"
                + "-- line comment\n"
                + "1"),
        instanceOf(Query.class));
    assertThat(
        SqlParser.createStatement(
            "CREATE TABLE IF NOT EXISTS \"doc\".\"data\" (\n"
                + "   \"week__generated\" "
                + "TIMESTAMP GENERATED ALWAYS AS date_trunc('week', \"ts\"),\n"
                + "   \"mid\" STRING, -- measurement id, mainly used for triggers, "
                + "starts for continuuous measurment with random uuid\n"
                + "   \"res\" INTEGER, -- resolution in ms\n"
                + "   \"ts\" TIMESTAMP,\n"
                + "   \"val_avg\" FLOAT,\n"
                + "   \"val_max\" FLOAT,\n"
                + "   \"val_min\" FLOAT,\n"
                + "   \"val_stddev\" FLOAT,\n"
                + "   \"vid\" STRING, -- variable id, unique uuid\n"
                + "   PRIMARY KEY (\"ts\", \"mid\", \"vid\", \"res\", \"week__generated\")\n"
                + ")\n"
                + "CLUSTERED INTO 3 SHARDS\n"
                + "PARTITIONED BY (\"res\", \"week__generated\")\n"
                + "WITH (\n"
                + "   number_of_replicas = '1'\n"
                + ");"),
        instanceOf(CreateTable.class));
  }

  @Test
  public void testPossibleExponentialBacktracking() throws Exception {
    SqlParser.createExpression("(((((((((((((((((((((((((((true)))))))))))))))))))))))))))");
  }

  @Test
  public void testDouble() throws Exception {
    assertExpression("123.", new DoubleLiteral("123"));
    assertExpression("123.0", new DoubleLiteral("123"));
    assertExpression(".5", new DoubleLiteral(".5"));
    assertExpression("123.5", new DoubleLiteral("123.5"));

    assertExpression("123E7", new DoubleLiteral("123E7"));
    assertExpression("123.E7", new DoubleLiteral("123E7"));
    assertExpression("123.0E7", new DoubleLiteral("123E7"));
    assertExpression("123E+7", new DoubleLiteral("123E7"));
    assertExpression("123E-7", new DoubleLiteral("123E-7"));

    assertExpression("123.456E7", new DoubleLiteral("123.456E7"));
    assertExpression("123.456E+7", new DoubleLiteral("123.456E7"));
    assertExpression("123.456E-7", new DoubleLiteral("123.456E-7"));

    assertExpression(".4E42", new DoubleLiteral(".4E42"));
    assertExpression(".4E+42", new DoubleLiteral(".4E42"));
    assertExpression(".4E-42", new DoubleLiteral(".4E-42"));
  }

  @Test
  public void testParameter() throws Exception {
    assertExpression("?", new ParameterExpression(1));
    for (int i = 0; i < 1000; i++) {
      assertExpression(String.format(Locale.ENGLISH, "$%d", i), new ParameterExpression(i));
    }
  }

  @Test
  public void testDoubleInQuery() {
    assertStatement(
        "SELECT 123.456E7 FROM DUAL",
        new Query(
            new QuerySpecification(
                selectList(new DoubleLiteral("123.456E7")),
                table(QualifiedName.of("dual")),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                Map.of(),
                List.of(),
                Optional.empty(),
                Optional.empty()),
            List.of(),
            Optional.empty(),
            Optional.empty()));
  }

  @Test
  public void testEmptyExpression() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createExpression(""),
        "line 1:1: mismatched input '<EOF>'");
  }

  @Test
  public void testEmptyStatement() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement(""),
        "line 1:1: mismatched input '<EOF>'");
  }

  @Test
  public void testExpressionWithTrailingJunk() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("1 + 1 x"),
        "line 1:7: extraneous input 'x' expecting");
  }

  @Test
  public void testTokenizeErrorStartOfLine() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("@select"),
        "line 1:1: extraneous input '@' expecting");
  }

  @Test
  public void testTokenizeErrorMiddleOfLine() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select * from foo where @what"),
        "line 1:25: no viable alternative at input 'select * from foo where @'");
  }

  @Test
  public void testTokenizeErrorIncompleteToken() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select * from 'oops"),
        "line 1:15: no viable alternative at input 'select * from ''");
  }

  @Test
  public void testParseErrorStartOfLine() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select *\nfrom x\nfrom"),
        "line 3:1: extraneous input 'from' expecting");
  }

  @Test
  public void testParseErrorMiddleOfLine() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select *\nfrom x\nwhere from"),
        "line 3:7: no viable alternative at input 'select *\\nfrom x\\nwhere from'");
  }

  @Test
  public void testParseErrorEndOfInput() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select * from"),
        "line 1:14: no viable alternative at input 'select * from'");
  }

  @Test
  public void testParseErrorEndOfInputWhitespace() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select * from  "),
        "line 1:16: no viable alternative at input 'select * from  '");
  }

  @Test
  public void testParseErrorBackquotes() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select * from `foo`"),
        "line 1:15: backquoted identifiers are not supported; "
            + "use double quotes to quote identifiers");
  }

  @Test
  public void testParseErrorBackquotesEndOfInput() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select * from foo `bar`"),
        "line 1:19: backquoted identifiers are not supported; "
            + "use double quotes to quote identifiers");
  }

  @Test
  public void testParseErrorDigitIdentifiers() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select 1x from dual"),
        "line 1:8: identifiers must not start with a digit; "
            + "surround the identifier with double quotes");
  }

  @Test
  public void testIdentifierWithColon() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select * from foo:bar"),
        "line 1:15: identifiers must not contain ':'");
  }

  @Test
  public void testParseErrorDualOrderBy() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select fuu from dual order by fuu order by fuu"),
        "line 1:35: mismatched input 'order'");
  }

  @Test
  public void testParseErrorReverseOrderByLimit() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select fuu from dual limit 10 order by fuu"),
        "line 1:31: mismatched input 'order' expecting {<EOF>, ';'}");
  }

  @Test
  public void testParseErrorReverseOrderByLimitOffset() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select fuu from dual limit 10 offset 20 order by fuu"),
        "line 1:41: mismatched input 'order' expecting {<EOF>, ';'}");
  }

  @Test
  public void testParseErrorReverseOrderByOffset() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select fuu from dual offset 20 order by fuu"),
        "line 1:32: mismatched input 'order' expecting {<EOF>, ';'}");
  }

  @Test
  public void testParseErrorReverseLimitOffset() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createStatement("select fuu from dual offset 20 order by fuu"),
        "line 1:32: mismatched input 'limit' expecting {<EOF>, ';'}");
  }

  @Test
  public void testParsingExceptionPositionInfo() {
    try {
      SqlParser.createStatement("select *\nfrom x\nwhere from");
      fail("expected exception");
    } catch (ParsingException e) {
      assertEquals(
          e.getMessage(),
          "line 3:7: no viable alternative at input 'select *\\nfrom x\\nwhere from'");
      assertEquals(
          e.getErrorMessage(), "no viable alternative at input 'select *\\nfrom x\\nwhere from'");
      assertEquals(e.getLineNumber(), 3);
      assertEquals(e.getColumnNumber(), 7);
    }
  }

  @Test
  public void testCurrentTimestamp() {
    assertExpression("CURRENT_TIMESTAMP", new CurrentTime(CurrentTime.Type.TIMESTAMP));
  }

  @Test
  public void testCurrentSchemaFunction() {
    assertInstanceOf("CURRENT_SCHEMA", FunctionCall.class);
    assertInstanceOf("CURRENT_SCHEMA()", FunctionCall.class);
  }

  @Test
  public void testUserFunctions() {
    assertInstanceOf("CURRENT_USER", FunctionCall.class);
    assertInstanceOf("SESSION_USER", FunctionCall.class);
    assertInstanceOf("USER", FunctionCall.class);
  }

  @Test
  public void testTrimFunctionExpression() {
    assertExpression(
        "TRIM(BOTH 'A' FROM chars)",
        new FunctionCall(
            new QualifiedName("trim"),
            List.of(
                new QualifiedNameReference(new QualifiedName("chars")),
                new StringLiteral("A"),
                new StringLiteral("BOTH"))));
  }

  @Test
  public void testTrimFunctionExpressionSingleArgument() {
    assertExpression(
        "TRIM(chars)",
        new FunctionCall(
            new QualifiedName("trim"),
            List.of(new QualifiedNameReference(new QualifiedName("chars")))));
  }

  @Test
  public void testTrimFunctionAllArgs() {
    assertInstanceOf("TRIM(LEADING 'A' FROM chars)", FunctionCall.class);
    assertInstanceOf("TRIM(TRAILING 'A' FROM chars)", FunctionCall.class);
    assertInstanceOf("TRIM(BOTH 'A' FROM chars)", FunctionCall.class);
  }

  @Test
  public void testTrimFunctionDefaultTrimModeOnly() {
    assertInstanceOf("TRIM('A' FROM chars)", FunctionCall.class);
  }

  @Test
  public void testTrimFunctionDefaultCharsToTrimOnly() {
    assertInstanceOf("TRIM(LEADING FROM chars)", FunctionCall.class);
    assertInstanceOf("TRIM(TRAILING FROM chars)", FunctionCall.class);
    assertInstanceOf("TRIM(BOTH FROM chars)", FunctionCall.class);
    assertInstanceOf("TRIM(FROM chars)", FunctionCall.class);
  }

  @Test
  public void testTrimFunctionDefaultTrimModeAndCharsToTrim() {
    assertInstanceOf("TRIM(chars)", FunctionCall.class);
  }

  @Test
  public void testTrimFunctionMissingFromWhenCharsToTrimIsPresentThrowsException() {
    assertThrows(
        ParsingException.class,
        () -> assertInstanceOf("TRIM(' ' chars)", FunctionCall.class),
        "line 1:10: no viable alternative at input 'TRIM(' ' chars'");
  }

  private void assertInstanceOf(String expr, Class<? extends Node> cls) {
    Expression expression = SqlParser.createExpression(expr);
    assertThat(expression, instanceOf(cls));
  }

  @Test
  public void testStackOverflowExpression() {
    assertThrows(
        ParsingException.class,
        () -> SqlParser.createExpression(Lists2.joinOn(" OR ", nCopies(4000, "x = y"), x -> x)),
        "line 1:1: expression is too large (stack overflow while parsing)");
  }

  @Test
  public void testStackOverflowStatement() {
    assertThrows(
        ParsingException.class,
        () ->
            SqlParser.createStatement(
                "SELECT " + Lists2.joinOn(" OR ", nCopies(4000, "x = y"), x -> x)),
        "line 1:1: statement is too large (stack overflow while parsing)");
  }

  @Test
  public void testDataTypesWithWhitespaceCharacters() {
    Cast cast = (Cast) SqlParser.createExpression("1::double precision");
    assertEquals(ColumnType.class, cast.getType().getClass());
    assertThat(cast.getType().name(), is("double precision"));
  }

  @Test
  public void testFromStringLiteralCast() {
    assertInstanceOf("TIMESTAMP '2016-12-31 01:02:03.123'", Cast.class);
    assertInstanceOf("int2 '2016'", Cast.class);
  }

  @Test
  public void testFromStringLiteralCastDoesNotSupportArrayType() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> SqlParser.createExpression("array(boolean) '[1,2,0]'"),
        "type 'string' cast notation only supports primitive types. "
            + "Use '::' or cast() operator instead.");
  }

  @Test
  public void testFromStringLiteralCastDoesNotSupportObjectType() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> SqlParser.createExpression("object '{\"x\": 10}'"),
        "type 'string' cast notation only supports primitive types. "
            + "Use '::' or cast() operator instead.");
  }

  private static void assertStatement(String query, Statement expected) {
    assertParsed(query, expected, SqlParser.createStatement(query));
  }

  private static void assertExpression(String expression, Expression expected) {
    assertParsed(expression, expected, SqlParser.createExpression(expression));
  }

  private static void assertParsed(String input, Node expected, Node parsed) {
    if (!parsed.equals(expected)) {
      fail(
          format(
              "expected%n%n%s%n%nto parse as%n%n%s%n%nbut was%n%n%s%n",
              indent(input), indent(formatSql(expected)), indent(formatSql(parsed))));
    }
  }

  private static String indent(String value) {
    String indent = "    ";
    return indent + value.trim().replaceAll("\n", "\n" + indent);
  }
}
