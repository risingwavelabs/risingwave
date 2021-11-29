package com.risingwave.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Identifier test. */
public class IdentifiersTest {

  @Test
  public void test_number_of_keywords() {
    // If this test is failing you are introducing a new reserved keyword which is a breaking
    // change.
    // Either add the new term to `nonReserved` in `SqlBase.4g` or add a breaking changes entry and
    // adapt this test.
    assertThat(
        (int) Identifiers.KEYWORDS.stream().filter(Identifiers.Keyword::isReserved).count(),
        is(96));
  }

  @Test
  public void testEscape() {
    assertThat(Identifiers.escape(""), is(""));
    assertThat(Identifiers.escape("\""), is("\"\""));
    assertThat(Identifiers.escape("ABC\""), is("ABC\"\""));
    assertThat(Identifiers.escape("\"ABC"), is("\"\"ABC"));
    assertThat(Identifiers.escape("abcDEF"), is("abcDEF"));
    assertThat(Identifiers.escape("œ"), is("œ"));
  }

  @Test
  public void testQuote() {
    assertThat(Identifiers.quote(""), is("\"\""));
    assertThat(Identifiers.quote("\""), is("\"\"\"\""));
    assertThat(Identifiers.quote("ABC"), is("\"ABC\""));
    assertThat(Identifiers.quote("EF"), is("\"EF\""));
  }

  @Test
  public void testQuoteIfNeeded() {
    assertThat(Identifiers.quoteIfNeeded(""), is("\"\""));
    assertThat(Identifiers.quoteIfNeeded("\""), is("\"\"\"\""));
    assertThat(Identifiers.quoteIfNeeded("fhjgadhjgfhs"), is("fhjgadhjgfhs"));
    assertThat(Identifiers.quoteIfNeeded("fhjgadhjgfhsÖ"), is("\"fhjgadhjgfhsÖ\""));
    assertThat(Identifiers.quoteIfNeeded("ABC"), is("\"ABC\""));
    assertThat(Identifiers.quoteIfNeeded("abc\""), is("\"abc\"\"\""));
    assertThat(Identifiers.quoteIfNeeded("select"), is("\"select\"")); // keyword
    assertThat(Identifiers.quoteIfNeeded("1column"), is("\"1column\""));
    assertThat(Identifiers.quoteIfNeeded("column name"), is("\"column name\""));
    assertThat(Identifiers.quoteIfNeeded("col1a"), is("col1a"));
    assertThat(Identifiers.quoteIfNeeded("_col"), is("_col"));
    assertThat(Identifiers.quoteIfNeeded("col_1"), is("col_1"));
    assertThat(Identifiers.quoteIfNeeded("col['a']"), is("\"col['a']\""));
  }

  @Test
  public void test_maybe_quote_expression_behaves_like_quote_if_needed_for_non_subscripts()
      throws Exception {
    for (String candidate :
        List.of(
            (""),
            ("\""),
            ("fhjgadhjgfhs"),
            ("fhjgadhjgfhsÖ"),
            ("ABC"),
            ("abc\""),
            ("select"),
            ("1column"),
            ("column name"),
            ("col1a"),
            ("_col"),
            ("col_1"))) {
      assertThat(
          Identifiers.maybeQuoteExpression(candidate), is(Identifiers.quoteIfNeeded(candidate)));
    }
  }

  @Test
  public void test_quote_expression_quotes_only_base_part_of_subscript_expression()
      throws Exception {
    assertThat(Identifiers.maybeQuoteExpression("col['a']"), is("col['a']"));
    assertThat(Identifiers.maybeQuoteExpression("Col['a']"), is("\"Col\"['a']"));
    assertThat(
        Identifiers.maybeQuoteExpression("col with space['a']"), is("\"col with space\"['a']"));
  }

  @Test
  public void test_quote_expression_quotes_keywords() throws Exception {
    assertThat(Identifiers.maybeQuoteExpression("select"), is("\"select\"")); // keyword
  }
}
