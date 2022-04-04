package com.risingwave.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Tests for Endpoint. */
public class EndPointTest {
  @Test
  void testEquals() {
    EndPoint ep1 = new EndPoint("127.0.0.1", 123);
    assertEquals(ep1.getHost(), "127.0.0.1");
    assertEquals(ep1.getPort(), 123);
    assertEquals(ep1.toString(), "EndPoint{host='127.0.0.1', port=123}");
    assertTrue(ep1.equals(ep1));

    EndPoint ep2 = new EndPoint("127.0.0.1", 123);
    assertTrue(ep1.equals(ep2));

    EndPoint ep3 = new EndPoint("127.0.0.1", 124);
    assertFalse(ep1.equals(ep3));

    assertFalse(ep1.equals(null));
    assertFalse(ep1.equals("123"));
  }
}
