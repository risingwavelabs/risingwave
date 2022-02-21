package com.risingwave.planner.rel.common;

import org.apache.calcite.rel.RelNode;

/** Extract the node identity from the result of its explainTerms function. */
public class IdentityExtractor {

  /**
   * @param node The node to be identified.
   * @param separator Since explainTerms function return the current node along with its input, we
   *     need to get rid of its input's information.
   * @return The identity of the node.
   */
  public static String getCurrentNodeIdentity(RelNode node, String separator) {
    var nodeWithItsChildren = node.explain();
    var endIndex = nodeWithItsChildren.indexOf("\n  " + separator);
    String identity = "";
    if (endIndex != -1) {
      identity = nodeWithItsChildren.substring(0, endIndex);
    } else {
      identity = nodeWithItsChildren;
    }
    if (identity.charAt(identity.length() - 1) == '\n') {
      identity = identity.substring(0, identity.length() - 1);
    }
    return identity;
  }
}
