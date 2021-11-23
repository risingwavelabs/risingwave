package com.risingwave.planner.util;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** Load plan test for verification. Batch/Stream planner share a common loader. */
public class PlanTestCaseLoader implements ArgumentsProvider {

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
    URL file = ResourceUtil.findFile(context.getRequiredTestClass(), ".xml");

    return parseTestCases(ResourceUtil.loadXml(file)).stream().map(Arguments::of);
  }

  private static Collection<PlannerTestCase> parseTestCases(Document document) {
    Element root = document.getDocumentElement();
    verify(
        ResourceUtil.ROOT_TAG.equals(root.getTagName()),
        "Root element's tag must be %s",
        ResourceUtil.ROOT_TAG);

    NodeList testCaseNodes = root.getElementsByTagName(ResourceUtil.TEST_CASE_TAG);
    List<PlannerTestCase> testCases = new ArrayList<>(testCaseNodes.getLength());
    for (int i = 0; i < testCaseNodes.getLength(); i++) {
      testCases.add(parseOneTestCase(testCaseNodes.item(i)));
    }

    return testCases;
  }

  private static PlannerTestCase parseOneTestCase(Node node) {
    verify(Node.ELEMENT_NODE == node.getNodeType(), "Root can only contains element child!");
    Element element = (Element) node;
    verify(
        ResourceUtil.TEST_CASE_TAG.equals(element.getTagName()),
        "Child of root element's tag must be %s.",
        ResourceUtil.TEST_CASE_TAG);

    String testCaseName = element.getAttribute(ResourceUtil.NAME_ATTR);
    verify(testCaseName != null && !testCaseName.isEmpty(), "Test case name can't be empty!");

    String sql = null;
    String plan = null;
    String phyPlan = null;
    String distPlan = null;
    String json = null;
    String primaryKey = null;
    NodeList resources = element.getElementsByTagName(ResourceUtil.RESOURCE_TAG);
    for (int i = 0; i < resources.getLength(); i++) {
      Element res = (Element) resources.item(i);
      String name = res.getAttribute(ResourceUtil.NAME_ATTR);
      if (ResourceUtil.SQL_TAG.equals(name)) {
        sql = ResourceUtil.getText(res).trim();
      } else if (ResourceUtil.PLAN_TAG.equals(name)) {
        plan = ResourceUtil.getText(res).trim();
      } else if (ResourceUtil.PHY_PLAN_TAG.equals(name)) {
        phyPlan = ResourceUtil.getText(res).trim();
      } else if (ResourceUtil.DIST_PLAN_TAG.equals(name)) {
        distPlan = ResourceUtil.getText(res).trim();
      } else if (ResourceUtil.PRIMARY_KEY_TAG.equals(name)) {
        primaryKey = ResourceUtil.getText(res).trim();
      } else if (ResourceUtil.JSON_TAG.equals(name)) {
        if (res.hasAttribute(ResourceUtil.PATH_TAG)) {
          String path = res.getAttribute(ResourceUtil.PATH_TAG);
          json = res.getAttribute(ResourceUtil.PATH_TAG);
        } else {
          // TODO: remove directly read json source. It should be a path to json file.
          json = ResourceUtil.getText(res).trim();
        }
      }
    }

    verifyNotNull(sql, "Sql content not found in test case: %s!", testCaseName);
    //  verifyNotNull(plan, "Plan content not found in test case: %s!", testCaseName);
    //  verifyNotNull(json, "Json plan content not found in test case: %s!", testCaseName);

    return new PlannerTestCase(testCaseName, sql, plan, phyPlan, distPlan, json, primaryKey);
  }
}
