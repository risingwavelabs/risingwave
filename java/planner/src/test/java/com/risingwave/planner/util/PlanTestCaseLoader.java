package com.risingwave.planner.util;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

public class PlanTestCaseLoader implements ArgumentsProvider {
  private static final String ROOT_TAG = "Root";
  private static final String TEST_CASE_TAG = "TestCase";
  private static final String RESOURCE_TAG = "Resource";
  private static final String NAME_ATTR = "name";
  private static final String SQL_TAG = "sql";
  private static final String PLAN_TAG = "plan";

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
    URL file = findFile(context.getRequiredTestClass(), ".xml");

    return parseTestCases(loadXml(file)).stream().map(Arguments::of);
  }

  private static URL findFile(Class<?> clazz, final String suffix) {
    // The reference file for class "com.foo.Bar" is "com/foo/Bar.xml"
    String rest = "/" + clazz.getName().replace('.', File.separatorChar) + suffix;
    return clazz.getResource(rest);
  }

  private static Document loadXml(URL refFile)
      throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
    DocumentBuilder docBuilder = fac.newDocumentBuilder();

    return docBuilder.parse(refFile.openStream());
  }

  private static Collection<PlannerTestCase> parseTestCases(Document document) {
    Element root = document.getDocumentElement();
    verify(ROOT_TAG.equals(root.getTagName()), "Root element's tag must be %s", ROOT_TAG);

    NodeList testCaseNodes = root.getElementsByTagName(TEST_CASE_TAG);
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
        TEST_CASE_TAG.equals(element.getTagName()),
        "Child of root element's tag must be %s.",
        TEST_CASE_TAG);

    String testCaseName = element.getAttribute(NAME_ATTR);
    verify(testCaseName != null && !testCaseName.isEmpty(), "Test case name can't be empty!");

    String sql = null;
    String plan = null;
    NodeList resources = element.getElementsByTagName(RESOURCE_TAG);
    for (int i = 0; i < resources.getLength(); i++) {
      Element res = (Element) resources.item(i);
      String name = res.getAttribute(NAME_ATTR);
      if (SQL_TAG.equals(name)) {
        sql = getText(res);
      } else if (PLAN_TAG.equals(name)) {
        plan = getText(res);
      }

      if (sql != null && plan != null) {
        break;
      }
    }

    verifyNotNull(sql, "Sql content not found in test case: %s!", testCaseName);
    verifyNotNull(plan, "Plan content not found in test case: %s!", testCaseName);

    return new PlannerTestCase(testCaseName, sql, plan);
  }

  /** Returns the text under an element. */
  private static String getText(Element element) {
    // If there is a <![CDATA[ ... ]]> child, return its text and ignore
    // all other child elements.
    final NodeList childNodes = element.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node node = childNodes.item(i);
      if (node instanceof CDATASection) {
        return node.getNodeValue();
      }
    }

    // Otherwise return all the text under this element (including
    // whitespace).
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node node = childNodes.item(i);
      if (node instanceof Text) {
        buf.append(((Text) node).getWholeText());
      }
    }
    return buf.toString();
  }
}
