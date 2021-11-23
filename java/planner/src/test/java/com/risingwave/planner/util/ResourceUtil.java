package com.risingwave.planner.util;

import java.io.File;
import java.net.URL;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/** Utils for planner test loader. */
public class ResourceUtil {
  public static final String ROOT_TAG = "Root";
  public static final String TEST_CASE_TAG = "TestCase";
  public static final String RESOURCE_TAG = "Resource";
  public static final String NAME_ATTR = "name";
  public static final String SQL_TAG = "sql";
  public static final String PLAN_TAG = "plan";
  public static final String PHY_PLAN_TAG = "physical";
  public static final String DIST_PLAN_TAG = "distributed";
  public static final String JSON_TAG = "json";
  public static final String PATH_TAG = "path";
  public static final String PRIMARY_KEY_TAG = "primaryKey";

  public static URL findFile(Class<?> clazz, final String suffix) {
    // The reference file for class "com.foo.Bar" is "com/foo/Bar.xml"
    String rest = "/" + clazz.getName().replace('.', File.separatorChar) + suffix;
    return clazz.getResource(rest);
  }

  public static Document loadXml(URL refFile) {
    try {
      DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = fac.newDocumentBuilder();

      return docBuilder.parse(refFile.openStream());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns the text under an element. */
  public static String getText(Element element) {
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
