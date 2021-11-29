package com.risingwave.planner.util;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

  private URL findJsonFileFromPathCore(String path) {
    String res = "com/risingwave/planner/json/" + path + ".json";
    URL ret = getClass().getClassLoader().getResource(res);
    return ret;
  }

  public static URL findJsonFileFromPath(String path) {
    ResourceUtil util = new ResourceUtil();
    return util.findJsonFileFromPathCore(path);
  }

  public static String readJsonStringFromUrl(URL url) {
    try {
      return Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
