package com.risingwave.planner.planner.streaming;

import static com.google.common.base.Verify.verify;

import com.risingwave.planner.util.ResourceUtil;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** Loading test cases for <code>StreamFragmenterTest</code>. */
public class StreamFragmenterTestLoader {
  public static List<String> loadSample(Class<?> klass) {
    Document doc = ResourceUtil.loadXml(ResourceUtil.findFile(klass, ".Sample.xml"));
    Element root = doc.getDocumentElement();

    verify(
        ResourceUtil.ROOT_TAG.equalsIgnoreCase(root.getTagName()),
        "Root element tag should be %s",
        ResourceUtil.ROOT_TAG);

    NodeList resources = root.getElementsByTagName(ResourceUtil.RESOURCE_TAG);
    List<String> samples = new ArrayList<>(resources.getLength());
    for (int i = 0; i < resources.getLength(); i++) {
      Element element = (Element) resources.item(i);
      if (element.hasAttribute(ResourceUtil.PATH_TAG)) {
        String jsonPath = element.getAttribute(ResourceUtil.PATH_TAG);
        URL jsonFile = ResourceUtil.findJsonFileFromPath(jsonPath);
        String json = ResourceUtil.readJsonStringFromUrl(jsonFile);
        samples.add(json);
      } else {
        samples.add(ResourceUtil.getText(element));
      }
    }
    return samples;
  }
}
