package com.risingwave.planner.util;

import static com.google.common.base.Verify.verify;
import static com.risingwave.planner.util.ResourceUtil.RESOURCE_TAG;
import static com.risingwave.planner.util.ResourceUtil.ROOT_TAG;
import static com.risingwave.planner.util.ResourceUtil.findFile;
import static com.risingwave.planner.util.ResourceUtil.getText;
import static com.risingwave.planner.util.ResourceUtil.loadXml;

import java.util.ArrayList;
import java.util.List;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class PlannerTestDdlLoader {
  public static List<String> load(Class<?> klass) {
    Document doc = loadXml(findFile(klass, ".ddl.xml"));
    Element root = doc.getDocumentElement();

    verify(ROOT_TAG.equalsIgnoreCase(root.getTagName()), "Root element tag should be %s", ROOT_TAG);

    NodeList resources = root.getElementsByTagName(RESOURCE_TAG);
    List<String> ddls = new ArrayList<>(resources.getLength());
    for (int i = 0; i < resources.getLength(); i++) {
      ddls.add(getText((Element) resources.item(i)));
    }

    return ddls;
  }
}
