package com.cloudera.recordbreaker.fisheye;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.basic.Label;

public class Overview extends WebPage {
  public Overview() {
    add(new Label("numFisheyeFiles", "10"));
  }
}
