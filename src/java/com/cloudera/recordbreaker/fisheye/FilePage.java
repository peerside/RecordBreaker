/*
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.recordbreaker.fisheye;

import com.cloudera.recordbreaker.analyzer.FileSummary;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.request.mapper.parameter.PageParameters;

/**
 * Wicket Page class that describes a specific File
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see WebPage
 */
public class FilePage extends WebPage {
  public FilePage() {
  }
  public FilePage(PageParameters params) {
    String fidStr = params.get("fid").toString();
    if (fidStr != null) {
      try {
        FileSummary fs = new FileSummary(FishEye.analyzer, Long.parseLong(fidStr));
        if (fs != null) {
          add(new Label("filetitle", fs.getFname()));
          add(new Label("filesubtitle", "in " + fs.getPath()));
          add(new Label("owner", fs.getOwner()));
          add(new Label("size", "" + fs.getSize()));
          add(new Label("lastmodified", fs.getLastModified()));
          return;
        }
      } catch (NumberFormatException nfe) {
      }
    }
    add(new Label("filetitle", "unknown"));
    add(new Label("filesubtitle", ""));
    add(new Label("owner", ""));
    add(new Label("size", ""));
    add(new Label("lastmodified", ""));
  }
}
