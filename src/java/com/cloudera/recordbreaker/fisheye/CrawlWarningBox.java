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

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.model.PropertyModel;

import java.net.URI;

/************************************************
 * Application-wide warning box that tells user
 * when there is no available crawl for this filesystem.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 *************************************************/
public class CrawlWarningBox extends WebMarkupContainer {
  final class CrawlErrorMsgHandler {
    public CrawlErrorMsgHandler() {
    }
    public String getErrorMsg() {
      FishEye fe = FishEye.getInstance();
      URI fsURI = fe.getFSURI();
      AccessController accessCtrl = fe.getAccessController();      
      String user = accessCtrl.getCurrentUser();

      String errorMsg = null;      
      if (fsURI == null) {
        errorMsg = "FishEye has no filesystem so there is nothing to display.  Fix this in Settings.";
      } else if (! fe.hasFSAndCrawl()) {
        errorMsg = "FishEye has a filesystem, but has not yet completed a crawl so there is nothing to display.  See crawl progress in Settings.";
      }
      return errorMsg;
    }
  }
  public CrawlWarningBox() {
    super("crawlWarningMsgContainer");
    add(new Label("crawlErrorLabel", new PropertyModel(new CrawlErrorMsgHandler(), "errorMsg")));
    setOutputMarkupPlaceholderTag(true);
    setVisibilityAllowed(false);
  }
  public void onConfigure() {
    FishEye fe = FishEye.getInstance();    
    setVisibilityAllowed(! fe.hasFSAndCrawl());
  }
}