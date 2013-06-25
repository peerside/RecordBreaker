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

import com.cloudera.recordbreaker.analyzer.FSAnalyzer;
import com.cloudera.recordbreaker.analyzer.FileSummary;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.WebMarkupContainer;

/************************************************
 * Application-wide warning box that tells user
 * when she does not have access for the current file/dir
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 *************************************************/
public class AccessControlWarningBox extends WebMarkupContainer {
  String targetName = null;
  int targetFid = -1;
  
  public AccessControlWarningBox(String name, int targetFid) {
    super(name);
    this.targetFid = targetFid;
    setOutputMarkupPlaceholderTag(true);
    setVisibilityAllowed(false);
  }
  
  public AccessControlWarningBox(String name, String targetName) {
    super(name);
    this.targetName = targetName;
    setOutputMarkupPlaceholderTag(true);
    setVisibilityAllowed(false);
  }

  public void onConfigure() {
    FishEye fe = FishEye.getInstance();
    AccessController accessCtrl = fe.getAccessController();
    FSAnalyzer fsAnalyzer = fe.getAnalyzer();
    
    FileSummary fileSummary = null;
    if (targetFid >= 0) {
      fileSummary = new FileSummary(fsAnalyzer, targetFid);
    } else if (targetName != null) {
      fileSummary = fsAnalyzer.getSingleFileSummary(targetName);
    }

    if (fileSummary != null) {
      setVisibilityAllowed(fe.hasFSAndCrawl() && !accessCtrl.hasReadAccess(fileSummary));
    } else {
      setVisibilityAllowed(false);
    }
  }
}
