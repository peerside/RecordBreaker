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

import org.apache.wicket.markup.html.WebMarkupContainer;

/****************************************************
 * <code>FileContentsTable</code> is a panel that shows the (structured)
 * contents of a FishEye file.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 ****************************************************/
public class FileContentsTable extends WebMarkupContainer {
  long fid = -1L;
  
  public FileContentsTable() {
    super("filecontentstable");
    setOutputMarkupPlaceholderTag(true);
    setVisibilityAllowed(false);
  }
  public FileContentsTable(long fid) {
    super("filecontentstable");
    this.fid = fid;
    setOutputMarkupPlaceholderTag(true);
    setVisibilityAllowed(false);
  }

  public void onConfigure() {
    if (fid < 0) {
      setVisibilityAllowed(false);
    } else {
      FishEye fe = FishEye.getInstance();
      AccessController accessCtrl = fe.getAccessController();    
      FSAnalyzer fsAnalyzer = fe.getAnalyzer();
      FileSummary fileSummary = new FileSummary(fsAnalyzer, fid);
      setVisibilityAllowed(fe.hasFSAndCrawl() && accessCtrl.hasReadAccess(fileSummary));
    }
  }
}