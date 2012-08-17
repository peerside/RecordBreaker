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
package com.cloudera.recordbreaker.analyzer;

import java.util.*;

/***********************************************************
 * <code>CrawlSummary</code> is an interface to info about a Crawl.
 * The data is "materialized" from the database upon the first call to
 * one of its accessors.
 *************************************************************/
public class CrawlSummary {
  FSAnalyzer analyzer;
  public long crawlid;
  public String started;
  public boolean isOngoing;
  public String finished;
  public long fsid;
  public boolean hasData = false;
  
  public CrawlSummary(FSAnalyzer analyzer, long crawlid) {
    this.analyzer = analyzer;
    this.crawlid = crawlid;
    this.hasData = false;
  }
  public CrawlSummary(FSAnalyzer analyzer, long crawlid, String started, String finished, boolean isOngoing, long fsid) {
    this.analyzer = analyzer;
    this.crawlid = crawlid;
    this.started = started;
    this.finished = finished;
    this.isOngoing = isOngoing;
    this.fsid = fsid;
    this.hasData = true;
  }

  public long getCrawlId() {
    return this.crawlid;
  }  

  public String getStartedDate() {
    if (! hasData) {
      getData();
    }
    return started;
  }

  protected void getData() {
    CrawlSummary fullSummary = analyzer.getCrawlSummaryData(this.crawlid);
    this.started = fullSummary.started;
    this.finished = fullSummary.finished;
    this.isOngoing = fullSummary.isOngoing;
    this.fsid = fullSummary.fsid;
    this.hasData = true;
  } 
}
