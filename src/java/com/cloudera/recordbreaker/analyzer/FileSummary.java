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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/*****************************************************
 * <code>FileSummary</code> is an interface to info about a File.
 * The data is "materialized" from the database upon the first call to
 * one of its accessors.
 *****************************************************/
public class FileSummary {
  FSAnalyzer analyzer;
  long fid;
  boolean hasData = false;
  FileSummaryData fsd = null;

  public FileSummary(FSAnalyzer analyzer, long fid) {
    this.analyzer = analyzer;
    this.fid = fid;
    this.hasData = false;
  }

  void getData() {
    this.fsd = analyzer.getFileSummaryData(this.fid);
    this.hasData = true;
  }

  public long getFid() {
    return fid;
  }    
  
  public String getFname() {
    if (!hasData) {
      getData();
    }
    return fsd.fname;
  }
  public String getOwner() {
    if (!hasData) {
      getData();
    }
    return fsd.owner;
  }
  public String getGroup() {
    if (!hasData) {
      getData();
    }
    return fsd.group;
  }
  public FsPermission getPermissions() {
    if (!hasData) {
      getData();
    }
    return fsd.permissions;
  }
  public long getSize() {
    if (!hasData) {
      getData();
    }
    return fsd.size;
  }
  public String getLastModified() {
    if (!hasData) {
      getData();
    }
    return fsd.lastModified;
  }
  public Path getPath() {
    return new Path(getParentDir(), getFname());
  }
  public String getParentDir() {
    if (!hasData) {
      getData();
    }
    return fsd.path;
  }
  public CrawlSummary getCrawl() {
    if (!hasData) {
      getData();
    }
    return new CrawlSummary(this.analyzer, fsd.crawlid);
  }
  public List<TypeGuessSummary> getTypeGuesses() {
    return analyzer.getTypeGuessesForFile(this.fid);
  }
}