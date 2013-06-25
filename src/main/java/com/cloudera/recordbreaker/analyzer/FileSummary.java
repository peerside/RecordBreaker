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
import java.io.IOException;
import java.io.InputStream;

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
  FileSummaryData fsd = null;
  List<TypeGuessSummary> typeGuesses = null;

  public FileSummary(FSAnalyzer analyzer, long fid) {
    this.analyzer = analyzer;
    this.fid = fid;
    this.fsd = null;
    this.typeGuesses = null;
  }
  public void addCachedData(FileSummaryData fsd) {
    this.fsd = fsd;
  }
  public void addCachedData(List<TypeGuessSummary> typeGuesses) {
    this.typeGuesses = typeGuesses;
  }
  void getData() {
    this.fsd = analyzer.getFileSummaryData(this.fid);
  }
  public DataDescriptor getDataDescriptor() throws IOException {
    // Can an implementation of DataDescriptor replace the file summary data?
    if (fsd == null) {
      getData();
    }
    return fsd.getDataDescriptor();
  }

  public InputStream getRawBytes() throws IOException {
    return analyzer.getRawBytes(getPath());
  }

  public long getFid() {
    return fid;
  }    
  public boolean isDir() {
    if (fsd == null) {
      getData();
    }
    return fsd.isDir;
  }
  public String getFname() {
    if (fsd == null) {
      getData();
    }
    return fsd.fname;
  }
  public String getOwner() {
    if (fsd == null) {
      getData();
    }
    return fsd.owner;
  }
  public String getGroup() {
    if (fsd == null) {
      getData();
    }
    return fsd.group;
  }
  public FsPermission getPermissions() {
    if (fsd == null) {
      getData();
    }
    return fsd.permissions;
  }
  public long getSize() {
    if (fsd == null) {
      getData();
    }
    return fsd.size;
  }
  public String getLastModified() {
    if (fsd == null) {
      getData();
    }
    return fsd.lastModified;
  }
  public Path getPath() {
    return new Path(getParentDir(), getFname());
  }
  public String getParentDir() {
    if (fsd == null) {
      getData();
    }
    return fsd.path;
  }
  public CrawlSummary getCrawl() {
    if (fsd == null) {
      getData();
    }
    return new CrawlSummary(this.analyzer, fsd.crawlid);
  }
  public List<TypeGuessSummary> getTypeGuesses() {
    if (typeGuesses == null) {
      typeGuesses = analyzer.getTypeGuessesForFile(this.fid);
    }
    return typeGuesses;
  }
}