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

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * The actual data for a file is contained in a <code>FileSummaryData</code> obj.
 *
 */
public class FileSummaryData {
  long fid;
  long crawlid;
  String fname;
  String owner;
  String group;
  FsPermission permissions;
  long size;
  String lastModified;
  String path;
  DataDescriptor dd;
  
  public FileSummaryData(long fid, long crawlid, String fname, String owner, String group, String permissions, long size, String lastModified, String path, DataDescriptor dd) {
    this.fid = fid;
    this.crawlid = crawlid;
    this.fname = fname;
    this.owner = owner;
    this.group = group;
    this.permissions = FsPermission.valueOf(permissions);
    this.size = size;
    this.lastModified = lastModified;
    this.path = path;
    this.dd = dd;
  }
}
