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

/************************************************************
 * <code>TypeGuessSummary</code> is an interface to info about a type
 * inference for a file.  It is mainly a box to hold pointers to Files,
 * Types, and Schemas.
 *************************************************************/
public class TypeGuessSummary {
  FSAnalyzer analyzer;
  long fid;
  long typeid;
  long schemaid;
  double score;
  
  public TypeGuessSummary(FSAnalyzer analyzer, long fid, long typeid, long schemaid, double score) {
    this.analyzer = analyzer;
    this.fid = fid;
    this.typeid = typeid;
    this.schemaid = schemaid;
    this.score = score;
  }
  public FileSummary getFileSummary() {
    return new FileSummary(analyzer, fid);
  }
  public TypeSummary getTypeSummary() {
    return new TypeSummary(analyzer, typeid);
  }
  public SchemaSummary getSchemaSummary() {
    return new SchemaSummary(analyzer, schemaid);
  }
  public double getScore() {
    return score;
  }
}
