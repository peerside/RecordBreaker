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

/*****************************************************
 * <code>TypeSummary</code> is an interface to info about a file type.
 * The data is "materialized" from the database upon the first call to
 * one of its accessors.
 *****************************************************/
public class TypeSummary {
  FSAnalyzer analyzer;
  long typeid;
  TypeSummaryData tsd = null;
  List<TypeGuessSummary> tgs = null;

  /**
   */
  public TypeSummary(FSAnalyzer analyzer, long typeid) {
    this.analyzer = analyzer;
    this.typeid = typeid;
    this.tsd = null;
    this.tgs = null;
  }
  public void addCachedData(TypeSummaryData tsd) {
    this.tsd = tsd;
  }
  public void addCachedData(List<TypeGuessSummary> tgs) {
    this.tgs = tgs;
  }
  public long getTypeId() {
    return typeid;
  }
  public String getLabel() {
    if (tsd == null) {
      this.tsd = analyzer.getTypeSummaryData(this.typeid);
    }
    return tsd.typeLabel;
  }
  public List<TypeGuessSummary> getTypeGuesses() {
    if (this.tgs == null) {
      this.tgs = analyzer.getTypeGuessesForType(this.typeid);
    }
    return tgs;
  }
}