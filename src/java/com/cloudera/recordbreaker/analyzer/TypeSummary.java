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
  boolean hasData = false;
  TypeSummaryData tsd = null;

  /**
   */
  public TypeSummary(FSAnalyzer analyzer, long typeid) {
    this.analyzer = analyzer;
    this.typeid = typeid;
    this.hasData = false;
  }
  void getData() {
    this.tsd = analyzer.getTypeSummaryData(this.typeid);
    this.hasData = true;
  }
  public String getLabel() {
    if (!hasData) {
      getData();
    }
    return tsd.typeLabel;
  }
  public String getDesc() {
    if (!hasData) {
      getData();
    }
    return tsd.typeDesc;
  }
  public List<TypeGuessSummary> getTypeGuesses() {
    return analyzer.getTypeGuessesForType(this.typeid);
  }
}