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

/**
 * <code>SchemaSummary</code> captures the metadata for a schema.
 * It has a handle to a SchemaSummaryData object, which is materialized
 * as needed.
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 */
public class SchemaSummary {
  FSAnalyzer analyzer;
  long schemaId;
  SchemaSummaryData ssd = null;
  List<TypeGuessSummary> typeGuesses = null;

  /**
   */
  public SchemaSummary(FSAnalyzer analyzer, long schemaId) {
    this.analyzer = analyzer;
    this.schemaId = schemaId;
    this.ssd = null;
    this.typeGuesses = null;
  }
  public void addCachedData(SchemaSummaryData ssd) {
    this.ssd = ssd;
  }
  public void addCachedData(List<TypeGuessSummary> typeGuesses) {
    this.typeGuesses = typeGuesses;
  }
  public long getSchemaId() {
    return schemaId;
  }
  void getData() {
    this.ssd = analyzer.getSchemaSummaryData(this.schemaId);
  }
  public String getIdentifier() {
    if (ssd == null) {
      getData();
    }
    return ssd.schemaIdentifier;
  }
  public String getDesc() {
    if (ssd == null) {
      getData();
    }
    return ssd.schemaDesc;
  }
  public List<TypeGuessSummary> getTypeGuesses() {
    if (typeGuesses == null) {
      typeGuesses = analyzer.getTypeGuessesForSchema(this.schemaId);
    }
    return typeGuesses;
  }
}