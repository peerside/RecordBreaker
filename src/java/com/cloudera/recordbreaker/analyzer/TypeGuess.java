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

/***************************************************
 * <code>TypeGuess</code> captures the system's estimates about
 * an item in the filesystem
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 ***************************************************/
public class TypeGuess {
  String typeLabel;
  String typeDesc;
  String schemaLabel;
  String schemaDesc;
  double score;
    
  public TypeGuess(String typeLabel, String typeDesc, String schemaLabel, String schemaDesc, double score) {
    this.typeLabel = typeLabel;
    this.typeDesc = typeDesc;
    this.schemaLabel = schemaLabel;
    this.schemaDesc = schemaDesc;
    this.score = score;
  }
  String getTypeLabel() {
    return typeLabel;
  }
  String getTypeDesc() {
    return typeDesc;
  }
  String getSchemaLabel() {
    return schemaLabel;
  }
  String getSchemaDesc() {
    return schemaDesc;
  }
  double getScore() {
    return score;
  }
}


