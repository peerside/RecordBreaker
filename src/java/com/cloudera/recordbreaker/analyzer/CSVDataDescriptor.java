/*
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

/*************************************************************************************
 * <code>CSVDataDescriptor</code> is a DataDescriptor for comma-separated-value files.
 *
 * These often come from spreadsheets or simple sensor devices.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see DataDescriptor
 **************************************************************************************/
public class CSVDataDescriptor implements DataDescriptor {
  File f;
  
  /**
   * Creates a new <code>CSVDataDescriptor</code> instance.
   *
   * @param f a <code>File</code> value
   * @exception IOException if an error occurs
   */
  public CSVDataDescriptor(File f) throws IOException {
    this.f = f;
  }

  /**
   * @return the <code>File</code>.
   */
  public File getFilename() {
    return this.f;
  }

  /**
   * @return a <code>String</code> value of 'csv'
   */
  public String getFileTypeIdentifier() {
    return "csv";
  }

  /**
   * Build a SchemaDescriptor for the CSV file, if possible.
   * At the very least, this figures out the columnar types and synthesizes
   * names.  But if topic-specific schema labels are available, we grab
   * those instead of using syn-labels.
   *
   * @return a <code>List<SchemaDescriptor></code> value
   */
  public List<SchemaDescriptor> getSchemaDescriptor() {
    List<SchemaDescriptor> results = new ArrayList<SchemaDescriptor>();
    results.add(new CSVSchemaDescriptor(f));
    return results;
  }
}