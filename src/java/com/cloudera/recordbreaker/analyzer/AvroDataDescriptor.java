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
import java.util.List;
import java.util.ArrayList;

import org.apache.avro.Schema;

/****************************************************************
 * <code>AvroDataDescriptor</code> is for characterizing preexisting Avro files.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see DataDescriptor
 *****************************************************************/
public class AvroDataDescriptor implements DataDescriptor {
  File f;
  
  /**
   * Creates a new <code>AvroDataDescriptor</code> instance.
   */
  public AvroDataDescriptor(File f) {
    this.f = f;
  }

  /**
   * @return a <code>File</code> 
   */
  public File getFilename() {
    return this.f;
  }

  /**
   * @return a <code>String</code> value for the filetype
   */
  public String getFileTypeIdentifier() {
    return "avro";
  }

  /**
   * <code>getSchemaDescriptor</code> is tailored to return Avro-specific Schema data.
   * There's not much file analysis that has to take place.
   *
   * @return a <code>List<SchemaDescriptor></code> value
   */
  public List<SchemaDescriptor> getSchemaDescriptor() {
    List<SchemaDescriptor> results = new ArrayList<SchemaDescriptor>();
    try {
      results.add(new AvroSchemaDescriptor(f));
    } catch (IOException iex) {
    }
    return results;
  }
}