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
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.avro.Schema;

/***********************************************************************
 * Describe class <code>TextRegexpDataDescriptor</code> here.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see DataDescriptor
 ***********************************************************************/
public class TextRegexpDataDescriptor implements DataDescriptor {
  FileSystem fs;
  Path p;
  String typeIdentifier;
  List<Pattern> regexps;
  List<Schema> schemas;

  /**
   * Creates a new <code>TextRegexpDataDescriptor</code> instance,
   * tailored to the structure described in the param list.
   *
   * @param typeIdentifier a <code>String</code> value
   * @param regexps a <code>List<Pattern></code> value
   * @param schemas a <code>List<Schema></code> value
   */
  public TextRegexpDataDescriptor(String typeIdentifier, List<Pattern> regexps, List<Schema> schemas) {
    this.typeIdentifier = typeIdentifier;
    this.regexps = regexps;
    this.schemas = schemas;
  }

  /**
   * Create a new instance of this descriptor with the same structural params,
   * but tailored for the input File object.
   */
  public TextRegexpDataDescriptor cloneWithFile(FileSystem fs, Path p) {
    TextRegexpDataDescriptor retItem = new TextRegexpDataDescriptor(typeIdentifier, regexps, schemas);
    retItem.fs = fs;
    retItem.p = p;
    return retItem;
  }

  /**
   * Test whether the input File corresponds to this data format.
   */
  public boolean testData(FileSystem fs, Path p) throws IOException {
    TextRegexpDataDescriptor candidateDataDesc = cloneWithFile(fs, p);
    List<SchemaDescriptor> candidateSchemaDescs = candidateDataDesc.getSchemaDescriptor();

    for (SchemaDescriptor candidateSchemaDesc: candidateSchemaDescs) {
      int maxTests = 100;
      int observedItems = 0;
      int numTests = 0;
      for (Iterator it = candidateSchemaDesc.getIterator(); it.hasNext() && numTests < maxTests; numTests++) {
        Object obj = it.next();
        if (obj != null) {
          observedItems++;
        }
      }
      double ratio = observedItems / (1.0 * numTests);
      if (ratio >= 0.3) {
        return true;
      }
    }
    return false;
  }
  
  ///////////////////////////////////
  // DataDescriptor
  //////////////////////////////////
  public Path getFilename() {
    return this.p;
  }
  public String getFileTypeIdentifier() {
    return typeIdentifier;
  }
  
  /**
   * Create a <code>SchemaDescriptor</code> that's tailored for
   * this text format and file.
   */
  public List<SchemaDescriptor> getSchemaDescriptor() {
    List<SchemaDescriptor> results = new ArrayList<SchemaDescriptor>();
    try {
      results.add(new TextRegexpSchemaDescriptor(fs, p, typeIdentifier, regexps, schemas));
    } catch (Exception iex) {
    }
    return results;
  }
}