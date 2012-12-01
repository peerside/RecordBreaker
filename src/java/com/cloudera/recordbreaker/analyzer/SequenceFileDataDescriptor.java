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

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

/**************************************************************
 * <code>SequenceFileDataDescriptor</code> is for capturing Hadoop SequenceFile key-val stores
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 * @version 1.0
 * @since 1.0
 **************************************************************/
public class SequenceFileDataDescriptor implements DataDescriptor {
  FileSystem fs;
  Path p;

  /**
   * Test whether this is a SequenceFile or not.
   */
  public static boolean isSequenceFile(FileSystem fs, Path p) {
    try {
      SequenceFile.Reader in = new SequenceFile.Reader(fs, p, new Configuration());
      try {
        return true;
      } finally {
        in.close();
      }
    } catch (IOException iex) {
      return false;
    }
  }
  
  public SequenceFileDataDescriptor(FileSystem fs, Path p) {
    this.fs = fs;
    this.p = p;
  }

  public Path getFilename() {
    return p;
  }

  public String getFileTypeIdentifier() {
    return "sequencefile";
  }

  /**
   * <code>getSchemaDescriptor</code> returns SequenceFile-specific schema info.
   */
  public List<SchemaDescriptor> getSchemaDescriptor() {
    List<SchemaDescriptor> results = new ArrayList<SchemaDescriptor>();
    try {
      results.add(new SequenceFileSchemaDescriptor(fs, p));
    } catch (IOException iex) {
    }
    return results;
  }
}
