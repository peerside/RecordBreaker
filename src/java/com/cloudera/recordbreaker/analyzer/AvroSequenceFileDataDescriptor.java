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
import java.util.TreeMap;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.hadoop.io.AvroSequenceFile;
import org.apache.avro.hadoop.io.AvroSequenceFile.Reader;

/************************************************************
 * AvroSequenceFileDataDescriptor is for SequenceFiles that contain Avro metadata
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 * @see DataDescriptor
 ************************************************************/
public class AvroSequenceFileDataDescriptor implements DataDescriptor {
  FileSystem fs;
  Path p;

  /**
   * Test whether this is an AvroSequenceFile or not.
   */
  public static boolean isAvroSequenceFile(FileSystem fs, Path p) {
    try {
      SequenceFile.Reader in = new SequenceFile.Reader(fs, p, new Configuration());
      try {
        SequenceFile.Metadata seqFileMetadata = in.getMetadata();
        TreeMap<Text, Text> kvs = seqFileMetadata.getMetadata();
        if (kvs.get(AvroSequenceFile.METADATA_FIELD_KEY_SCHEMA) != null &&
            kvs.get(AvroSequenceFile.METADATA_FIELD_VALUE_SCHEMA) != null) {
          return true;
        } else {
          return false;
        }
      } finally {
        in.close();
      }
    } catch (IOException iex) {
      return false;
    }
  }

  public AvroSequenceFileDataDescriptor(FileSystem fs, Path p) {
    this.fs = fs;
    this.p = p;
  }

  public Path getFilename() {
    return p;
  }

  public String getFileTypeIdentifier() {
    return "avrosequencefile";
  }

  /**
   * <code>getSchemaDescriptor</code> returns SequenceFile-specific schema info.
   */
  public List<SchemaDescriptor> getSchemaDescriptor() {
    List<SchemaDescriptor> results = new ArrayList<SchemaDescriptor>();
    try {
      results.add(new AvroSequenceFileSchemaDescriptor(fs, p));
    } catch (IOException iex) {
    }
    return results;
  }
}
