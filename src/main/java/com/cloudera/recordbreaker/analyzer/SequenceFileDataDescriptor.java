/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
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

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.recordbreaker.hive.HiveSerDe;

/*****************************************************
 * <code>SequenceFileDataDescriptor</code> describes SequenceFile
 * data in the wild.  We iterate through it by returning Avro instances.
 *
 * @author Michael Cafarella
 *****************************************************/
public class SequenceFileDataDescriptor extends GenericDataDescriptor {
  final public static String SEQFILE_TYPE = "sequencefile";

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
  
  public SequenceFileDataDescriptor(Path p, FileSystem fs) throws IOException {
    super(p, fs, SEQFILE_TYPE);
    schemas.add(new SequenceFileSchemaDescriptor(this));
  }

  public SequenceFileDataDescriptor(Path p, FileSystem fs, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, SEQFILE_TYPE, schemaReprs, schemaDescs, schemaBlobs);
  }

  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    // We can ignore the schemaid and blob here.
    return new SequenceFileSchemaDescriptor(this, schemaRepr, blob);
  }

  ///////////////////////////////////
  // GenericDataDescriptor
  //////////////////////////////////
  public boolean isHiveSupported() {
    return true;
  }
  public String getStorageFormatString(Schema targetSchema) {
    return "SEQUENCEFILE";
  }
  public String getHiveSerDeClassName() {
    return "com.cloudera.recordbreaker.hive.SequenceFileSerDe";
  }
}