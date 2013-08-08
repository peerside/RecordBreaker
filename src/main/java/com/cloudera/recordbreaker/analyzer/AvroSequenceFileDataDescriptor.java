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

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.avro.hadoop.io.AvroSequenceFile;

import org.apache.hadoop.conf.Configuration;

/*****************************************************
 * <code>AvroSequenceFileDataDescriptor</code> describes the
 * special case of Avro data stored in a SequenceFile format.
 * We convert the data to standard Avro for querying by Hive/Impala.
 *
 * @author Michael Cafarella
 *****************************************************/
public class AvroSequenceFileDataDescriptor extends GenericDataDescriptor {
  final public static String AVROSEQFILE_TYPE = "avrosequencefile";

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

  public AvroSequenceFileDataDescriptor(Path p, FileSystem fs) throws IOException {
    super(p, fs, AVROSEQFILE_TYPE);
    schemas.add(new AvroSequenceFileSchemaDescriptor(this));
  }

  public AvroSequenceFileDataDescriptor(Path p, FileSystem fs, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, AVROSEQFILE_TYPE, schemaReprs, schemaDescs, schemaBlobs);
  }

  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    return new AvroSequenceFileSchemaDescriptor(this, schemaRepr);
  }

  ///////////////////////////////////
  // GenericDataDescriptor
  //////////////////////////////////
  public boolean isHiveSupported() {
    return false;
  }
  public void prepareAvroFile(FileSystem srcFs, FileSystem dstFs, Path dst, Configuration conf) throws IOException {
    throw new IOException("AvroSeq-to-Avro conversion yet implemented");
  }
}
  
