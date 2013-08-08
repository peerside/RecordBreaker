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
import org.apache.hadoop.fs.FileUtil;

import org.apache.hadoop.conf.Configuration;

/*****************************************************
 * <code>AvroDataDescriptor</code> describes data that
 * was found in Avro format in the wild.  That is, we have not
 * had to translate the data into Avro (as we do for other filetypes
 * before most processing); someone wrote it to HDFS in Avro.
 *
 * @author Michael Cafarella
 *****************************************************/
public class AvroDataDescriptor extends GenericDataDescriptor {
  final public static String AVRO_TYPE = "avro";
  
  public AvroDataDescriptor(Path p, FileSystem fs) throws IOException {
    super(p, fs, AVRO_TYPE);
    schemas.add(new AvroSchemaDescriptor(this));
  }

  public AvroDataDescriptor(Path p, FileSystem fs, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, AVRO_TYPE, schemaReprs, schemaDescs, schemaBlobs);
  }

  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    // We can ignore the schemaid and blob here.
    return new AvroSchemaDescriptor(this, schemaRepr);
  }

  ///////////////////////////////////
  // GenericDataDescriptor
  //////////////////////////////////
  public void prepareAvroFile(FileSystem srcFs, FileSystem dstFs, Path dst, Configuration conf) throws IOException {
    FileUtil.copy(srcFs, getFilename(), dstFs, dst, false, true, conf);
  }
}