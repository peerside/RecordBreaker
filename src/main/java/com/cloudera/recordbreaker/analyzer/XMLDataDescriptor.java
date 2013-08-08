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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;

import org.apache.hadoop.conf.Configuration;

/*****************************************************
 * <code>XMLDataDescriptor</code> describes data that
 * was found in XML in the wild.  We convert the data to
 * Avro for querying by Hive/Impala.  This only works for
 * 'simple' XML files that resemble lists of records.  (Think: items
 * that can be easily parsed using SAX rather than a full DOM parser.)
 *
 * @author Michael Cafarella
 *****************************************************/
public class XMLDataDescriptor extends GenericDataDescriptor {
  final public static String XML_TYPE = "xml";
  
  public XMLDataDescriptor(Path p, FileSystem fs) throws IOException {
    super(p, fs, XML_TYPE);
    schemas.add(new XMLSchemaDescriptor(this));
  }

  public XMLDataDescriptor(Path p, FileSystem fs, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, XML_TYPE, schemaReprs, schemaDescs, schemaBlobs);
  }

  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    return new XMLSchemaDescriptor(this, schemaRepr, blob);
  }

  ///////////////////////////////////
  // GenericDataDescriptor
  //////////////////////////////////
  public boolean isHiveSupported() {
    return false;
  }
  public void prepareAvroFile(FileSystem srcFs, FileSystem dstFs, Path dst, Configuration conf) throws IOException {
    throw new IOException("XML-to-Avro conversion yet implemented");
  }
}
  
