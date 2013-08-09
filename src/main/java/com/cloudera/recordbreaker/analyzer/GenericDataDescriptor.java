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

import org.apache.avro.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.List;

import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import au.com.bytecode.opencsv.CSVParser;

/*****************************************************************
 * Describe class <code>GenericDataDescriptor</code> here.
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 *****************************************************************/
public abstract class GenericDataDescriptor implements DataDescriptor {
  private static final Log LOG = LogFactory.getLog(GenericDataDescriptor.class);  
  final public static String AVROSEQFILE_TYPE = "avrosequencefile";
  final private static String HIVE_SERDE_CLASSNAME = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";  

  List<SchemaDescriptor> schemas;
  FileSystem fs;
  Path p;
  String filetype;

  public GenericDataDescriptor(Path p, FileSystem fs, String filetype) throws IOException {
    this.p = p;
    this.fs = fs;
    this.filetype = filetype;
    this.schemas = new ArrayList<SchemaDescriptor>();
  }
  
  public GenericDataDescriptor(Path p, FileSystem fs, String filetype, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    this.fs = fs;
    this.p = p;
    this.filetype = filetype;
    this.schemas = new ArrayList<SchemaDescriptor>();

    for (int i = 0; i < schemaReprs.size(); i++) {
      this.schemas.add(loadSchemaDescriptor(schemaReprs.get(i), schemaDescs.get(i), schemaBlobs.get(i)));
    }
  }

  abstract SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException;

  public Path getFilename() {
    return this.p;
  }
  public String getFileTypeIdentifier() {
    return this.filetype;
  }
  public List<SchemaDescriptor> getSchemaDescriptor() {
    return schemas;
  }
  public InputStream getRawBytes() throws IOException {
    return fs.open(p);
  }

  //////////////////////////
  // Hive Support
  //////////////////////////
  public boolean isHiveSupported() {
    return true;
  }
  public Schema getHiveTargetSchema() {
    SchemaDescriptor sd = this.getSchemaDescriptor().get(0);
    List<Schema> unionFreeSchemas = SchemaUtils.getUnionFreeSchemasByFrequency(sd, 100, true);
    return unionFreeSchemas.get(0);
  }
  public abstract void prepareAvroFile(FileSystem srcFs, FileSystem dstFs, Path dst, Configuration conf) throws IOException;

  public String getHiveCreateTableStatement(String tablename) {
    SchemaDescriptor sd = this.getSchemaDescriptor().get(0);
    Schema parentS = sd.getSchema();
    List<Schema> unionFreeSchemas = SchemaUtils.getUnionFreeSchemasByFrequency(sd, 100, true);
    String escapedSchemaString = unionFreeSchemas.get(0).toString();
    escapedSchemaString = escapedSchemaString.replace("'", "\\'");

    String creatTxt = "create table " + tablename + " ROW FORMAT SERDE '" + HIVE_SERDE_CLASSNAME + "' " +
      "STORED AS " + getStorageFormatString(unionFreeSchemas.get(0));
    return creatTxt;
  }
  
  public String getStorageFormatString(Schema targetSchema) {
    String escapedSchemaString = targetSchema.toString().replace("'", "\\'");
    return "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
      "TBLPROPERTIES('avro.schema.literal'='" + escapedSchemaString + "')";
  }

  public String getHiveImportDataStatement(String tablename, Path importFile) {
    String fname = importFile.toString();
    String localMarker = "";
    if (fname.startsWith("file")) {
      localMarker = "local ";
    }
    String loadTxt = "load data " + localMarker + "inpath '" + importFile + "' overwrite into table " + tablename;
    return loadTxt;
  }
  public String getDeserializerPayload() {
    throw new UnsupportedOperationException("Cannot run Hive queries on file " + getFilename());
  }
}