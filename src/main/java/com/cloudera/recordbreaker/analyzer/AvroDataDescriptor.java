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

import com.cloudera.recordbreaker.hive.HiveSerDe;

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
  public boolean isHiveSupported() {
    return true;
  }
  public void prepareAvroFile(FileSystem srcFs, FileSystem dstFs, Path dst, Configuration conf) throws IOException {
    FileUtil.copy(srcFs, getFilename(), dstFs, dst, false, true, conf);
  }
  
  public String getHiveCreateTableStatement(String tablename) {
    SchemaDescriptor sd = this.getSchemaDescriptor().get(0);
    Schema parentS = sd.getSchema();
    List<Schema> unionFreeSchemas = SchemaUtils.getUnionFreeSchemasByFrequency(sd, 100, true);
    String escapedSchemaString = unionFreeSchemas.get(0).toString();
    escapedSchemaString = escapedSchemaString.replace("'", "\\'");

    String creatTxt = "create table " + tablename + " ROW FORMAT SERDE '" + getHiveSerDeClassName() + "' " +
      "STORED AS " + getStorageFormatString(unionFreeSchemas.get(0));
    return creatTxt;
  }
  public String getStorageFormatString(Schema targetSchema) {
    String escapedSchemaString = targetSchema.toString().replace("'", "\\'");
    return "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
      "TBLPROPERTIES('avro.schema.literal'='" + escapedSchemaString + "')";
  }
  public String getHiveSerDeClassName() {
    return "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  }
}