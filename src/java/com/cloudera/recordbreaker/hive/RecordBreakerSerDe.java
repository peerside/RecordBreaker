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
package com.cloudera.recordbreaker.hive;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.io.File;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericContainer;

import com.cloudera.recordbreaker.learnstructure.InferredType;
import com.cloudera.recordbreaker.hive.borrowed.AvroDeserializer;
import com.cloudera.recordbreaker.hive.borrowed.AvroGenericRecordWritable;
import com.cloudera.recordbreaker.hive.borrowed.AvroObjectInspectorGenerator;

/**********************************************************************
 * <code>RecordBreakerSerDe</code> parses a text file using a parser
 * and schema induced by RecordBreaker.
 *
 * @author "Michael Cafarella" 
 **********************************************************************/
public class RecordBreakerSerDe implements SerDe {
  final public static String DESERIALIZER = "recordbreaker.typetree";
  final public static String TARGET_SCHEMA = "recordbreaker.schema";  

  private static final Log LOG = LogFactory.getLog(RecordBreakerSerDe.class);
  
  InferredType typeTree;
  Schema schema;
  List<String> columnNames;
  List<TypeInfo> columnTypes;
  ObjectInspector oi;
  AvroDeserializer avroDeserializer;  
  
  /**
   * <code>initialize</code> sets up the RecordBreaker SerDe.
   * Importantly, it deserializes the text scanner, type signature, and
   * column names.
   *
   * @param conf a <code>Configuration</code> value
   * @param tbl a <code>Properties</code> value
   */
  public void initialize(Configuration conf, Properties tbl) {
    String recordBreakerTypeTreePayload = tbl.getProperty(DESERIALIZER);
    String targetAvroSchemaRepr = tbl.getProperty(TARGET_SCHEMA);

    try {
      DataInputStream in = new DataInputStream(new FileInputStream(new File(recordBreakerTypeTreePayload)));
      this.typeTree = InferredType.readType(in);
    } catch (UnsupportedEncodingException uee) {
      uee.printStackTrace();
    } catch (IOException iex) {
      iex.printStackTrace();
    }
    this.schema = Schema.parse(targetAvroSchemaRepr);
    
    try {
      AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(schema);
      this.columnNames = aoig.getColumnNames();
      this.columnTypes = aoig.getColumnTypes();
      this.oi = aoig.getObjectInspector();
    } catch (SerDeException sde) {
      sde.printStackTrace();
    }
    LOG.error("LOG query on schema " + schema.toString());
    this.avroDeserializer = new AvroDeserializer();
  }

  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
  }

  public Class<? extends Writable> getSerializedClass() {
    return AvroGenericRecordWritable.class;
  }

  public Object deserialize(Writable blob) throws SerDeException {
    // Using the RecordBreaker InferredType parser, parse the text into an Avro object.
    String rowStr = ((Text) blob).toString();
    GenericRecord resultObj = (GenericRecord) typeTree.parse(rowStr);

    // If not a match, return null    
    if (resultObj == null) {
      return null;
    }
    // Check to see if the resulting Avro object matches the Schema we want for
    // the current Hive table.
    Schema curSchema = resultObj.getSchema();
    if (curSchema.toString().hashCode() != schema.toString().hashCode()) {
      LOG.error("ROW has schema " + curSchema.toString());
      return null;
    }

    // TEST HERE.  IF FAIL, THEN RETURN NULL
    LOG.error("EMIT: " + resultObj);

    // Translate the Avro object into format that Hive wants
    return avroDeserializer.deserialize(columnNames, columnTypes, new AvroGenericRecordWritable(resultObj), schema);
  }

  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new SerDeException("Cannot serialize to RecordBreaker-parsed objects");
  }

  public SerDeStats getSerDeStats() {
    return null;
  }
}