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

import com.cloudera.recordbreaker.hive.borrowed.AvroDeserializer;
import com.cloudera.recordbreaker.hive.borrowed.AvroGenericRecordWritable;
import com.cloudera.recordbreaker.hive.borrowed.AvroObjectInspectorGenerator;

/********************************************************
 * Superclass <code>HiveSerDe</code> provides the infrastructure for
 * datatype-specific Hive SerDes that support in-Fisheye query processing.
 *
 * @author "Michael Cafarella"
 ********************************************************/
public abstract class HiveSerDe implements SerDe {
  final public static String DESERIALIZER = "deserializerinfo";
  final public static String TARGET_SCHEMA = "schemainfo";
  private static final Log LOG = LogFactory.getLog(HiveSerDe.class);

  Schema schema;
  List<String> columnNames;
  List<TypeInfo> columnTypes;
  ObjectInspector oi;
  AvroDeserializer avroDeserializer;  

  /**
   * <code>initialize</code> loads in the appropriate schema for this
   * datafile.  It also loads in deserializer info and passes it to a
   * subclass' "initDeserializer" method.
   *
   * Finally, it creates a lot of objects (column names, types,
   * avro deserializers) that we need to provide info to Hive, regardless
   * of what kind of data we're reading in.
   */
  public void initialize(Configuration conf, Properties tbl) {
    String targetSchemaRepr = tbl.getProperty(TARGET_SCHEMA);
    this.schema = Schema.parse(targetSchemaRepr);
    try {
      AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(schema);
      this.columnNames = aoig.getColumnNames();
      this.columnTypes = aoig.getColumnTypes();
      this.oi = aoig.getObjectInspector();
    } catch (SerDeException sde) {
      sde.printStackTrace();
    }
    this.avroDeserializer = new AvroDeserializer();
    initDeserializer(tbl.getProperty(DESERIALIZER));
  }

  /**
   * initDeserializer() and deserializeRowBlob() are subclass-specific
   */
  abstract void initDeserializer(String s);
  abstract GenericData.Record deserializeRowBlob(Writable blob);

  /**
   * Standard Hive deserialization object
   */
  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
  }

  /**
   * Fisheye translates everything to Avro before returning to Hive
   */
  public Class<? extends Writable> getSerializedClass() {
    return AvroGenericRecordWritable.class;
  }

  /**
   * Deserialize a row description.  Most of the interesting work is done
   * via a call to the subclass' deserializeRowBlob() method.  The
   * code below does some housekeeping work with the avroDeserializer
   * object.
   */
  public Object deserialize(Writable blob) throws SerDeException {
    GenericData.Record rowRecord = deserializeRowBlob(blob);
    if (rowRecord == null) {
      return null;
    }
    Schema curSchema = rowRecord.getSchema();
    if (curSchema.toString().hashCode() != schema.toString().hashCode()) {
      LOG.error("ROW has schema " + curSchema.toString());
      return null;
    }
    return avroDeserializer.deserialize(columnNames, columnTypes, new AvroGenericRecordWritable(rowRecord), schema);
    
  }
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new SerDeException("Cannot serialize to Fisheye-parsed objects");
  }
  public SerDeStats getSerDeStats() {
    return null;
  }
}