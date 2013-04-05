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

import com.cloudera.recordbreaker.learnstructure.InferredType;

/**********************************************************************
 * <code>RecordBreakerSerDe</code> parses a text file using a parser
 * and schema induced by RecordBreaker.
 *
 * @author "Michael Cafarella" 
 **********************************************************************/
public class RecordBreakerSerDe extends HiveSerDe {
  private static final Log LOG = LogFactory.getLog(RecordBreakerSerDe.class);
  InferredType typeTree;

  /**
   * <code>initDeserializer</code> sets up the RecordBreaker-specific
   * (that is, specific to UnknownText) parts of the SerDe.  In particular,
   * it loads in the text scanner description.
   */
  void initDeserializer(String recordBreakerTypeTreePayload) {
    try {
      DataInputStream in = new DataInputStream(new FileInputStream(new File(recordBreakerTypeTreePayload)));
      this.typeTree = InferredType.readType(in);
    } catch (UnsupportedEncodingException uee) {
      uee.printStackTrace();
    } catch (IOException iex) {
      iex.printStackTrace();
    }
  }

  /**
   * Deserialize a single line of text in the raw input.
   * Transform into a GenericData.Record object for Hive.
   */
  GenericData.Record deserializeRowBlob(Writable blob) {
    String rowStr = ((Text) blob).toString();
    return (GenericData.Record) typeTree.parse(rowStr);
  }
}