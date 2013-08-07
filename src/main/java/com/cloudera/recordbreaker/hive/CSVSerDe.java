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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.recordbreaker.analyzer.CSVRowParser;

/***************************************************************
 * The <code>CSVSerDe</code> deserializes text CSV-format strings
 * into tuples that Hive can process.
 *
 * @author "Michael Cafarella"
 ***************************************************************/
public class CSVSerDe extends HiveSerDe {
  private static final Log LOG = LogFactory.getLog(CSVSerDe.class);
  String headerRowHash = null;
  CSVRowParser rowParser = null;

  void initDeserializer(String payload) {
    this.headerRowHash = payload;
    this.rowParser = new CSVRowParser(schema, headerRowHash);
  }

  GenericData.Record deserializeRowBlob(Writable blob) {
    String rowStr = ((Text) blob).toString();
    if (("" + rowStr.hashCode()).compareTo(headerRowHash) == 0) {
      return null;
    }
    return rowParser.parseRow(rowStr);
  }
}