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
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.hadoop.io.AvroDatumConverter;
import org.apache.avro.hadoop.io.AvroDatumConverterFactory;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.cloudera.recordbreaker.analyzer.CSVRowParser;

/***************************************************************
 * The <code>SequenceFileSerDe</code> deserializes binary SequenceFile
 * data into Avro tuples that Hive can process
 *
 * @author "Michael Cafarella"
 ***************************************************************/
public class SequenceFileSerDe extends HiveSerDe {
  private static final Log LOG = LogFactory.getLog(SequenceFileSerDe.class);
  AvroDatumConverter valADC = null;

  void initDeserializer(String valClassNamePayload) {
    // none necessary
    AvroDatumConverterFactory adcFactory = new AvroDatumConverterFactory(new Configuration());
    try {
      this.valADC = adcFactory.create(Class.forName(valClassNamePayload));
    } catch (ClassNotFoundException cnfe) {
      cnfe.printStackTrace();
    }
  }

  GenericData.Record deserializeRowBlob(Writable blob) {
    if (valADC != null) {
      GenericData.Record cur = new GenericData.Record(schema);
      cur.put("val", this.valADC.convert(blob));
      return cur;
    } else {
      return null;
    }
  }
}