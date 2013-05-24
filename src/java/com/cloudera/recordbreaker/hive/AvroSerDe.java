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

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import com.cloudera.recordbreaker.hive.HiveSerDe;

/**********************************************************************
 * <code>AvroSerDe</code> parses Avro data for querying by Hive.
 *
 * @author "Michael Cafarella" 
 **********************************************************************/
public class AvroSerDe extends HiveSerDe {
  private static final Log LOG = LogFactory.getLog(AvroSerDe.class);

  /**
   * Describe <code>initDeserializer</code> method here.
   */
  public void initDeserializer(String s) {
    // none necessary
    LOG.error("INIT DESERIALIZER");
  }

  /**
   * Describe <code>deserializeRowBlob</code> method here.
   */
  public GenericData.Record deserializeRowBlob(Writable blob) {
    byte[] rawBytes = ((Text) blob).getBytes();
    
    try {
      DataFileStream<GenericData.Record> dfs = new DataFileStream<GenericData.Record>(new DataInputStream(new ByteArrayInputStream(rawBytes)), new GenericDatumReader<GenericData.Record>());
      while (dfs.hasNext()) {
        return (GenericData.Record) dfs.next();
      }
    } catch (IOException iex) {
      iex.printStackTrace();
    }
    return null;
  }
}
