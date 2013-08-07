/*
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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

import java.util.Iterator;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

/************************************************************************
 * <code>AvroSchemaDescriptor</code> returns Avro-specific Schema data.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see SchemaDescriptor
 *************************************************************************/
public class AvroSchemaDescriptor extends GenericSchemaDescriptor {
  static String SCHEMA_ID = "avro";
  
  /**
   * Creates a new <code>AvroSchemaDescriptor</code> instance.
   * In particular, it loads the Avro file and grabs the Schema object.
   */
  public AvroSchemaDescriptor(DataDescriptor dd) throws IOException {
    super(dd);
  }
  public AvroSchemaDescriptor(DataDescriptor dd, String schemaRepr) throws IOException {
    super(dd, schemaRepr);
  }

  void computeSchema() throws IOException {
    DataFileStream<Void> reader = new DataFileStream<Void>(dd.getRawBytes(), new GenericDatumReader<Void>());    
    try {
      this.schema = reader.getSchema();
    } finally {
      reader.close();
    }
  }

  public byte[] getPayload() {
    return "".getBytes();
  }

  /**
   */
  public Iterator getIterator() {
    return new Iterator() {
      Object nextElt = null;
      DataFileStream<Void> reader = null;      
      {
        try {
          reader = new DataFileStream<Void>(dd.getRawBytes(), new GenericDatumReader<Void>());          
          nextElt = lookahead();
        } catch (IOException iex) {
          this.nextElt = null;
        }
      }
      public boolean hasNext() {
        return nextElt != null;
      }
      public synchronized Object next() {
        Object toReturn = nextElt;
        nextElt = lookahead();
        return toReturn;
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
      Object lookahead() {
        try {
          if (reader.hasNext()) {
            return reader.next();
          }
          reader.close();
        } catch (IOException iex) {
          iex.printStackTrace();
        }
        return null;
      }
    };
  }
  
  /**
   * @return a <code>String</code> that annotates the schema
   */
  public String getSchemaSourceDescription() {
    return SCHEMA_ID;
  }
}