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

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

/************************************************************************
 * <code>AvroSchemaDescriptor</code> returns Avro-specific Schema data.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see SchemaDescriptor
 *************************************************************************/
public class AvroSchemaDescriptor implements SchemaDescriptor {
  FileSystem fs;
  Path p;
  Schema schema;
  
  /**
   * Creates a new <code>AvroSchemaDescriptor</code> instance.
   * In particular, it loads the Avro file and grabs the Schema object.
   */
  public AvroSchemaDescriptor(FileSystem fs, Path p) throws IOException {
    this.fs = fs;
    this.p = p;
    //DataFileReader<Void> reader = new DataFileReader<Void>(fs, new GenericDatumReader<Void>());
    DataFileStream<Void> reader = new DataFileStream<Void>(fs.open(p), new GenericDatumReader<Void>());    
    try {
      this.schema = reader.getSchema();
    } finally {
      reader.close();
    }
  }

  /**
   * @return the <code>Schema</code> value
   */
  public Schema getSchema() {
    return schema;
  }
  
  /**
   */
  public Iterator getIterator() {
    return new Iterator() {
      Object nextElt = null;
      //DataFileReader<Void> reader = null;
      DataFileStream<Void> reader = null;      
      {
        try {
          //reader = new DataFileReader<Void>(f, new GenericDatumReader<Void>());
          reader = new DataFileStream<Void>(fs.open(p), new GenericDatumReader<Void>());          
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
   * @return a <code>String</code> that uniquely identifies the schema
   */
  public String getSchemaIdentifier() {
    return schema.toString();
  }

  /**
   * @return a <code>String</code> that annotates the schema
   */
  public String getSchemaSourceDescription() {
    return "avro";
  }
}