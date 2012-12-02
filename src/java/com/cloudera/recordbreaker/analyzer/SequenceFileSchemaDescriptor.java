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

import java.util.Map;
import java.util.List;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;

/**************************************************************
 * <code>SequenceFileSchemaDescriptor</code> is for capturing Hadoop
 * SequenceFile schema metadata
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 * @version 1.0
 * @since 1.0
 **************************************************************/
public class SequenceFileSchemaDescriptor implements SchemaDescriptor {
  FileSystem fs;
  Path p;
  Schema schema;
  Schema keySchema;
  Schema valSchema;
  Class keyClass;
  Class valClass;
  String schemaSrcDesc;

  /**
   * A new <code>SequenceFileSchemaDescriptor</code> takes a path and a filesystem
   */
  public SequenceFileSchemaDescriptor(FileSystem fs, Path p) throws IOException {
    this.fs = fs;
    this.p = p;
    computeSchema();
  }

  /**
   * Figure out the schema for non-Avro SequenceFiles.  Yields a synthesized Avro schema.
   */
  void computeSchema() throws IOException {
    try {
      SequenceFile.Reader in = new SequenceFile.Reader(fs, p, new Configuration());
      try {
        //
        // Build Avro schemas out of the SequenceFile key/val classes.  We will use
        // these them to display schema information to the user.
        //
        this.keyClass = in.getKeyClass();
        this.valClass = in.getValueClass();
        this.keySchema = ReflectData.get().getSchema(keyClass);
        this.valSchema = ReflectData.get().getSchema(valClass);

        //
        // Build a "pair record" with "key" and "value" fields to hold the subschemas.
        //
        List<Schema.Field> fieldList = new ArrayList<Schema.Field>();        
        fieldList.add(new Schema.Field("key", keySchema, "", null));
        fieldList.add(new Schema.Field("val", valSchema, "", null));
        this.schema = Schema.createRecord(fieldList);
        this.schemaSrcDesc = "sequencefile";
      } finally {
        in.close();
      }
    } catch (IOException iex) {
    }
  }

  /**
   * Return the previously-assembled schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Return object that iterates through the schema-conformant rows of the file.
   * Return instances of Avro's GenericRecord.
   */
  public Iterator getIterator() {
    return new Iterator() {
      Object nextElt = null;
      SequenceFile.Reader in = null;
      {
        try {
          in = new SequenceFile.Reader(fs, p, new Configuration());
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
        if (in != null) {
          nextElt = lookahead();
        }
        return toReturn;
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
      Object lookahead() {
        try {
          Writable key = (Writable) keyClass.newInstance();
          Writable val = (Writable) valClass.newInstance();

          if (in.next(key, val)) {
            GenericData.Record cur = new GenericData.Record(schema);
            cur.put("key", key);
            cur.put("val", val);
            return cur;
          }
        } catch (IOException iex) {
          iex.printStackTrace();
        } catch (InstantiationException inex) {
          inex.printStackTrace();          
        } catch (IllegalAccessException illacc) {
          illacc.printStackTrace();          
        }
        in = null;
        return null;
      }
    };
  }

  /**
   * A string that uniquely identifies the file's schema.  Not necessarily
   * human-readable.
   */
  public String getSchemaIdentifier() {
    return schema.toString();
  }

  /**
   * Human-readable string that summarizes the file's structure
   */
  public String getSchemaSourceDescription() {
    return schemaSrcDesc;
  }

  public static void main(String argv[]) throws IOException {
    if (argv.length < 1) {
      System.err.println("SequenceFileSchemaDescriptor (-test <file>|-write <outfile>)");
      return;
    }

    if ("-test".equals(argv[0])) {
      System.err.println("Test SequenceFileSchemaDescriptor");
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);
      Path p = new Path(argv[1]);
      SequenceFileSchemaDescriptor sd = new SequenceFileSchemaDescriptor(fs, p);
      System.err.println("Loaded SequenceFileSchemaDescriptor");
      Schema s = sd.getSchema();
      System.err.println("Got schema: " + s);
      System.err.println();

      for (Iterator it = sd.getIterator(); it.hasNext(); ) {
        GenericData.Record pair = (GenericData.Record) it.next();
        System.err.println(pair);
      }
    } else if ("-write".equals(argv[0])) {
      System.err.println("Write SequenceFile");
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);
      Path p = new Path(argv[1]);

      SequenceFile.Writer outseq = SequenceFile.createWriter(fs,
                                                             conf,
                                                             p,
                                                             IntWritable.class,
                                                             UTF8.class,
                                                             SequenceFile.CompressionType.BLOCK,
                                                             new BZip2Codec());
      try {
        outseq.append(new IntWritable(1), new UTF8("Foo"));
        outseq.append(new IntWritable(2), new UTF8("Bar"));
        outseq.append(new IntWritable(3), new UTF8("Baz"));        
      } finally {
        outseq.close();
      }
    } else {
      System.err.println("Did not recognize params.");
    }
  }
}

