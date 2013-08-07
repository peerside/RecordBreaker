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

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.hadoop.io.AvroSequenceFile;
import org.apache.avro.hadoop.io.AvroSequenceFile.Reader;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;


/**************************************************************
 * <code>AvroSequenceFileSchemaDescriptor</code> is for capturing Hadoop
 * AvroSequenceFile schema metadata
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 * @version 1.0
 * @since 1.0
 **************************************************************/
public class AvroSequenceFileSchemaDescriptor extends GenericSchemaDescriptor {
  public static String SCHEMA_ID = "avrosequencefile";
  
  public AvroSequenceFileSchemaDescriptor(DataDescriptor dd) throws IOException {
    super(dd);
  }
  public AvroSequenceFileSchemaDescriptor(DataDescriptor dd, String schemaRepr) throws IOException {
    super(dd, schemaRepr);
  }

  void computeSchema() throws IOException {
    try {
      AvroSequenceFile.Reader.Options options = new AvroSequenceFile.Reader.Options()
        .withFileSystem(FSAnalyzer.getInstance().getFS())
        .withInputPath(dd.getFilename())
        .withConfiguration(new Configuration());
      AvroSequenceFile.Reader in = new AvroSequenceFile.Reader(options);
      try {
        //
        // Look for the Avro metadata key in the SequenceFile.  This will encode the Avro schema.
        //
        SequenceFile.Metadata seqFileMetadata = in.getMetadata();
        TreeMap<Text, Text> kvs = seqFileMetadata.getMetadata();
        Text keySchemaStr = kvs.get(AvroSequenceFile.METADATA_FIELD_KEY_SCHEMA);
        Text valSchemaStr = kvs.get(AvroSequenceFile.METADATA_FIELD_VALUE_SCHEMA);
        Schema keySchema = Schema.parse(keySchemaStr.toString());
        Schema valSchema = Schema.parse(valSchemaStr.toString());

        //
        // Build a "pair record" with "key" and "value" fields to hold the subschemas.
        //
        List<Schema.Field> fieldList = new ArrayList<Schema.Field>();        
        fieldList.add(new Schema.Field("key", keySchema, "", null));
        fieldList.add(new Schema.Field("val", valSchema, "", null));
        this.schema = Schema.createRecord(fieldList);
      } finally {
        in.close();
      }
    } catch (IOException iex) {
    }
  }

  /**
   * Return object that iterates through the schema-conformant rows of the file.
   * Return instances of Avro's GenericRecord.
   */
  public Iterator getIterator() {
    return new Iterator() {
      Object nextElt = null;
      AvroSequenceFile.Reader reader = null;
      AvroSequenceFile.Reader.Options options = new AvroSequenceFile.Reader.Options()
        .withFileSystem(FSAnalyzer.getInstance().getFS())
        .withInputPath(dd.getFilename())
        .withConfiguration(new Configuration());
      {
        try {
          reader = new AvroSequenceFile.Reader(options);
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
        if (reader != null) {
          nextElt = lookahead();
        }
        return toReturn;
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
      Object lookahead() {
        try {
          AvroKey k = new AvroKey();
          AvroValue v = new AvroValue();

          k = (AvroKey) reader.next((Object) null);
          if (k != null) {
            v = (AvroValue) reader.getCurrentValue((Object) v);              
            GenericData.Record cur = new GenericData.Record(schema);
            cur.put("key", k.datum());
            cur.put("val", v.datum());
            return cur;
          } else {
            reader.close();
            reader = null;
            return null;
          }
        } catch (IOException iex) {
          iex.printStackTrace();
        }
        return null;
      }
    };
  }

  /**
   * Human-readable string that summarizes the file's structure
   */
  public String getSchemaSourceDescription() {
    return SCHEMA_ID;
  }

  /**
  public static void main(String argv[]) throws IOException {
    if (argv.length < 2) {
      System.err.println("AvroSequenceFileSchemaDescriptor (-test <file>|-write <out>|-read <in>)");
      return;
    }

    if ("-test".equals(argv[0])) {
      System.err.println("Test SequenceFileSchemaDescriptor");
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);
      Path p = new Path(argv[1]);
      AvroSequenceFileSchemaDescriptor sd = new AvroSequenceFileSchemaDescriptor(fs, p);
      Schema s = sd.getSchema();
      System.err.println("Got schema: " + s);
      System.err.println();

      for (Iterator it = sd.getIterator(); it.hasNext(); ) {
        GenericData.Record pair = (GenericData.Record) it.next();
        System.err.println("Got back pair: " + pair);
      }

    } else if ("-write".equals(argv[0])) {
      Path p = new Path(argv[1]);
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);
      Schema keySchema = Schema.create(Schema.Type.STRING);

      List<Schema.Type> schemaTypes = new ArrayList<Schema.Type>();
      schemaTypes.add(Schema.Type.INT);
      schemaTypes.add(Schema.Type.INT);

      List<Schema.Field> schemaFields = new ArrayList<Schema.Field>();
      schemaFields.add(new Schema.Field("height", Schema.create(Schema.Type.DOUBLE), "test", null));
      schemaFields.add(new Schema.Field("age", Schema.create(Schema.Type.INT), "test", null));
      schemaFields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), "test", null));

      //Schema valSchema = Schema.createRecord(schemaFields);
      Schema valSchema = Schema.createRecord("base", "doc", "", false);
      valSchema.setFields(schemaFields);

      AvroSequenceFile.Writer.Options options = new AvroSequenceFile.Writer.Options()
        .withFileSystem(fs)
        .withOutputPath(p)
        .withKeySchema(keySchema)
        .withValueSchema(valSchema)        
        .withConfiguration(new Configuration());
      AvroSequenceFile.Writer out = new AvroSequenceFile.Writer(options);
      try {
        for (int i = 0; i < 10; i++) {
          AvroKey k = new AvroKey();
          k.datum(new Utf8("Hello"));

          AvroValue v = new AvroValue();
          GenericData.Record vdatum = new GenericData.Record(valSchema);
          vdatum.put("height", new Double(33.4));
          vdatum.put("age", new Integer(38));
          vdatum.put("name", new Utf8("Foo Bar"));          
          v.datum(vdatum);
          out.append(k, v);
        }
      } finally {
        out.close();
      }
    } else if ("-read".equals(argv[0])) {
      Path p = new Path(argv[1]);
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);
      
      AvroSequenceFile.Reader.Options options = new AvroSequenceFile.Reader.Options()
        .withFileSystem(fs)
        .withInputPath(p)
        .withConfiguration(new Configuration());
      AvroSequenceFile.Reader in = new AvroSequenceFile.Reader(options);

      try {
        AvroKey k = new AvroKey();
        AvroValue v = new AvroValue();
        SequenceFile.Metadata seqFileMetadata = in.getMetadata();
        TreeMap<Text, Text> kvs = seqFileMetadata.getMetadata();
        Text keySchemaStr = kvs.get(AvroSequenceFile.METADATA_FIELD_KEY_SCHEMA);
        Text valSchemaStr = kvs.get(AvroSequenceFile.METADATA_FIELD_VALUE_SCHEMA);
        Schema keySchema = Schema.parse(keySchemaStr.toString());
        Schema valueSchema = Schema.parse(valSchemaStr.toString());

        System.err.println("Read key schema: " + keySchema);
        System.err.println("Read val schema: " + valueSchema);
        
        while (k != null) {
          k = (AvroKey) in.next((Object) null);
          if (k != null) {
            v = (AvroValue) in.getCurrentValue((Object) v);            
            System.err.println("k=" + k.datum() + ", v=" + v.datum());
          }
        }
      } finally {
        in.close();
      }
    } else {
      System.err.println("Did not recognize params.");
    }
  }
  **/      
}
