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
import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.BufferedInputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData;

import com.cloudera.recordbreaker.schemadict.SchemaSuggest;
import com.cloudera.recordbreaker.schemadict.DictionaryMapping;
import com.cloudera.recordbreaker.learnstructure.LearnStructure;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.codec.binary.Base64;

/*****************************************************
 * <code>UnknownTextDataDescriptor</code> encapsulates log files with which we are unfamiliar.
 * It is the only DataDescriptor implementation to use the LearnStructure and SchemaDictionary
 * work.
 *
 * @author "Michael Cafarella" 
 ******************************************************/
public class UnknownTextDataDescriptor extends GenericDataDescriptor {
  public static String TEXTDATA_TYPE = "structured-text";
  
  /**
   * Test whether the input param is a text file.
   * We do this by examining the first k bytes.  If 90% or more
   * of them are ASCII chars, then we assume it's text.
   */
  final static double asciiThreshold = 0.9;
  public static boolean isTextData(FileSystem fs, Path p) {
    try {
      BufferedInputStream in = new BufferedInputStream(fs.open(p));
      try {
        byte buf[] = new byte[1024];
        int numBytes = in.read(buf);
        if (numBytes < 0) {
          return false;
        }
        int numASCIIChars = 0;
        for (int i = 0; i < numBytes; i++) {
          if (buf[i] >= 32 && buf[i] < 128) {
            numASCIIChars++;
          }
        }
        return ((numASCIIChars / (1.0 * numBytes)) > asciiThreshold);
      } finally {
        in.close();
      }
    } catch (IOException iex) {
      return false;
    }
  }
  
  File schemaDictDir;
  List<SchemaDescriptor> schemaDescriptors = new ArrayList<SchemaDescriptor>();
  
  /**
   * Creates a new <code>UnknownTextDataDescriptor</code>.
   */
  final static double TUPLE_PCT = 0.75;
  public UnknownTextDataDescriptor(FileSystem fs, Path p, File schemaDictDir) throws IOException {
    super(p, fs, TEXTDATA_TYPE);
    
    this.schemaDictDir = schemaDictDir;
    UnknownTextSchemaDescriptor tsd = new UnknownTextSchemaDescriptor(this);

    // Test if this schema descriptor can parse the file
    boolean hasLatentStructure = false;
    int numTuples = 0;
    for (Iterator it = tsd.getIterator(); it.hasNext(); ) {
      numTuples++;
      it.next();
    }
    int numLines = 0;
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(p)));
    while (in.readLine() != null) {
      numLines++;
    }

    numTuples = Math.min(numTuples, UnknownTextSchemaDescriptor.MAX_LINES);
    numLines = Math.min(numLines, UnknownTextSchemaDescriptor.MAX_LINES);
    if ((numTuples / (1.0 * numLines)) < TUPLE_PCT) {
      throw new IOException("Cannot parse structured text data");
    }
    this.schemas.add(new UnknownTextSchemaDescriptor(this));
  }

  public UnknownTextDataDescriptor(FileSystem fs, Path p, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, TEXTDATA_TYPE, schemaReprs, schemaDescs, schemaBlobs);
  }

  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    return new UnknownTextSchemaDescriptor(this, schemaRepr, blob);
  }
  ///////////////////////////////////
  // GenericDataDescriptor
  //////////////////////////////////
  public void prepareAvroFile(FileSystem srcFs, FileSystem dstFs, Path dst, Configuration conf) throws IOException {
    SchemaDescriptor sd = this.getSchemaDescriptor().get(0);
    List<Schema> unionFreeSchemas = SchemaUtils.getUnionFreeSchemasByFrequency(sd, 100, true);
    Schema schema = unionFreeSchemas.get(0);

    // Open stream to write out Avro contents
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, dstFs.create(dst, true));
    int numRecords = 0;
    int MAX_RECORDS = 1000;
    try {
      for (Iterator it = sd.getIterator(); it.hasNext() && numRecords < MAX_RECORDS; ) {
        GenericData.Record rowRecord = (GenericData.Record) it.next();
        if (rowRecord.getSchema().toString().hashCode() != schema.toString().hashCode()) {
          continue;
        }
        dataFileWriter.append(rowRecord);
        numRecords++;
      }
    } finally {
      dataFileWriter.close();
    }
  }
}