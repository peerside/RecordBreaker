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

import org.apache.avro.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.hadoop.io.AvroSequenceFile;

import au.com.bytecode.opencsv.CSVParser;

/*****************************************************************
 * Describe class <code>GenericDataDescriptor</code> here.
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 *****************************************************************/
public class GenericDataDescriptor implements DataDescriptor {
  final public static String CSV_TYPE = "csv";
  final public static String XML_TYPE = "xml";
  final public static String AVRO_TYPE = "avro";
  final public static String AVROSEQFILE_TYPE = "avrosequencefile";
  final public static String SEQFILE_TYPE = "sequencefile";
  
  private static int MAX_LINES = 25;
  private static int MIN_MEAN_ELTS = 3;
  private static int MIN_LINE_COUNT = 10;
  private static double MAX_ALLOWABLE_LINE_STDDEV = 0.1;

  /**
   * Test whether a given file is amenable to CSV processing
   */
  public static boolean isCSV(FileSystem fs, Path p) {
    String fname = p.getName();    
    if (fname.endsWith(".csv")) {
      return true;
    }
    CSVParser parser = new CSVParser();
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(p)));
      try {
        int lineCount = 0;
        List<Integer> observedEltCounts = new ArrayList<Integer>();
        int totalEltCount = 0;
        int minEltCount = Integer.MAX_VALUE;
        int maxEltCount = -1;

        String line = null;
        while (lineCount < MAX_LINES && ((line = in.readLine()) != null)) {
          String parts[] = parser.parseLine(line);
          int numElts = parts.length;
          minEltCount = Math.min(minEltCount, numElts);
          maxEltCount = Math.max(maxEltCount, numElts);
          totalEltCount += numElts;
          observedEltCounts.add(numElts);
        
          lineCount++;
        }
        double meanEltCount = totalEltCount / (1.0 * observedEltCounts.size());
        double totalVariance = 0;
        for (Integer v: observedEltCounts) {
          totalVariance += Math.pow(v - meanEltCount, 2);
        }
        double variance = totalVariance / observedEltCounts.size();
        double stddev = Math.sqrt(variance);
        if (lineCount >= MIN_LINE_COUNT && meanEltCount >= MIN_MEAN_ELTS && ((stddev / meanEltCount) < MAX_ALLOWABLE_LINE_STDDEV)) {
          return true;
        }
      } finally {
        in.close();
      }
    } catch (IOException ie) {
    }
    return false;
  }

  /**
   * Test whether this is an AvroSequenceFile or not.
   */
  public static boolean isAvroSequenceFile(FileSystem fs, Path p) {
    try {
      SequenceFile.Reader in = new SequenceFile.Reader(fs, p, new Configuration());
      try {
        SequenceFile.Metadata seqFileMetadata = in.getMetadata();
        TreeMap<Text, Text> kvs = seqFileMetadata.getMetadata();
        if (kvs.get(AvroSequenceFile.METADATA_FIELD_KEY_SCHEMA) != null &&
            kvs.get(AvroSequenceFile.METADATA_FIELD_VALUE_SCHEMA) != null) {
          return true;
        } else {
          return false;
        }
      } finally {
        in.close();
      }
    } catch (IOException iex) {
      return false;
    }
  }

  /**
   * Test whether this is a SequenceFile or not.
   */
  public static boolean isSequenceFile(FileSystem fs, Path p) {
    try {
      SequenceFile.Reader in = new SequenceFile.Reader(fs, p, new Configuration());
      try {
        return true;
      } finally {
        in.close();
      }
    } catch (IOException iex) {
      return false;
    }
  }

  List<SchemaDescriptor> schemas;
  FileSystem fs;
  Path p;
  String filetype;

  public GenericDataDescriptor(Path p, FileSystem fs, String filetype) throws IOException {
    this.p = p;
    this.fs = fs;
    this.filetype = filetype;
    this.schemas = new ArrayList<SchemaDescriptor>();

    if (AVRO_TYPE.equals(filetype)) {
      schemas.add(new AvroSchemaDescriptor(this));
    } else if (AVROSEQFILE_TYPE.equals(filetype)) {
      schemas.add(new AvroSequenceFileSchemaDescriptor(this));
    } else if (CSV_TYPE.equals(filetype)) {
      schemas.add(new CSVSchemaDescriptor(this));
    } else if (SEQFILE_TYPE.equals(filetype)) {
      schemas.add(new SequenceFileSchemaDescriptor(this));
    } else if (XML_TYPE.equals(filetype)) {
      schemas.add(new XMLSchemaDescriptor(this));
    }
  }
  
  public GenericDataDescriptor(Path p, FileSystem fs, String filetype, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    this.fs = fs;
    this.p = p;
    this.filetype = filetype;
    this.schemas = new ArrayList<SchemaDescriptor>();

    for (int i = 0; i < schemaReprs.size(); i++) {
      this.schemas.add(loadSchemaDescriptor(schemaReprs.get(i), schemaDescs.get(i), schemaBlobs.get(i)));
    }
  }

  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    SchemaDescriptor sd = null;
    if (AvroSchemaDescriptor.SCHEMA_ID.equals(schemaId)) {
      sd = new AvroSchemaDescriptor(this, schemaRepr);      
    } else if (AvroSequenceFileSchemaDescriptor.SCHEMA_ID.equals(schemaId)) {
      sd = new AvroSequenceFileSchemaDescriptor(this, schemaRepr);
    } else if (CSVSchemaDescriptor.SCHEMA_ID.equals(schemaId)) {
      sd = new CSVSchemaDescriptor(this, schemaRepr, blob);
    } else if (SequenceFileSchemaDescriptor.SCHEMA_ID.equals(schemaId)) {
      sd = new SequenceFileSchemaDescriptor(this, schemaRepr);
    } else if (XMLSchemaDescriptor.SCHEMA_ID.equals(schemaId)) {
      sd = new XMLSchemaDescriptor(this, schemaRepr, blob);
    } else {
      throw new IOException("Unrecognized schema descriptor: " + schemaId);
    }
    return sd;
  }

  public Path getFilename() {
    return this.p;
  }
  public String getFileTypeIdentifier() {
    return this.filetype;
  }
  public List<SchemaDescriptor> getSchemaDescriptor() {
    return schemas;
  }
  public InputStream getRawBytes() throws IOException {
    return fs.open(p);
  }
}