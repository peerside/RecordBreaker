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
package com.cloudera.recordbreaker.analyzer;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import au.com.bytecode.opencsv.CSVParser;

/*****************************************************
 * <code>CSVDataDescriptor</code> describes comma-separated
 * textual data.  Based on previous analysis of the file,
 * we know whether the first line should be treated as
 * schema info or not.
 *
 * @author Michael Cafarella
 *****************************************************/
public class CSVDataDescriptor extends GenericDataDescriptor {
  final public static String CSV_TYPE = "csv";
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

  public CSVDataDescriptor(Path p, FileSystem fs) throws IOException {
    super(p, fs, CSV_TYPE);
    schemas.add(new CSVSchemaDescriptor(this));
  }

  public CSVDataDescriptor(Path p, FileSystem fs, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, CSV_TYPE, schemaReprs, schemaDescs, schemaBlobs);
  }

  public SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    return new CSVSchemaDescriptor(this, schemaRepr, blob);
  }

  ///////////////////////////////////
  // GenericDataDescriptor
  //////////////////////////////////
  public void prepareAvroFile(FileSystem srcFs, FileSystem dstFs, Path dst, Configuration conf) throws IOException {
    // THIS IS WHERE THE MAGIC HAPPENS!!!
    // Convert CSV into Avro!!!!
    SchemaDescriptor sd = this.getSchemaDescriptor().get(0);
    List<Schema> unionFreeSchemas = SchemaUtils.getUnionFreeSchemasByFrequency(sd, 100, true);
    Schema schema = unionFreeSchemas.get(0);

    String headerRowHash = new String(sd.getPayload());
    CSVRowParser rowParser = new CSVRowParser(schema, headerRowHash);

    // Open stream to write out Avro contents
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, dstFs.create(dst, true));
    int numRecords = 0;
    int MAX_RECORDS = 1000;
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(srcFs.open(getFilename())));
      try {
        String rowStr = null;
        while (((rowStr = in.readLine()) != null) && (numRecords < MAX_RECORDS)) {
          if (("" + rowStr.hashCode()).compareTo(headerRowHash) == 0) {
            continue;
          }
          GenericData.Record record = rowParser.parseRow(rowStr);
          if (record == null) {
            continue;
          }
          if (record.getSchema().toString().hashCode() != schema.toString().hashCode()) {
            continue;
          }
          dataFileWriter.append(record);
          numRecords++;
        }
      } finally {
        in.close();
      }
    } finally {
      dataFileWriter.close();
    }
  }
}
