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

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import au.com.bytecode.opencsv.CSVParser;

import com.cloudera.recordbreaker.hive.HiveSerDe;

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
  public String getHiveCreateTableStatement(String tablename) {
    SchemaDescriptor sd = this.getSchemaDescriptor().get(0);
    Schema parentS = sd.getSchema();
    List<Schema> unionFreeSchemas = SchemaUtils.getUnionFreeSchemasByFrequency(sd, 100, true);
    String escapedSchemaString = unionFreeSchemas.get(0).toString();
    escapedSchemaString = escapedSchemaString.replace("'", "\\'");

    StringBuffer creatTxt = new StringBuffer("create external table " + tablename + "(");
    Schema s = unionFreeSchemas.get(0);
    List<Schema.Field> fields = s.getFields();
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field f = fields.get(i);
      creatTxt.append(f.name() + " " + schemaTypeToString(f.schema().getType()));
      if (i < fields.size()-1) {
        creatTxt.append(", ");
      }
    }
    creatTxt.append(") ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    return creatTxt.toString();
  }

  String schemaTypeToString(Schema.Type st) {
    if ((st == Schema.Type.INT) ||
        (st == Schema.Type.LONG)) {
      return "int";
    } else if ((st == Schema.Type.FLOAT) ||
               (st == Schema.Type.DOUBLE)) {
      return "double";
    } else {
      return "String";
    }
  }

  public String getHiveImportDataStatement(String tablename) {
    String fname = getFilename().toString();
    String localMarker = "";
    if (fname.startsWith("file")) {
      localMarker = "local ";
    }
    String loadTxt = "load data " + localMarker + "inpath '" + getFilename() + "' overwrite into table " + tablename;
    return loadTxt;
  }
  
  public boolean isHiveSupported() {
    return true;
  }
  public String getHiveSerDeClassName() {
    return "com.cloudera.recordbreaker.hive.CSVSerDe";
  }
}
