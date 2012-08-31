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
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import au.com.bytecode.opencsv.CSVParser;

/*************************************************************************************
 * <code>CSVDataDescriptor</code> is a DataDescriptor for comma-separated-value files.
 *
 * These often come from spreadsheets or simple sensor devices.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see DataDescriptor
 **************************************************************************************/
public class CSVDataDescriptor implements DataDescriptor {
  private static int MAX_LINES = 25;
  private static int MIN_MEAN_ELTS = 3;
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

        if (meanEltCount >= MIN_MEAN_ELTS && ((stddev / meanEltCount) < MAX_ALLOWABLE_LINE_STDDEV)) {
          return true;
        }
      } finally {
        in.close();
      }
    } catch (IOException ie) {
    }
    return false;
  }

  FileSystem fs;
  Path p;
  
  /**
   * Creates a new <code>CSVDataDescriptor</code> instance.
   *
   * @param f a <code>File</code> value
   * @exception IOException if an error occurs
   */
  public CSVDataDescriptor(FileSystem fs, Path p) throws IOException {
    this.fs = fs;
    this.p = p;
  }

  /**
   * @return the <code>File</code>.
   */
  public Path getFilename() {
    return this.p;
  }

  /**
   * @return a <code>String</code> value of 'csv'
   */
  public String getFileTypeIdentifier() {
    return "csv";
  }

  /**
   * Build a SchemaDescriptor for the CSV file, if possible.
   * At the very least, this figures out the columnar types and synthesizes
   * names.  But if topic-specific schema labels are available, we grab
   * those instead of using syn-labels.
   *
   * @return a <code>List<SchemaDescriptor></code> value
   */
  public List<SchemaDescriptor> getSchemaDescriptor() {
    List<SchemaDescriptor> results = new ArrayList<SchemaDescriptor>();
    try {
      results.add(new CSVSchemaDescriptor(fs, p));
    } catch (IOException iex) {
    }
    return results;
  }
}