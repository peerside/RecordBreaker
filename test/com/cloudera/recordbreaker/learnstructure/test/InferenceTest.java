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
package com.cloudera.recordbreaker.learnstructure.test;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.rules.TemporaryFolder;

import com.cloudera.recordbreaker.learnstructure.LearnStructure;

/**
 * TestInference tests the LearnStructure component's structure-inference code.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 * @version 1.0
 * @since 1.0
 */
public abstract class InferenceTest {
  private static double MIN_PARSE_RATIO = 0.85;
  private static int MAX_RUNTIME = 30;
  private TemporaryFolder tmpOutDir = new TemporaryFolder();
  private Timeout timer = new Timeout(MAX_RUNTIME);
  static File sampleDir = new File(System.getProperty("test.samples.dir", "src/samples"), "textdata");
  
  /**
   * runSingletonTest() executes LearnStructure test for a single given input text file.
   *
   * @param inputData a <code>File</code> value
   * @return a <code>boolean</code> value;  did the test succeed?
   */
  boolean runSingletonTest(File inputData) {
    File tmpSingletonDir = tmpOutDir.newFolder("testinference-" + inputData.getName());
    File avroFile = new File(tmpSingletonDir, LearnStructure.DATA_FILENAME);
    File schemaFile = new File(tmpSingletonDir, LearnStructure.SCHEMA_FILENAME);
    
    try {
      LearnStructure ls = new LearnStructure();
      try {
        // Infer structure
        ls.inferRecordFormat(inputData, tmpSingletonDir, true);

        // Test the inferred structure
        // First, load in the avro file and see how many records there are.
        int avroCount = 0;
        DataFileReader in = new DataFileReader(avroFile, new GenericDatumReader());
        try {
          Iterator it = in.iterator();
          while (it.hasNext()) {
            avroCount++;
            it.next();
          }
        } finally {
          in.close();
        }

        // Also, check to see how many records exist in the original input
        int lineCount = 0;
        BufferedReader in2 = new BufferedReader(new FileReader(inputData));
        try {
          while (in2.readLine() != null) {
            lineCount++;
          }
        } finally {
          in2.close();
        }

        // Was the synthesized parser able to figure out the file?
        double parseRatio = avroCount / (1.0 * lineCount);
        return (parseRatio > MIN_PARSE_RATIO);
      } catch (IOException e) {
        try {
          System.err.println("File: " + inputData.getCanonicalPath());
        } catch (IOException ex) {
          ex.printStackTrace();
        }
        e.printStackTrace();
        return false;
      }
    } finally {
      // remove temp files
      tmpSingletonDir.delete();
    }
  }
}