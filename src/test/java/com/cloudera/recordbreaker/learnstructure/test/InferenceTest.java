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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;

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
  static File sampleDir = new File(System.getProperty("test.samples.dir", "src/samples"), "textdata"); 
  
  /**
   * runSingletonTest() executes LearnStructure test for a single given input text file.
   *
   * @param inputData a <code>File</code> value
   * @return a <code>boolean</code> value;  did the test succeed?
   */
  boolean runSingletonTest(File workingDir, File inputData) {
    File tmpSingletonDir = new File(workingDir, "testinference-" + inputData.getName());
    try {
      FileSystem localFS = FileSystem.getLocal(new Configuration());
      tmpSingletonDir.mkdir();
      Path schemaFile = new Path(tmpSingletonDir.getCanonicalPath(), LearnStructure.SCHEMA_FILENAME);
      Path parseTreeFile = new Path(tmpSingletonDir.getCanonicalPath(), LearnStructure.PARSER_FILENAME);
      Path jsonDataFile = new Path(tmpSingletonDir.getCanonicalPath(), LearnStructure.JSONDATA_FILENAME);
      Path avroFile = new Path(tmpSingletonDir.getCanonicalPath(), LearnStructure.DATA_FILENAME);

      LearnStructure ls = new LearnStructure();
      // Check to see how many records exist in the original input
      int lineCount = 0;
      BufferedReader in2 = new BufferedReader(new FileReader(inputData));
      try {
        while (in2.readLine() != null) {
          lineCount++;
        }
      } finally {
        in2.close();
      }

      // Infer structure
      ls.inferRecordFormat(localFS, new Path(inputData.getCanonicalPath()), localFS, schemaFile, parseTreeFile, jsonDataFile, avroFile, false, lineCount);

      // Test the inferred structure
      // First, load in the avro file and see how many records there are.
      int avroCount = 0;
      DataFileReader in = new DataFileReader(new File(avroFile.toString()), new GenericDatumReader());
      try {
        Iterator it = in.iterator();
        while (it.hasNext()) {
          avroCount++;
          it.next();
        }
      } finally {
        in.close();
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
    } finally {
      // remove temp files
      tmpSingletonDir.delete();
    }
  }
}