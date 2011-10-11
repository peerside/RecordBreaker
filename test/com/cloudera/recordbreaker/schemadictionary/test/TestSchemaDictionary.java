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
package com.cloudera.recordbreaker.schemadictionary.test;


import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;

import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.reflect.ReflectDatumReader;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.rules.TemporaryFolder;

import com.cloudera.recordbreaker.schemadict.SchemaMapping;
import com.cloudera.recordbreaker.schemadict.SchemaSuggest;
import com.cloudera.recordbreaker.schemadict.SchemaDictionary;
import com.cloudera.recordbreaker.schemadict.DictionaryMapping;
import com.cloudera.recordbreaker.schemadict.SchemaDictionaryEntry;


/**
 * TestSchemaDictionary tests the SchemaDictionary component using a huge number
 * of Avro-formatted databases.  This test suite is currently designed to run
 * on the output of src/python/schemacorpus/buildSchemaDictTests.py, which uses
 * data derived from Metaweb's Freebase system.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 * @version 1.0
 * @since 1.0
 * @see InferenceTest
 */
public class TestSchemaDictionary {
  static File inputDbDir = new File(System.getProperty("test.samples.dir", "src/samples"), "dbs");
  static File trainDbDir = new File(inputDbDir, "train");
  static File testDbDir = new File(inputDbDir, "test");

  @Rule
  public TemporaryFolder tmpOutDir = new TemporaryFolder();
  File workingDir = null;

  /**
   * Creates a new <code>TestSchemaDictionary</code> instance.
   */
  public TestSchemaDictionary() {
  }

  @Before
  public void prepare() {
    workingDir = tmpOutDir.newFolder("workingdir");
  }

  @Test(timeout=100000)
  public void testSchemaDictionary() throws IOException {
    int maxDictSize = 500;
    int maxTestSize = Math.min(10, maxDictSize);
    int MAX_MAPPINGS = 5;
    double MINIMUM_MEAN_RECIPROCAL_RANK = 0.75;
    Random r = new Random();
    
    //
    // Build schema dictionary out of the "train" set
    //
    File dictDir = new File(workingDir, "dict");
    SchemaDictionary sd = new SchemaDictionary(dictDir);
    System.err.println("Building schema dictionary...");
    try {
      // Insert the files
      File targetList[] = trainDbDir.listFiles();
      for (int i = 0; i < targetList.length; i++) {
        File f = targetList[i];
        if (f.getName().endsWith(".avro")) {
          sd.addDictionaryElt(f, f.getName());
          if (i >= maxDictSize) {
            break;
          }
        }
      }
    } catch (Exception iex) {
      iex.printStackTrace();
    }

    //
    // Now evaluate the dictionary using the "test" set
    //
    System.err.println("Testing schema dictionary...");
    SchemaSuggest ss = new SchemaSuggest(dictDir);
    double totalReciprocalRank = 0;
    int i = 0;

    // Iterate through all files in the test dir
    System.err.println("Examining: " + testDbDir);
    for (File f: testDbDir.listFiles()) {
      if (f.getName().endsWith(".avro")) {
        String testName = f.getName();
        System.err.println("Testing against " + testName + "...");

        // Go through the top-MAX_MAPPINGS related schemas, as returned by SchemaDictionary
        int rank = 1;
        List<DictionaryMapping> mappings = ss.inferSchemaMapping(f, MAX_MAPPINGS);
        double scores[] = new double[MAX_MAPPINGS];
        for (DictionaryMapping mapping: mappings) {
          SchemaDictionaryEntry dictEntry = mapping.getDictEntry();
          SchemaMapping smap = mapping.getMapping();
          scores[rank-1] = smap.getDist();

          // Did the query database match one of the returned results?
          System.err.println("  " + rank + ".  (" + smap.getDist() + ") " + mapping.getDictEntry().getInfo());
          if (dictEntry.getInfo().equals(testName)) {
            // If so, find the max rank of any object that had the match's score.
            // (This is necessary because multiple objects can have the same match score.
            //   The current match's rank isn't necessarily the one to use.)
            double currentScore = smap.getDist();
            int correctRank = rank;
            for (int j = 0; j < rank; j++) {
              if (scores[j] == currentScore) {
                correctRank = j+1;
                break;
              }
            }

            // Now that we know the correct rank, compute this database's reciprocal rank result
            double reciprocalRank = 1.0 / correctRank;
            totalReciprocalRank += reciprocalRank;
            break;
          }
          rank++;
        }
        i++;        
        System.err.println("After " + i + " tests, MRR is " + (totalReciprocalRank / i));
        System.err.println();

        if (i >= maxTestSize) {
          break;
        }
      }
    }
    double meanReciprocalRank = totalReciprocalRank / i;
    System.err.println("Mean reciprocal rank: " + meanReciprocalRank);

    // Since we're testing on data that is drawn directly from dbs already known to
    // SchemaDictionary, we expect very good results from the mapping ranking.
    Assert.assertTrue(meanReciprocalRank >=  0.75);
  }

  @After
  public void teardown() {
  }
}

