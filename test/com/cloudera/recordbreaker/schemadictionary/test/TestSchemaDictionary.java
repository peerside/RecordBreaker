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
import java.util.TreeMap;
import java.util.ArrayList;

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
import com.cloudera.recordbreaker.schemadict.SchemaStatisticalSummary;


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
  static File inputDbDir = new File(System.getProperty("test.samples.dir", "src/samples/dbs/freebase"));
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

  /**
   */
  @Test(timeout=200000)
  public void testSchemaDictionary() throws IOException {
    try {
      int maxDictSize = 3000;
      int maxTestSize = maxDictSize;
      int MAX_MAPPINGS = 10;
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
      // Now evaluate the dictionary using the "test" set.
      // Be sure to keep a lot of statistics about match failures
      //
      System.err.println("Testing schema dictionary...");
      SchemaSuggest ss = new SchemaSuggest(dictDir);
      ss.setUseAttributeLabels(false);
      TreeMap<Integer, Integer> overallSizes = new TreeMap<Integer, Integer>();
      TreeMap<Integer, Integer> failureSizes = new TreeMap<Integer, Integer>();
      List<Schema> failedSchemas = new ArrayList<Schema>();
      List<SchemaStatisticalSummary> failedSummaries = new ArrayList<SchemaStatisticalSummary>();
      double totalReciprocalRank = 0;
      int i = 0;
      int failures = 0;

      // Iterate through all files in the test dir      
      System.err.println("Examining: " + testDbDir);
      for (File f: testDbDir.listFiles()) {
        try {
          if (f.getName().endsWith(".avro")) {
            String testName = f.getName();
            SchemaStatisticalSummary testSummary = new SchemaStatisticalSummary("input");
            Schema testSchema = testSummary.createSummaryFromData(f);
            int schemaSize = testSchema.getFields().size();
            Integer sizeCount = overallSizes.get(schemaSize);
            if (sizeCount == null) {
              sizeCount = new Integer(0);
            }
            overallSizes.put(schemaSize, new Integer(sizeCount.intValue() + 1));

            System.err.println("Testing against " + testName);
            System.err.println("Schema size is " + schemaSize);

            // Go through the top-MAX_MAPPINGS related schemas, as returned by SchemaDictionary
            int rank = 1;
            long startTime = System.currentTimeMillis();
            List<DictionaryMapping> mappings = ss.inferSchemaMapping(f, MAX_MAPPINGS);
            long endTime = System.currentTimeMillis();
            System.err.println("  it took " + ((endTime - startTime) / 1000.0) + ", returned " + mappings.size() + " elts");
        
            double scores[] = new double[mappings.size()];
            boolean foundGoal = false;
            for (DictionaryMapping mapping: mappings) {
              SchemaDictionaryEntry dictEntry = mapping.getDictEntry();
              SchemaMapping smap = mapping.getMapping();
              scores[rank-1] = smap.getDist();

              // Did the query database match one of the returned results?
              System.err.println("  " + rank + ".  (" + smap.getDist() + ") " + mapping.getDictEntry().getInfo() + " (size=" + mapping.getDictEntry().getSchema().getFields().size() + ")");

              if (dictEntry.getInfo().equals(testName)) {
                // If so, find the max rank of any object that had the match's score.
                // (This is necessary because multiple objects can have the same match score.
                //   The current match's rank isn't necessarily the one to use.)
                System.err.println("Found mapping: " + smap.toString());              

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
                foundGoal = true;
                break;
              }
              rank++;
            }
            if (! foundGoal) {
              failures++;
              sizeCount = failureSizes.get(schemaSize);
              if (sizeCount == null) {
                sizeCount = new Integer(0);
              }
              failureSizes.put(schemaSize, new Integer(sizeCount.intValue() + 1));
              failedSchemas.add(testSchema);
              failedSummaries.add(testSummary);
            }
            i++;        
            System.err.println("After " + i + " tests, MRR is " + (totalReciprocalRank / i));
            System.err.println();

            if (i >= maxTestSize) {
              break;
            }
          }
        } catch (IOException iex) {
          continue;
        }
      }
      double meanReciprocalRank = totalReciprocalRank / i;
      System.err.println("Mean reciprocal rank: " + meanReciprocalRank);
      System.err.println();
      System.err.println("*** Overall Distribution ***");
      int cumulativeFrequency = 0;
      for (Integer size: overallSizes.keySet()) {
        int frequency = overallSizes.get(size);
        cumulativeFrequency += frequency;
        double ratio = frequency / (1.0 * i);
        double cumulativeRatio = cumulativeFrequency / (1.0 * i);
        System.err.println("  " + size + ":  " + frequency + " (" + ratio + ")  (cumulative=" + cumulativeRatio + ")");
      }
      System.err.println();
      
      System.err.println("*** Failure Distribution ***");
      cumulativeFrequency = 0;
      for (Integer size: failureSizes.keySet()) {
        int frequency = failureSizes.get(size);
        cumulativeFrequency += frequency;
        double ratio = frequency / (1.0 * failures);
        double cumulativeRatio = cumulativeFrequency / (1.0 * failures);
        System.err.println("  " + size + ":  " + frequency + " (" + ratio + ")  (cumulative=" + cumulativeRatio + ")");
      }
      System.err.println();

      System.err.println("Number of match tests: " + i);
      double ratio = failures / (1.0 * i);      
      System.err.println("Number of match test failures: " + failures + " (" + ratio + ")");
      System.err.println();

      System.err.println("*** Failed Test Schemas ***");
      for (Schema failedSchema: failedSchemas) {
        System.err.println("FAILED SCHEMA: " + failedSchema.getName());
        for (Schema.Field field: failedSchema.getFields()) {
          System.err.println("  " + field.toString());
        }
        System.err.println();
      }
      
      // Since we're testing on data that is drawn directly from dbs already known to
      // SchemaDictionary, we expect very good results from the mapping ranking.
      Assert.assertTrue(meanReciprocalRank >=  0.75);
    } catch (Exception iex) {
      iex.printStackTrace();
    }
  }

  @After
  public void teardown() {
  }
}

