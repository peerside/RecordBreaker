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
package com.cloudera.recordbreaker.schemadict;

import java.io.*;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/****************************************************************
 * SchemaSuggest generates labels for an Avro file with schema elts that are not
 * usefully named.  It uses a dictionary of schemas/data with high-quality labels.
 * It compares the candidate avro data to everything in the dictionary, finding the
 * k most-similar entries.  It then computes a mapping between the candidate schema
 * and each of the k best ones.  The user can then use the resulting schema to replace
 * the candidate's badly-labeled one.
 *
 * This class is particularly useful when operating on an Avro schema that was
 * algorithmically-generated (\ie, through learn-avro).
 *
 * @author mjc
 ****************************************************************/
public class SchemaSuggest {
  int NUM_BUCKETS = 20;
  SchemaDictionary dict;
  List<List<SchemaDictionaryEntry>> dictBySize;
  
  /**
   * Load in the Schema Dictionary from the indicated file.
   */
  public SchemaSuggest(File dataDir) throws IOException {
    this.dict = new SchemaDictionary(dataDir);

    // The 'dictBySize' structure allows us to perform schema inference
    // more quickly, by avoiding examination of schemas that can't possibly
    // be returned by inferSchemaMapping().
    this.dictBySize = new ArrayList<List<SchemaDictionaryEntry>>();
    for (int i = 0; i < NUM_BUCKETS; i++) {
      dictBySize.add(new ArrayList<SchemaDictionaryEntry>());
    }

    for (SchemaDictionaryEntry elt: dict.contents()) {
      Schema comparisonSchema = elt.getSchema();
      int comparisonSchemaSize = comparisonSchema.getFields().size();
      if (comparisonSchemaSize < dictBySize.size()-1) {
        dictBySize.get(comparisonSchemaSize-1).add(elt);
      } else {
        dictBySize.get(dictBySize.size()-1).add(elt);
      }
    }
  }

  /**
   * This method infers new schema labels for each element in the input.  It returns a Schema object that
   * has the identical format as the input file's Schema object, but the labels may be changed.
   */
  public List<DictionaryMapping> inferSchemaMapping(File avroFile, int k) throws IOException {
    SchemaStatisticalSummary srcSummary = new SchemaStatisticalSummary("input");
    Schema srcSchema = null;
    try {
      srcSchema = srcSummary.createSummaryFromData(avroFile);
    } finally {
    }
    System.err.println("Schema size is " + srcSchema.getFields().size());
    
    //
    // Compare the statistics to the database of schema statistics.  Find the closest matches, both
    // on a per-attribute basis and structurally.
    //
    int schemaSize = srcSchema.getFields().size();
    //
    // We start testing the input database against known schemas that have an identical
    // number of attributes, which should allow for the best matches.  This gives us an
    // initial set of distances.  We then expand the search to schemas of greater or fewer
    // attributes, as long as a given bucket of size-k schemas has a min-distance of less
    // than the current top-k matches.
    //
    //
    TreeSet<DictionaryMapping> sorter = new TreeSet<DictionaryMapping>();
    int numMatches = 0;
    List<Integer> seenIndexes = new ArrayList<Integer>();
    int searchRadius = 0;
    boolean seenAllCandidates = false;
    int srcSchemaSize = srcSchema.getFields().size();
    int totalSchemasExamined = 0;
    
    while (! seenAllCandidates) {
      // Examine the relevant schema buckets, compute all matches to those schemas
      for (int j = Math.max(1, srcSchemaSize - searchRadius);
           j <= Math.min(NUM_BUCKETS, srcSchemaSize + searchRadius); j++) {

        if (seenIndexes.contains(j-1)) {
          continue;
        }
        for (SchemaDictionaryEntry elt: dictBySize.get(j-1)) {
          SchemaMapping mapping = srcSummary.getBestMapping(elt.getSummary());
          totalSchemasExamined++;
          sorter.add(new DictionaryMapping(mapping, elt));
          numMatches++;
        }
        seenIndexes.add(j-1);
      }

      // Have we examined the entire corpus of known schemas?
      if ((srcSchemaSize - searchRadius) <= 1 && (srcSchemaSize + searchRadius) >= NUM_BUCKETS) {
        seenAllCandidates = true;
      } else {
        // Test to see if the best matches are good enough that we can stop looking.
        // We compare the lowest known match distance to the minimum distance for matches
        // in the closest non-examined buckets.
        int lowestSize = srcSchemaSize - searchRadius - 1;
        int highestSize = srcSchemaSize + searchRadius + 1;
        double minNearbyDistance = Double.MAX_VALUE;
        if (lowestSize >= 1) {
          minNearbyDistance = Math.min(minNearbyDistance,
                                       SchemaStatisticalSummary.getMinimumMappingCost(srcSchemaSize, lowestSize));
        }
        if (highestSize <= NUM_BUCKETS) {
          minNearbyDistance = Math.min(minNearbyDistance,
                                       SchemaStatisticalSummary.getMinimumMappingCost(srcSchemaSize, highestSize));
        }
        // Grab from the Sorter the elt that is MIN_ELTS_SUGGESTED into the sorted list
        if (sorter.size() >= k) {
          DictionaryMapping testDictMapping = null;
          int idx = 0;
          for (DictionaryMapping cur: sorter) {
            idx++;
            if (idx == k) {
              testDictMapping = cur;
              break;
            }
          }
          if (testDictMapping.getMapping().getDist() < minNearbyDistance) {
            seenAllCandidates = true;
          }
        }
      }
      searchRadius++;
    }
      
    // Return the k best schema mappings
    double smallestDistance = sorter.first().getMapping().getDist();
    List<DictionaryMapping> dsts = new ArrayList<DictionaryMapping>();
    for (DictionaryMapping dp: sorter) {
      if (dsts.size() > k && dp.getMapping().getDist() > smallestDistance) {
        break;
      }
      dsts.add(dp);
    }
    double pct = totalSchemasExamined / (1.0 * dict.contents().size());
    System.err.println("Final search radius of " + searchRadius + " yielded a search over " + pct + " of all known databases.");
    return dsts;
  }

  /**
   * SchemaSuggest takes an avro file where schema elements may be anonymous.  It then attempts to 
   * compute good labels for the anonymous elts.  By default, this tool simply prints out the
   * suggested labels, if any.  The user may include a flag to rewrite the input data using
   * the new labels.
   *
   * schemaSuggest avroFile 
   *
   */
  public static void main(String argv[]) throws IOException {
    CommandLine cmd = null;
    boolean debug = false;
    Options options = new Options();
    options.addOption("?", false, "Help for command-line");
    options.addOption("f", true, "Accept suggestions and rewrite input to a new Avro file");
    options.addOption("d", false, "Debug mode");
    options.addOption("k", true, "How many matches to emit.");

    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaSuggest", options, true);
      System.err.println("Required inputs: <schemadictionary> <anonymousAvro>");
      System.exit(-1);
    }

    if (cmd.hasOption("?")) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaSuggest", options, true);
      System.err.println("Required inputs: <schemadictionary> <anonymousAvro>");
      System.exit(0);
    }

    if (cmd.hasOption("d")) {
      debug = true;
    }

    int k = 1;
    if (cmd.hasOption("k")) {
      try {
        k = Integer.parseInt(cmd.getOptionValue("k"));
      } catch (NumberFormatException nfe) {
      }
    }

    String[] argArray = cmd.getArgs();
    if (argArray.length < 2) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaSuggest", options, true);
      System.err.println("Required inputs: <schemadictionary> <anonymousAvro>");
      System.exit(0);
    }

    File dataDir = new File(argArray[0]).getCanonicalFile();
    File inputData = new File(argArray[1]).getCanonicalFile();
    SchemaSuggest ss = new SchemaSuggest(dataDir);
    List<DictionaryMapping> mappings = ss.inferSchemaMapping(inputData, k);

    if (! cmd.hasOption("f")) {
      System.out.println("Ranking of closest known data types, with match-distance (smaller is better):");
      int counter = 1;
      for (DictionaryMapping mapping: mappings) {
        SchemaMapping sm = mapping.getMapping();
        List<SchemaMappingOp> bestOps = sm.getMapping();

        System.err.println();
        System.err.println();
        System.err.println("-------------------------------------------------------------");
        System.out.println(counter + ".  '" + mapping.getDictEntry().getInfo() + "', with distance: " + sm.getDist());

        List<SchemaMappingOp> renames = new ArrayList<SchemaMappingOp>();
        List<SchemaMappingOp> extraInTarget = new ArrayList<SchemaMappingOp>();
        List<SchemaMappingOp> extraInSource = new ArrayList<SchemaMappingOp>();

        for (SchemaMappingOp op: bestOps) {
          if (op.opcode == SchemaMappingOp.CREATE_OP) {
            extraInTarget.add(op);
          } else if (op.opcode == SchemaMappingOp.DELETE_OP) {
            if (op.getS1DatasetLabel().compareTo("input") == 0) {
              extraInSource.add(op);
            } else {
              extraInTarget.add(op);
            }
          } else if (op.opcode == SchemaMappingOp.TRANSFORM_OP) {
            renames.add(op);
          }
        }

        System.err.println();
        System.err.println(" DISCOVERED LABELS");
        int counterIn = 1;
        if (renames.size() == 0) {
          System.err.println("  (None)");
        } else {
          for (SchemaMappingOp op: renames) {
            System.err.println("  " + counterIn + ".  " + "In '" + op.getS1DatasetLabel() + "', label '" + op.getS1FieldLabel() + "' AS " + op.getS2FieldLabel());
            if (debug) {
              if (op.getS1DocStr() != null && op.getS1DocStr().length() > 0) {
                System.err.println("         '" + op.getS1DocStr() + "'  ==> '" + op.getS2DocStr() + "'");
              }
            }
            counterIn++;
          }
        }

        System.err.println();
        System.err.println(" UNMATCHED ITEMS IN TARGET DATA TYPE");
        counterIn = 1;
        if (extraInTarget.size() == 0) {
          System.err.println("  (None)");
        } else {
          for (SchemaMappingOp op: extraInTarget) {
            System.err.println("  " + counterIn + ".  " + op.getS1FieldLabel());
            if (debug) {
              if (op.getS1DocStr() != null && op.getS1DocStr().length() > 0) {
                System.err.println("         " + op.getS1DocStr());
              }
            }
            counterIn++;
          }
        }

        System.err.println();
        System.err.println(" UNMATCHED ITEMS IN SOURCE DATA");
        counterIn = 1;
        if (extraInSource.size() == 0) {
          System.err.println("  (None)");
        } else {
          for (SchemaMappingOp op: extraInSource) {
            System.err.println("  " + counterIn + ".  " + op.getS1FieldLabel());
            if (debug) {
              if (op.getS1DocStr() != null && op.getS1DocStr().length() > 0) {
                System.err.println("         " + op.getS1DocStr());
              }
            }
            counterIn++;
          }
        }
        counter++;
      }
    }
  }
}